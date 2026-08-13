# IRCTC Use Case — Pain Points & Root-Cause Analysis
### Day 2 / Module 1: Application and Linux Performance Analysis

## Scenario

IRCTC's ticket-booking service degrades during a Tatkal (rush-booking)
window. A performance tester is asked to correlate system-resource
behaviour with application evidence and pin down the real bottleneck —
not just "it's slow," but which resource, in which process, caused by
which code path.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "Booking button just spins during Tatkal, for everyone at once" | Booking latency degrades sharply and uniformly under concurrent load |
| 2 | "PNR status check sometimes returns 'connection refused' during peak" | Intermittent failures, worse later in the day than at open |
| 3 | "The app server needs a restart every evening or it slows to a crawl" | Gradual, one-directional slowdown over hours, not tied to any single request |

## Root-cause identification, mapped to Day 2 / Module 1 topics

### 1. CPU utilization, run queue, and GIL contention → pain point #1

**Captured evidence** (this repo, `main.py`'s `/api/book`):

| Load | CPU time per booking |
|---|---|
| 1 request, no concurrency | 58.5ms |
| 30 concurrent requests | 780ms – 1,588ms (sorted spread) |

**Root cause:** `_allocate_seat()` runs a brute-force O(seats²) scan on
every booking — 58ms of real CPU work per call even in isolation. Under
concurrency, that CPU-bound work runs inside a thread pool
(`loop.run_in_executor`), but Python's GIL means only one thread
actually executes Python bytecode at a time — so 30 concurrent bookings
don't run in parallel, they queue for the GIL. The result: per-request
time balloons by **20-27x**, and — critically — this shows up on Linux
as **high CPU utilization (`top`, `%usr`) with a growing run queue
(`vmstat`'s `r` column)**, not as high I/O wait or network latency.
This is the tester's first checkpoint: confirm the bottleneck is CPU
before chasing database or network theories.

**On the real Linux training VM, correlate with:**
```
top -H -p <pid>          # per-thread CPU%, confirms which threads are hot
vmstat 1                 # r column climbing = more runnable threads than cores
pidstat -p <pid> 1        # %usr/%system per process, context switches
```

### 2. Memory growth and disk fsync latency → pain point #3 and part of #1

**Captured evidence:**
- `WAITLIST` grew to 31 entries after ~31 bookings in this test session
  and is never trimmed — `memory_rss_mb` climbs monotonically with
  booking volume (45.9MB and rising in this run; over a full day of
  real Tatkal volume, this is the "needs a restart every evening" bug).
- Every booking does a synchronous `f.write()` + `f.flush()` +
  `os.fsync()` — in this test, 0.5ms locally on an otherwise-idle SSD,
  but each fsync is a **real, blocking syscall** whose cost scales with
  disk contention, not with app code. Under real load, or on a busier /
  network-attached disk, this same code path is where `iostat`'s
  `await` would spike.

**Root cause:** the waitlist is meant to be an operational cache but has
no eviction or persistence-then-clear policy, so RSS grows linearly with
total bookings served since the last restart — classic memory-pressure
signature. The fsync-per-write is a separate, compounding issue: even
with memory fixed, every booking still pays a real disk round-trip
inside the request path.

**On the real Linux training VM, correlate with:**
```
pidstat -r -p <pid> 1     # RSS/VSZ growth trend over time
free -m                   # confirm it's this process, not OS cache noise
iostat -x 1                # await, %util on the disk serving booking_log.txt
pidstat -d -p <pid> 1     # per-process disk read/write, confirms it's this app
```

### 3. File descriptor / socket leak → pain point #2

**Captured evidence:**
```
15 PNR checks -> 7 leaked a socket, ending at total_leaked_sockets=7
/api/proc-metrics -> "connections_by_status": {"CLOSE_WAIT": 7, ...}
                      "num_fds": 22 (up from 14 at a clean start)
```

**Root cause:** `/api/pnr/{pnr}` calls a simulated external gateway over
a raw blocking socket, and ~30% of calls never close it (a stand-in for
a real legacy integration bug — e.g. an exception path that skips
`sock.close()`). Each leaked socket holds a file descriptor and leaves
the remote side in `CLOSE_WAIT`. Under sustained traffic this
accumulates until the process hits its open-file-descriptor limit
(`ulimit -n`), at which point *new* PNR checks (and potentially new
bookings, since they share the same process) start failing with
connection errors — explaining why pain point #2 gets worse later in
the day and looks like a network problem when it's actually a leak in
this process specifically.

**On the real Linux training VM, correlate with:**
```
lsof -p <pid> | wc -l           # total open FDs for this process, trending up
lsof -p <pid> | grep CLOSE_WAIT  # confirms which FDs are stuck, not just busy
ss -s                            # system-wide socket summary
ulimit -n                        # the ceiling this process will eventually hit
```

### 4. Distinguishing load-generator, application, and dependency bottlenecks

A tester who only watches the load-generator's reported latency would
see "PNR checks are slow" and might blame the network. The three
metrics above let you place the bottleneck correctly:
- **CPU/run-queue high, disk/network normal** → application-side (bug #1).
- **RSS climbing, disk await elevated, CPU normal** → application-side
  memory + this app's own disk writes (bug #2), not the DB or a shared
  disk array.
- **FD/socket count climbing, specifically in this PID via `lsof -p`**
  → application-side leak in the gateway-integration code (bug #3), not
  the gateway service itself (which responds in a flat, real 300ms
  every time — check `gateway_ms` in the `/api/pnr` response to confirm
  the dependency itself isn't slow).

## How the hands-on lab reproduces this

1. Start `main.py`. Confirm `/api/proc-metrics` returns clean baseline
   numbers.
2. Book one ticket, note `cpu_ms` and `disk_ms` in the response.
3. Click "Fire rush" (or POST 20-30 concurrent bookings) and watch
   `cpu_ms` degrade non-linearly — correlate with `top`/`vmstat` on the
   real Linux VM, or the CPU%/thread count in `/api/proc-metrics` here.
4. Fire 15+ PNR checks and watch `connections_by_status.CLOSE_WAIT` and
   `num_fds` climb — correlate with `lsof -p <pid>` / `ss -s` on Linux.
5. Leave the app running and keep booking — watch `memory_rss_mb` and
   `waitlist_size` climb without bound; on Linux, `pidstat -r` shows the
   same trend against wall-clock time.
6. For each symptom, write down: what metric moved, which Linux command
   would show it in production, and whether it's client-side (load
   generator), application-side, or dependency-side.

## Honesty note on this environment

This dev box is macOS, not Linux — `vmstat`, `pidstat`, `ss`, `sar`, and
`htop` aren't available here, so `/api/proc-metrics` (via `psutil`)
stands in as a cross-platform equivalent and every number in this file
was captured through it, live, on this machine. The exact Linux command
sequences above are written for the actual training VM and were not
run or verified on this box — run them there and confirm the same
signatures (CPU%, run queue, RSS trend, FD/CLOSE_WAIT count) show up
under the same load steps.
