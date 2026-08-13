# IRCTC Use Case — Python Profiling Pain Points & Root-Cause Analysis
### Day 2 / Module 2: Python Application Profiling

## Scenario

IRCTC's fare-estimation and refund-history endpoints are slow and the
whole app occasionally "freezes" for a few seconds at a time, even for
users doing something unrelated (checking train status). A performance
tester profiles the running FastAPI process with cProfile, tracemalloc,
and (on a real Linux box) py-spy, to find the exact function and line
responsible — not just "the fare API is slow."

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "The whole site froze for a second, not just the fare page I was on" | Unrelated endpoints (health/status checks) stall while someone else is fetching a fare estimate |
| 2 | "Fare estimate for a long-distance train takes forever" | `/api/fare/estimate` latency grows with trip distance, disproportionately |
| 3 | "Refund history page gets slower as the day goes on" | Gradual, cumulative slowdown tied to total requests served, not to any one user's data size |

## Root-cause identification, mapped to Module 2 topics

### 1. Blocking I/O inside an async request path → pain point #1

**Captured evidence** (this repo, real requests fired concurrently
against the live server):

| Path | Fare call finishes at | Health check finishes at |
|---|---|---|
| Buggy (`time.sleep()` on the event loop) | t+322ms | **t+267ms** (should be ~1ms) |
| Fixed (`await asyncio.sleep()`) | t+317ms | **t+1.7ms** |

**Root cause:** `/api/fare/estimate`'s buggy path calls `time.sleep(0.3)`
directly inside an `async def` handler with no executor hand-off. Since
FastAPI/uvicorn run on a single-threaded asyncio event loop by default,
a blocking call there doesn't just slow *that* request — it **freezes
the entire process**, so an unrelated `/api/health` call issued while
the fare call is in flight measurably waits behind it (267ms, when it
should return in ~1ms). This is exactly pain point #1: "the whole site
froze," not "one page was slow." The fix — `await asyncio.sleep()` plus
`run_in_executor` for the CPU work — drops the health check's wait to
1.7ms even while an identical fare call is still running.

### 2. CPU hotspot located via cProfile → pain point #2

**Captured evidence** (`/api/debug/profile/start` → 10x
`/api/fare/estimate` calls → `/api/debug/profile/stop`, real cProfile
output from the live process):

| Input size | `time.sleep` cost | `_compute_fare_matrix` cost |
|---|---|---|
| distance_km=300, passengers=2 | 304.6ms/call | 0.56ms/call |
| distance_km=8000, passengers=20 | 305.0ms/call (fixed) | **59.4ms/call** |

**Root cause:** `_compute_fare_matrix` is an unmemoized O(distance ×
passengers × 3) triple-nested loop. At small trip distances its cost is
negligible next to the blocking sleep — but it scales with trip
distance, so long-distance fares (exactly pain point #2's complaint)
pay a real, growing CPU cost on top of the fixed sleep cost. **This is
the point of profiling instead of guessing**: at small inputs, a tester
who assumed "the loop is the problem" would optimize the wrong thing
first; cProfile's actual cumulative-time numbers show which cost
dominates *for a given input size*, and here both bugs are real but
scale differently.

**Fix validated:** re-profiling the memoized path shows
`_compute_fare_matrix` executes with `ncalls=1` even after 5 requests
with the same parameters — later calls are served from `_fare_cache` at
effectively zero cost, confirmed directly in the pstats output, not
assumed.

### 3. Memory growth located via tracemalloc → pain point #3

**Captured evidence** (`/api/debug/tracemalloc/start` → N ×
`/api/refund/history` → `/api/debug/tracemalloc/snapshot`):

| Calls | Buggy: top allocation (`main.py:120`) | Fixed (capped at 20): same line |
|---|---|---|
| 15 | 750.4 KB / 11,659 objects | 752.9 KB / 11,708 objects (cap not yet reached) |
| 40 | **2,030.0 KB / 31,565 objects** | **1,039.8 KB / 16,060 objects** |

**Root cause:** every call to `/api/refund/history` appends a fresh
200-record snapshot to a global list that's never trimmed.
`main.py:120` (the list-comprehension building those 200 records) is
tracemalloc's single largest allocator by an order of magnitude over
everything else in the process — pinpointing the exact line, not just
"the app uses more memory." At 15 calls the fix and the bug look
identical because the 20-entry cap hasn't engaged yet; at 40 calls the
divergence is unambiguous — buggy memory is **~2x** the fixed version's
and still growing linearly, while the fixed version has flattened.

### 4. What py-spy and Scalene add beyond this lab's own captures

`py-spy dump --pid <pid>` and `py-spy top --pid <pid>` sample the *real*
call stack of a running process without any code changes — useful when
you can't add debug endpoints (production incident) or need to confirm
cProfile's own overhead isn't distorting the picture (cProfile
instruments every call; py-spy's sampling doesn't). `scalene` adds
per-line CPU *and* memory attribution together, including native
extensions cProfile can't see into.

## Honesty note: py-spy was not run in this environment

`py-spy` is installed in this project's venv and `py-spy dump --pid
<pid>` / `py-spy top --pid <pid>` are the exact commands to run against
this app's live PID — but on macOS, py-spy requires root
(`sudo py-spy dump --pid <pid>`), and running `sudo` non-interactively
wasn't appropriate here (needs a real terminal password prompt). On the
Linux training VM this typically works without `sudo` (or with
`--cap-add=SYS_PTRACE` if containerized) — run it there while firing
load at `/api/fare/estimate` and confirm `_compute_fare_matrix` and
`time.sleep` show up as the hot frames, matching the cProfile evidence
above. Everything else in this document — the blocking-I/O timing, the
cProfile pstats numbers, and the tracemalloc allocation sizes — was
captured live against this repo's running server.

## How the hands-on lab reproduces this

1. Start `main.py`. Use the UI's section 1 to fire a buggy fare call and
   a health check simultaneously — confirm the health check is delayed.
   Repeat with the fixed toggle — confirm it isn't.
2. Use section 2 to profile 20 buggy calls, then 20 fixed calls at the
   same `distance_km`/`passengers` — compare `_compute_fare_matrix`'s
   `ncalls` and `cumtime` between runs.
3. Use section 3 to grow refund history 15x under tracemalloc, buggy
   vs. fixed — note `main.py:120`'s size/count diff in each.
4. On the Linux training VM, additionally run `py-spy top --pid <pid>`
   while section 3's rush is firing, and compare its sampled hot frames
   to cProfile's cumulative-time ranking from this lab.
5. Write up: which of the three bugs would you fix first for IRCTC's
   real Tatkal traffic, and why — tie the answer to which pain point
   (#1, #2, or #3) causes the worst customer-visible failure mode fastest.
