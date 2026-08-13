# Hands-on lab prompt for Claude — Day 2 / Module 1

Use this after running the lab steps in `CASE_STUDY.md` on the Linux
training VM, once you have your own Linux tool output to paste in.

```
I'm profiling an IRCTC-style ticket booking API for a Day 2 performance
analysis lab. The app has three known injected issues: a CPU-bound seat
allocation algorithm, an unbounded in-memory waitlist + synchronous disk
fsync on every booking, and a socket/FD leak on ~30% of PNR status
checks. Here's what I captured:

BASELINE /api/proc-metrics:
cpu_percent=__, memory_rss_mb=__, num_fds=__, connections_by_status=__

AFTER 30 concurrent bookings (Tatkal rush simulation):
per-request cpu_ms sorted: [paste the array from the "Fire rush" response]

AFTER 15 PNR checks:
connections_by_status=__ (specifically CLOSE_WAIT count), num_fds=__

LINUX COMMAND OUTPUT (paste what you captured on the training VM):
top -H -p <pid>: __
vmstat 1: __
pidstat -r -p <pid> 1: __
lsof -p <pid> | grep CLOSE_WAIT: __
iostat -x 1: __

Acting as a performance tester writing the day's deliverable (profiling
and trace-analysis report), do the following:
1. For each symptom (CPU spike under concurrency, growing CLOSE_WAIT
   count, climbing RSS), state which specific Linux command output
   proves it, and rank the three issues by which will cause a customer-
   facing outage soonest under sustained Tatkal-level traffic.
2. Explain why the CPU degradation isn't linear with concurrency (58ms
   solo vs 780-1588ms at 30 concurrent) -- what does this tell you about
   whether adding more app server threads would help, versus adding
   more processes/instances?
3. For the FD leak: calculate roughly how many PNR checks it would take
   to exhaust a default `ulimit -n` of 1024, given the leak rate you
   observed, and state what error users would see right before that
   limit is hit.
4. Write the "ranked hypotheses + validation experiment" section of the
   deliverable: for each of the 3 issues, one experiment that would
   prove it's the AND ONLY the root cause (isolating it from the other
   two).

Cite my actual captured numbers, not generic advice.
```

## Notes
- Attach `main.py` and `CASE_STUDY.md` so Claude reasons from the actual
  code (the O(n²) scan, the fsync call, the 30% leak rate) rather than
  assumptions about what a "typical" booking API does.
- The case study is explicit that its own captured numbers come from
  macOS via `psutil`, not real Linux tool output — replace those
  Linux-command placeholders with your own VM's real output before
  asking Claude to reason about them.
