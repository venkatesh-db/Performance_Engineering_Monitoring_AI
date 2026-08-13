# Hands-on lab prompt for Claude — Day 2 / Module 2

```
I profiled an IRCTC-style fare-estimation API with cProfile and tracemalloc
against the live FastAPI process. Here's what I captured:

EVENT-LOOP BLOCKING TEST (fare estimate + concurrent health check):
Buggy:  fare finished at t+322ms, health check finished at t+267ms (should be ~1ms)
Fixed:  fare finished at t+317ms, health check finished at t+1.7ms

CPROFILE (10 calls, distance_km=300, passengers=2, buggy path):
time.sleep: 304.6ms/call cumulative
_compute_fare_matrix: 0.56ms/call cumulative

CPROFILE (same, but distance_km=8000, passengers=20):
time.sleep: 305.0ms/call
_compute_fare_matrix: 59.4ms/call

CPROFILE (fixed/memoized path, 5 identical calls):
_compute_fare_matrix_fixed: ncalls=5
_compute_fare_matrix (underlying, unmemoized): ncalls=1

TRACEMALLOC (main.py:120, the refund-history record builder):
Buggy,  40 calls: 2,030.0 KB / 31,565 objects
Fixed,  40 calls: 1,039.8 KB / 16,060 objects (capped at 20 history entries)

Acting as a performance tester writing the profiling report for this lab,
do the following:
1. Rank the three bugs (blocking I/O, unmemoized O(n^3) fare matrix,
   unbounded refund history) by which causes the worst production
   incident fastest under real Tatkal-level concurrent traffic, citing
   my specific numbers for each.
2. For the CPU hotspot: explain why _compute_fare_matrix's cost was
   negligible at small trip distances but became the dominant cost
   contributor as distance grew, and what this means for capacity
   planning around IRCTC's actual longest-distance routes.
3. For the blocking I/O bug: explain precisely why a SINGLE blocking
   call inside ONE endpoint degrades an UNRELATED endpoint's latency,
   in terms of what asyncio's event loop is and isn't doing while
   time.sleep() runs.
4. For the memory growth: given the 40-call numbers, estimate roughly
   how many refund-history calls it would take to reach 1GB of retained
   memory on the buggy path, and confirm the fix's cap prevents that
   entirely regardless of call volume.
5. Recommend one experiment using py-spy (not cProfile) that would
   catch something cProfile's own instrumentation might miss or distort.

Cite my actual numbers -- don't give generic profiling advice.
```

## Notes
- Attach `main.py` and `CASE_STUDY.md` so Claude reasons from the real
  code paths (the exact loop bounds, the exact cache key, the exact
  history cap) instead of assumptions.
- `CASE_STUDY.md` is explicit that py-spy itself was never run in this
  dev environment (needs `sudo` on macOS) — if you run it on the Linux
  training VM, paste its real `top`/`dump` output and ask Claude to
  cross-check it against the cProfile numbers above.
