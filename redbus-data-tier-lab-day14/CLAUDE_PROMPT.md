# Hands-on lab prompt for Claude — Module 4

Use this after running the lab steps in `CASE_STUDY.md`. Fill in the
blanks with your own captured numbers (or use the real ones already
captured in this repo as a starting point).

```
I'm running the Module 4 hands-on lab against a redBus-style search +
booking API backed by real PostgreSQL, real Redis, and Kafka (broker
optional). Here's what I captured:

POOL EXHAUSTION (POOL_MAX_SIZE=3, 15 concurrent uncached searches):
Sorted server-side latencies (ms): 172, 198, 212, 325, 353, 369, 483,
528, 542, 649, 694, 707, 814, 859, 876

CACHE-ASIDE (same query, back-to-back):
1st call (Postgres): 187ms   2nd call (Redis): 0.68ms

DIRECT COMPONENT BENCHMARKS:
pgbench (10 clients, 10s): 6,217 TPS, avg latency 1.6ms
redis-benchmark (50 clients): SET 99,010 rps / GET 89,847 rps, p50 ~0.29ms

KAFKA (if you ran it -- otherwise say "not run, no broker available"):
producer rate: ___ msgs/sec
consumer --process-delay-ms: ___
observed lag growth: ___

Acting as a performance tester writing up root-cause findings, do the
following:
1. Explain the 172ms-to-876ms latency spread using the pool size and
   per-request hold time -- what pool size would keep P95 under, say,
   400ms at this concurrency, and what's the tradeoff of just raising
   the pool size arbitrarily high?
2. Explain why pgbench's 1.6ms and the app's 172-876ms are BOTH correct
   measurements of "the same database" -- what does each one actually
   measure, and why would quoting pgbench's number in a capacity
   planning doc for this API be misleading?
3. Quantify the cache-aside win (275x) and identify one failure mode
   this simple TTL-based cache doesn't protect against (cache stampede
   or hot-key skew) -- describe a concrete scenario where it would bite
   redBus during a flash sale.
4. If I give you Kafka producer/consumer numbers, explain what a
   growing lag trend (vs. a stable one) tells you about whether the
   consumer side needs more instances, more partitions, or neither.

Cite the specific numbers I gave you, not generic advice.
```

## Notes
- Attach `main.py`, `db/schema.sql`, and `CASE_STUDY.md` so Claude
  reasons from the actual pool size, index state, and cache TTL in this
  repo, not assumptions.
- If you complete the Kafka portion (requires `docker compose -f
  kafka/docker-compose.yml up -d`), paste real producer/consumer numbers
  before asking question 4 — don't let Claude guess at lag figures you
  haven't captured.
