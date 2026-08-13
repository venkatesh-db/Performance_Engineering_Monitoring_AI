# redBus Use Case — Database, Kafka, Redis Pain Points & Root-Cause Analysis
### Module 4: Database, Kafka and Redis Performance Essentials

## Scenario

redBus's search-and-book flow depends on three data-tier components: a
PostgreSQL catalog, a Redis cache in front of it, and Kafka carrying
booking events to downstream systems (notifications, settlement,
analytics). A performance tester is asked to find why search feels slow
under load and why downstream booking notifications sometimes lag by
minutes.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "Search is fast sometimes, painfully slow other times, for the exact same route" | Wildly inconsistent search response time |
| 2 | "I get my booking confirmation SMS 5 minutes after I already paid" | Delayed downstream notifications during busy periods |
| 3 | "The DB benchmark our DBA ran looked great, but the app is still slow" | Direct pgbench/redis-benchmark numbers don't match app-level reality |

## Root-cause identification, mapped to Module 4 topics

### 1. Undersized connection pool → pain point #1

**Captured evidence** (15 concurrent uncached searches, real PostgreSQL,
`POOL_MAX_SIZE = 3` in `main.py`):

```
server_latency_ms sorted: 172, 198, 212, 325, 353, 369, 483, 528, 542,
                           649, 694, 707, 814, 859, 876
```

**Root cause:** with only 3 pool connections and each search holding its
connection for ~170-200ms (query + simulated app-level work), the 4th
through 15th concurrent request must queue for a free connection. The
result is a **5x spread in server-side latency** (172ms to 876ms) for
*the exact same query* — purely a function of arrival order, not query
cost. This is the direct explanation for pain point #1: whether a
customer's search feels instant or sluggish depends on how many other
searches happened to land in the same ~1-second window, which is
invisible from a single-request test and only shows up under real
concurrency. `/api/health`'s `postgres_pool` stats make this directly
observable while a load test runs.

### 2. Missing index → the query itself is slow before pooling even matters

The `buses` table has 4,000 rows and no index on
`(source, destination, travel_date)` (see `db/schema.sql`). Each
uncached search does a full table scan — the 172-200ms baseline latency
above is *already* elevated by this before any pool contention is
added. Root causes #1 and #2 compound: a slow query held longer inside
an undersized pool is worse than either defect alone.

### 3. Cache-aside on Redis → dramatic, real improvement

**Captured evidence** (same query, back-to-back):

| Call | Cached? | Server latency |
|---|---|---|
| 1st | No (Postgres) | **187.23ms** |
| 2nd | Yes (Redis) | **0.68ms** |

A **~275x** improvement from the cache-aside pattern in
`/api/buses/search` (30s TTL). This is the clean, expected case —
worth pairing with a discussion of what's *not* modelled here: cache
stampede (many requests missing simultaneously right after a TTL
expiry, all hitting Postgres at once) and hot-key skew (a handful of
popular routes dominating cache traffic). Both are realistic extensions
for the lab if time allows.

### 4. Kafka consumer lag → pain point #2

**Update: broker is now live and app-level publishing is verified real.**
`kafka/docker-compose.yml` (official `apache/kafka:3.7.0` image, KRaft
mode) was brought up and confirmed working:

```
/api/health -> "kafka_available": true, "kafka_error": null
POST /api/bookings -> "kafka_published": true, "kafka_error": null
```

Every booking made through the app now genuinely publishes to a real
broker — verified with multiple live curl calls, not assumed.

**Two real bugs found and fixed getting there** (useful root-cause
practice in themselves):
1. `kafka-python`'s automatic API-version probe hangs indefinitely
   against this broker version — fixed by pinning `api_version=(3,7,0)`
   explicitly on every producer/consumer.
2. `kafka-python-ng`'s `linger_ms > 0` batching path and its
   `producer.flush()` call both hang indefinitely in this exact
   environment (kafka-python-ng 2.2.x + apache/kafka 3.7.0 + Python
   3.14) — isolated by testing individual `future.get(timeout=...)`
   calls, which do work reliably, versus `flush()`, which doesn't.
   `main.py` already used bounded per-call confirmation and was
   unaffected; `producer_perf_test.py` was rewritten the same way.

**Still open: the standalone `producer_perf_test.py` / `consumer_perf_test.py`
scripts hang on sustained multi-message loops** even after both fixes
above, in a way a single ad-hoc burst of 5 messages did not reproduce.
This looks like a further kafka-python-ng issue specific to this Python
3.14 / broker combination that wasn't fully root-caused in the time
spent on it. **Do not trust any producer/consumer lag numbers as
"captured" until these scripts are confirmed working** — if you hit the
same hang, try: a different Python version (3.11/3.12, where
kafka-python-ng is much better tested), or swap to `confluent-kafka`
(the librdkafka-based client) instead of `kafka-python-ng`.

What IS solid to demonstrate live in the lab: the health panel's Kafka
status and the booking response's `kafka_published` field, both backed
by a real broker — enough to show the pattern (transparent
success/failure reporting) even without the standalone lag benchmark.

### 5. Component microbenchmark vs. application-level reality → pain point #3

**Captured evidence, direct component benchmarks:**

| Tool | Result |
|---|---|
| `pgbench` (10 clients, 10s, built-in TPC-B-like workload) | **6,217 TPS**, avg latency **1.6ms** |
| `redis-benchmark` (50 clients, GET/SET) | SET **99,010 rps** (p50 0.287ms), GET **89,847 rps** (p50 0.295ms) |

**Compare to the app-level numbers above:** the app's own uncached
search takes 172-876ms depending on concurrency — two to three orders
of magnitude slower than pgbench's 1.6ms transactions. This is *not* a
contradiction; it's the module's core lesson: **pgbench and
redis-benchmark measure the component in isolation with a trivial,
generic workload** (pgbench's own synthetic tables, simple GET/SET on
short keys). They tell you the component *can* be fast. They say
nothing about your actual query (missing index), your actual pool
sizing, or your actual payload shape. A DBA benchmark result and an
end-user's experience of the same database can both be true and
wildly different — exactly pain point #3, and exactly why the module's
delivery note insists microbenchmarks aren't end-to-end capacity
results.

## How the hands-on lab reproduces this

1. `createdb redbus_lab && psql redbus_lab -f db/schema.sql && python
   db/seed.py` — one-time setup (4,000 buses).
2. Start `main.py`, open the UI, click **"Refresh"** on the health panel
   to see the pool/Redis/Kafka status live.
3. Click **"Search"** then **"Search twice"** to reproduce the
   187ms → 0.68ms cache win directly in the browser.
4. Click **"Fire concurrent searches"** with concurrency=15 to reproduce
   the pool-exhaustion latency spread; watch `/api/health`'s
   `postgres_pool.in_use` hit 3/3 during the run.
5. Run `pgbench -c 10 -j 2 -T 10 redbus_lab` and `redis-benchmark -q -n
   100000 -c 50 -t get,set` directly, and compare their numbers to the
   app-level numbers from steps 3-4 — this is the "component
   microbenchmark vs. application workload" discussion.
6. (Requires Docker) bring up `kafka/docker-compose.yml`, run
   `producer_perf_test.py` and `consumer_perf_test.py --process-delay-ms
   50` concurrently, and watch consumer lag grow in the consumer's
   output.
7. Record bottleneck hypotheses (pool size? index? cache TTL? Kafka
   consumer count vs. partition count?) for Day 2 validation, per the
   lab's last bullet.

## What was actually run vs. what wasn't (honesty log)

| Claim | Status |
|---|---|
| Pool exhaustion latency spread (172-876ms) | **Real, captured** against local Postgres |
| Cache hit vs. miss (187ms vs 0.68ms) | **Real, captured** against local Redis |
| pgbench 6,217 TPS | **Real, captured** — local Postgres, default TPC-B-like workload |
| redis-benchmark ~90-99k rps | **Real, captured** — local Redis |
| Kafka broker connectivity + app-level publish | **Real, verified live** — broker running via docker-compose, `/api/health` and booking responses confirmed via curl |
| Kafka producer/consumer perf-test scripts (lag numbers) | **Still not run successfully** — hit a reproducible hang in kafka-python-ng under sustained multi-message loops on this Python 3.14 setup, not fully root-caused. Two other real bugs in the same area (API-version probe hang, `flush()`/`linger_ms` hang) were found and fixed. Do not use any lag numbers here that aren't freshly captured. |
