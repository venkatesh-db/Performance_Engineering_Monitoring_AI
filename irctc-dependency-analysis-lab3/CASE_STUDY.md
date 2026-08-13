# IRCTC Use Case — Deep Dependency Analysis Pain Points & Root Cause
### Day 2 / Module 3: Deep Dependency Analysis

## Scenario

IRCTC's availability-check and booking-event pipeline touches three
dependencies at once: PostgreSQL (availability lookups), Kafka
(downstream booking-event fan-out), and Redis (a cache in front of
Postgres). A performance tester is asked to correlate symptoms across
all three and find which dependency is actually responsible for each
complaint — not "the database is slow," but the specific index, the
specific partition count, the specific eviction, the specific retry
pattern.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "DBA says the query plan looks fine, but writes to availability are slow" | EXPLAIN shows a good plan; the cost is elsewhere (index maintenance, not query cost) |
| 2 | "Booking confirmations for some trains lag way behind others during a rush" | Kafka consumer lag differs by topic/partition config, not by train |
| 3 | "Even our most popular train's availability check goes stale/slow randomly" | A "hot" cache key gets evicted anyway under heavy unrelated traffic |
| 4 | "One slow dependency call seems to make everything worse, not just that one path" | Retries on a struggling Postgres pool pile on more load, not less |

## Root-cause identification, mapped to Module 3 topics

### 1. EXPLAIN evidence + the real cost of an unnecessary index → pain point #1

**Captured evidence** (real `EXPLAIN (ANALYZE, FORMAT JSON)` against a
6,000-row `availability` table, real `pg_stat_user_indexes`):

```
Query: ... WHERE train_no=$1 AND travel_date=$2 AND is_active=true
Node Type: Index Scan on idx_availability_train_date
Planning: 0.457ms  Execution: 0.123ms
```

```
pg_stat_user_indexes:
  idx_availability_train_date  idx_scan=2      size=352KB  <- actually used
  idx_availability_is_active   idx_scan=0      size=64KB   <- NEVER used, ever
```

**Root cause:** `is_active` is ~95% `true` across the table — a
textbook low-selectivity column. The planner correctly ignores
`idx_availability_is_active` even when the query explicitly filters on
it, because a composite index on `(train_no, travel_date)` is already
far more selective. `idx_scan=0` is the decisive proof a tester needs:
this index has **never once** been used by the planner, yet it exists,
occupies 64KB, and — critically — every `INSERT`/`UPDATE` on the table
still has to maintain it. (A direct timing comparison of bulk updates
with/without the index was too noisy to show cleanly on this machine's
fast local SSD at this table size — `pg_stat_user_indexes` is the
reliable evidence, and is what a real tester would reach for over a
noisy microbenchmark.) This is pain point #1 exactly: the query plan is
genuinely fine; the cost is a write-path tax nobody is getting any
read-side benefit from.

### 2. Kafka partition parallelism vs. consumer lag → pain point #2

**Captured evidence** (real broker, real producer/consumer, same
150-message burst, same 20ms per-message processing delay, only the
partition/consumer count changed):

| Topic | Partitions | Consumers | Max consumer lag |
|---|---|---|---|
| `availability.events.p1` | 1 | 1 | **471ms** |
| `availability.events.p3` | 3 | 3 | **26ms** |

**Root cause:** a 1-partition topic can only ever be consumed by 1
consumer within a group — Kafka's partition is the unit of parallelism,
not the consumer count you configure. Three consumers pointed at a
1-partition topic would still bottleneck on that single partition. The
3-partition topic lets 3 consumers genuinely split the 150-message
backlog roughly evenly (54/38/58 in this run) and process it in
parallel, cutting max lag by **~18x** for identical per-message cost.
This directly explains pain point #2: if the real system provisions
enough consumer *instances* but the topic itself has too few
*partitions*, lag persists regardless of how many consumers you add —
the fix is partition count, not consumer count.

**Honesty note on getting this measurement right:** the first two
attempts at this comparison produced garbage lag numbers (72 seconds,
44 seconds) that were pure artifacts of the *time between running
commands* in this session, not real system lag — and a separate real
bug surfaced along the way: three consumer threads in the same Python
process defaulted to colliding `client_id`s, which silently caused the
whole 3-consumer group to consume nothing at all. Fixed by giving each
consumer thread an explicit, unique `client_id`. The 471ms/26ms numbers
above are from a clean run with that fix in place, producer and
consumers running genuinely concurrently.

### 3. Redis hot keys, eviction, and the limits of "hot key protection" → pain point #3

**Captured evidence** (isolated Redis container, `maxmemory=3MB`,
`allkeys-lru`, real `INFO` stats):

```
Before pressure: used_memory=1.18MB, evicted_keys=0
After ~15,000 synthetic distinct-key lookups: used_memory=3.147MB (at cap), evicted_keys=1003
GET avail:12000:2026-08-20 (the "hot" key) immediately after: (nil) -- evicted too
```

**Root cause:** under `allkeys-lru`, a key is only protected from
eviction by being *recently* accessed — being logically "hot" in your
traffic model doesn't matter if a flood of cold, never-repeated keys
arrives fast enough to push it out of the recency window before its
next real hit. This is pain point #3's exact mechanism: the naive
assumption "our most popular train's cache entry is safe" is false
under `allkeys-lru` if noisy/low-value traffic (bots, scanners, or in
this lab's case, synthetic one-off lookups) floods the same Redis
instance. The fix isn't "add more cache" — it's either a dedicated
key space / separate cache instance for known-hot keys, or a
`volatile-lru` policy with explicit longer TTLs on hot keys so they
aren't competing on equal footing with disposable ones.

### 4. Backpressure and retry amplification → pain point #4

**Captured evidence** (Postgres pool capped at 3 connections, 30
concurrent callers, each needing one 300ms query):

| Retry strategy | Total attempts issued | Successes | Amplification factor |
|---|---|---|---|
| Naive (immediate retry, no cap) | **165** | 30/30 (100%) | 5.5x |
| Backoff (exponential, capped at 3 tries) | **81** | 9/30 (30%) | 2.7x |

**Root cause, and an honest tradeoff, not a clean "backoff wins" story:**
naive retries eventually get every caller through, but at **5.5x** the
real load on an already-saturated Postgres pool — exactly pain point
#4's "one slow dependency makes everything worse": each failed caller
immediately retries, adding to the very queue that made it fail in the
first place, a genuine amplification loop. Backoff cuts that load
almost in half (2.7x) by spacing retries out and giving up after 3 —
but the direct cost is that **70% of callers never succeeded** within
their retry budget. Neither strategy alone is "correct" for IRCTC's
real Tatkal traffic: the right answer is backoff *plus* either a higher
retry cap for user-facing calls, a queue with graceful degradation
("you're in a virtual queue"), or a circuit breaker that stops
new attempts entirely once the pool is provably saturated rather than
letting either strategy keep hammering it.

## How the hands-on lab reproduces this

1. Section 1: run EXPLAIN, then click "Show real pg_stat_user_indexes"
   before and after creating the low-selectivity index — confirm
   `idx_scan` stays 0 no matter how many queries you run.
2. Section 2: produce to both Kafka topics, then run
   `kafka/consumer_lag_test.py` against each from a terminal (see exact
   commands below) — compare max lag.
3. Section 3: fire the hot-key storm repeatedly (10+ times) until
   `evicted_keys` in the stats panel is nonzero, then immediately
   `redis-cli -p 6380 get avail:12000:2026-08-20` — discuss whether the
   hot key survived.
4. Section 4: fire naive vs. backoff retry storms at concurrency=30 —
   compare `total_attempts` and `total_successes`, and discuss which
   tradeoff IRCTC should actually choose for Tatkal traffic.

## Exact commands for section 2 (run in a terminal, not the UI)

```bash
# after clicking "Produce 150 events to p1 topic" in the UI:
python kafka/consumer_lag_test.py --topic availability.events.p1 --num-consumers 1 --process-delay-ms 20

# after clicking "Produce 150 events to p3 topic" in the UI:
python kafka/consumer_lag_test.py --topic availability.events.p3 --num-consumers 3 --process-delay-ms 20
```
