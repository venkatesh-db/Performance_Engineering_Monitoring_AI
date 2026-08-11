# redBus Use Case — Customer Pain Points & Root-Cause Analysis
### Module 1: Introduction to Performance Engineering

## Scenario

A performance tester is validating the "search buses → select seats" flow
on a redBus-style booking site ahead of a festival-season traffic spike
(historically 5–8x normal concurrent users).

## Customer-facing pain points (what support tickets / app reviews say)

| # | Customer complaint | Where it shows up in the UI |
|---|---|---|
| 1 | "Search takes forever during evening hours" | Spinner on the search results page for 3–8s |
| 2 | "App feels fine alone, but crawls when I try during a sale" | Search works instantly with few users, degrades sharply under load |
| 3 | "Seat map takes longer to load for bigger buses (sleeper coaches)" | Seat grid population time scales with seat count |
| 4 | "I got a payment timeout but money was deducted" | Errors appear specifically at peak, not at low load |

## Captured evidence (real Locust run against this repo, 2026-08-11)

| Scenario | Users | RPS | P50 | P95 | P99 | Max | Errors |
|---|---|---|---|---|---|---|---|
| Baseline | 10 | 6.0 | 79ms | 170ms | 300ms | 312ms | 0% |
| Peak | 200 | 20.1 | 7,900ms | 8,700ms | 8,900ms | 9,000ms | 0% |

(`baseline_10u_stats.csv` / `peak_200u_stats.csv` in this repo have the raw numbers.)

## Root-cause identification (performance tester's investigation)

Mapped directly to Module 1 concepts:

### 1. Response time vs. throughput divergence → saturation, not just utilization
Baseline (10 users) delivered 6.0 RPS at 79ms median. At 200 users, RPS
only rose to 20.1 — a 20x increase in offered load produced barely 3x
more throughput, while median latency jumped **100x** (79ms → 7,900ms).
By Little's Law (`L = λW`), a system with unconstrained capacity would
keep λ (RPS) scaling roughly with users while W (response time) stays
flat. Here throughput is nearly flat and W absorbs almost the entire
load increase — the textbook signature of a saturated resource (the
single locked DB connection), not a CPU-bound app tier.

### 2. Every percentile degrades together, not just the tail
This system doesn't show the "P50 fine, P95/P99 bad" pattern some
bottlenecks produce — because a single global lock serializes *every*
request equally, P50 (7,900ms), P95 (8,700ms) and P99 (8,900ms) all
collapsed together at peak. That's itself a diagnostic clue: when the
whole distribution shifts uniformly instead of just the tail, suspect a
single shared serialization point (a lock, a connection, a queue)
rather than a resource that only some requests contend for (e.g. a hot
cache key or a slow downstream call affecting a subset of traffic).

### 3. Root cause #1 — DB connection serialization
Instrumentation (APM / thread dump / simple timing harness) shows every
`/api/search` and `/api/seats` call queues behind a single lock before
touching the database. Concurrent requests **serialize** instead of
running in parallel — utilization of the app server CPU stays low even
as response time explodes, which is the tell that the bottleneck is a
shared resource with no pooling, not raw compute.

### 4. Root cause #2 — missing index / full table scan
Even once a request gets the lock, `WHERE source=? AND destination=?
AND travel_date=?` has no supporting index, so query cost grows with
catalog size. This explains complaint #2: the same query is fast on a
small dataset and slow once the operator catalog grows — a regression
that functional testing alone would never catch.

### 5. Root cause #3 — N+1 query in seat map
`/api/seats/{bus_id}` issues one query per seat instead of a single
batched query. Response time for this endpoint scales **linearly with
seat count**, explaining complaint #3 (sleeper coaches with more berths
are disproportionately slow).

### 6. Errors appear only at peak → coordinated omission / timeout cascade
Payment-timeout-but-money-deducted (#4) is the visible symptom of a
request that got seat-map latency added on top of DB lock wait time,
exceeding the payment gateway's client-side timeout while the backend
transaction still completed — a reminder to test **saturation
behavior and error rate together**, not response time in isolation.

## How the hands-on lab reproduces this

1. Start `main.py` (FastAPI + SQLite, seeded with 4,000 buses).
2. Run a controlled baseline with `locustfile.py` at low concurrency —
   record P50/P95/P99, RPS, error rate.
3. Ramp concurrency and re-run — watch throughput plateau while P95/P99
   diverge from P50 (Little's Law violation signature).
4. Confirm the load generator itself isn't the bottleneck (check Locust
   worker CPU) before blaming the server.
5. Correlate the plateau with the DB lock (root cause #1), the search
   endpoint's per-request cost growing with catalog size (root cause #2),
   and seat-map latency scaling with bus size (root cause #3).

## Fixes to discuss (not implemented in the demo, left as an exercise)

- Use a connection pool (e.g. SQLAlchemy pool or per-request connections)
  instead of one shared connection + global lock.
- Add a composite index on `(source, destination, travel_date)`.
- Replace the seat N+1 loop with a single `WHERE bus_id = ?` query
  returning all seat rows.
