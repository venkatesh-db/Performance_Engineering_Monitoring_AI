# Licious Use Case — Prometheus/PromQL Pain Points & Root Cause
### Day 3 / Module 1: Prometheus Metrics and PromQL

## Scenario

Licious (meat/seafood delivery) runs a flash-sale-style demand spike
whenever a popular cut goes on discount. A performance tester is asked
to instrument the order API with Prometheus, then use PromQL alone —
no application logs, no code changes — to find what breaks under load
and why.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "Add to cart / checkout felt fine, then suddenly every request queued" | Latency degrades sharply and non-linearly, not gradually |
| 2 | "A brand-new label showed up for every single product page view" | Prometheus/Grafana becomes sluggish, disk usage climbs unexpectedly |
| 3 | "Support says '0 errors' but customers reported failed checkouts" | Error-rate dashboards can look clean while a real problem exists elsewhere |
| 4 | "The dashboard says everything's fine, but only right this second" | A metric queried moments after a spike ends looks deceptively calm |

## Root-cause identification, mapped to Module 1 topics

### 1. Cardinality control — a real bug found while building this lab

**Captured evidence:** while wiring up the automatic HTTP-metrics
middleware, `endpoint` labels came out as `/api/products/3`,
`/api/products/5`, `/api/products/11`, ... — one distinct label value
**per product ID**, instead of the templated route
`/api/products/{product_id}`. Confirmed via a real Prometheus query:

```
http_requests_total{endpoint="/api/products/3"}  -- a separate series
http_requests_total{endpoint="/api/products/5"}  -- another separate series
...
```

**Root cause:** the middleware read `request.scope.get("route")`
*before* calling `await call_next(request)` — but Starlette only
populates `scope["route"]` with the matched, templated route **during**
routing, which happens inside `call_next()`. Read too early, it falls
back to the raw resolved URL path. Fixed by moving the route lookup to
*after* `call_next()` returns. This is exactly pain point #2's
mechanism at real scale: an e-commerce catalog with thousands of SKUs
would create thousands of permanent time series for what should be one
logical endpoint — the textbook Prometheus cardinality explosion, caught
here because we actually ran the instrumentation and looked at real
label values instead of assuming the middleware was correct.

### 2. Connection-pool saturation under a flash sale → pain point #1

**Captured evidence** (real, `/api/summary` sampled mid-burst against a
pool capped at 4 connections):

```
Idle:      db_pool_in_use = 0/4
Mid-burst (50 concurrent product lookups): db_pool_in_use = 4/4  (100%)
```

**Root cause:** `db_pool_in_use` (a Gauge, updated from the live
`asyncpg.Pool` on every scrape) is the PromQL-visible signal for
`100 * db_pool_in_use / db_pool_max_size` climbing to 100% the instant
concurrent product lookups exceed the pool's 4-connection capacity —
this is the exact query the hands-on lab asks for. Correlated with the
P95 latency query on `/api/products/{product_id}` (see below), the
tester can prove causation, not just coincidence: pool saturation and
latency spike happen in the same request window.

### 3. Rate-window sensitivity — a real, reproducible PromQL gotcha → pain point #4

**Captured evidence:** the exact same PromQL query returned two
different-looking "correct" answers depending on timing and window:

```
Right after a 2-second, 60-request burst, queried with [5m]:
  histogram_quantile(0.95, sum(rate(...[5m])) by (le)) -> NaN (all buckets rate()=0)

Immediately after a 15-second SUSTAINED burst, queried with [1m]:
  histogram_quantile(0.95, sum(rate(...[1m])) by (le)) -> 0.477s (477ms)
```

**Root cause, and the actual teaching point:** `rate()` computes an
*average* increase per second over its window. A short, sharp burst
that ends well before the window closes gets averaged down toward zero
— the query isn't wrong, it's honestly reporting that *over the last 5
minutes*, load was mostly quiet. This is pain point #4 exactly: a
tester glancing at a `[5m]` panel moments after an incident spike can
see a deceptively calm graph and conclude "nothing happened" — the fix
isn't a different metric, it's matching the query window to the
timescale of the event you're investigating (a 15-second spike needs a
`[1m]` or shorter window, not `[5m]`).

### 4. Cache hit ratio and error rate — clean signals, correctly showing "no problem here" → pain point #3

**Captured evidence:**
```
sum(rate(cache_hits_total[1m])) / (hits+misses) -> 0.7536  (75.4% hit ratio)
100 * sum(rate(http_requests_total{status=~"5.."}[1m])) / sum(rate(http_requests_total[1m])) -> empty result (0 matching series = 0% error rate)
```

**Why this matters for pain point #3:** an empty PromQL result for the
error-rate query means literally zero 5xx responses were recorded — a
real, correct "all clear" from the metrics layer. If a real Licious
customer reports a failed checkout despite this, the tester's next move
isn't to distrust the dashboard — it's to check whether the failure
happened client-side (network drop before the request reached the
server, so it never got counted at all) or in a dependency this
specific app-level metric doesn't cover (e.g. a payment gateway
timeout that returns a 200 with a business-level decline, the same
"business error vs. infra error" distinction from Day 2's dependency
lab). A clean Prometheus dashboard proves the *instrumented* path had
no errors — it doesn't prove nothing went wrong anywhere.

## How the hands-on lab reproduces this

1. Start `main.py`, confirm `/metrics` returns real Prometheus
   exposition format (`curl http://localhost:8008/metrics`).
2. Bring up real Prometheus + exporters:
   `docker compose -f prometheus/docker-compose.yml up -d`, confirm all
   three targets show `"health": "up"` at
   `http://localhost:9090/api/v1/targets`.
3. Click "Fire flash sale" in the UI (or run it a few times over ~15s
   for a sustained burst, not one instant spike), then use section 5's
   PromQL links to open each query live in Prometheus.
4. Compare a `[5m]` vs `[1m]` window on the same P95-latency query
   immediately after a burst — reproduce the NaN-vs-real-number
   contrast from finding #3 above.
5. Check `endpoint` label cardinality with
   `count(count by (endpoint) (http_requests_total))` — confirm it
   stays at a small, fixed number (one per route) rather than growing
   with every distinct product ID.

## What was actually run vs. what wasn't (honesty log)

Every number in this document is real, captured against a genuinely
running Prometheus server (`http://localhost:9090`) scraping this
app's real `/metrics` endpoint plus real `postgres_exporter` and
`redis_exporter` containers — verified via `/api/v1/targets` showing
all three as `up`, not assumed. The cardinality bug and the rate-window
NaN behavior were both discovered by actually querying, not written
into the case study from a template.
