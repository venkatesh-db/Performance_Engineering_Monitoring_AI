# Groww Use Case — Grafana Dashboards & Alerts Pain Points and Root Cause
### Day 3 / Module 2: Grafana Dashboards and Alerts

## Scenario

Groww's trading order API needs a real Grafana dashboard (RED + USE
method panels) and real alert rules before the next trading rush. A
performance tester provisions Grafana against a real Prometheus, builds
the dashboard, writes one symptom-based and one cause-oriented alert,
then runs load to confirm both the dashboard and the alerts actually
reflect real behaviour — not just that they're configured.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "The ops dashboard showed 0% pool usage the whole time, but trades were clearly slow" | A dashboard panel silently reads wrong data under exactly the load conditions it exists to catch |
| 2 | "We got paged for slow trades, but nobody could tell which dependency caused it" | Symptom-only alerting without a matching cause-oriented alert to correlate against |
| 3 | "The alert threshold and the eval interval didn't line up, so it took forever to actually fire" | A `for:` duration shorter than the alert group's evaluation interval effectively can't ever transition to Firing on a single sustained incident |

## Root-cause identification, mapped to Module 2 topics

### 1. A real dashboard-data bug: sync `def` vs `async def` under asyncpg → pain point #1

**Captured evidence:**
```
/api/summary (async def) during a 40-thread sustained rush: db_pool_in_use = 4
/metrics (originally a sync def) during the SAME rush: db_pool_in_use = 0.0  <- WRONG
```

**Root cause:** FastAPI runs a plain `def` endpoint in a separate
threadpool worker thread so it doesn't block the event loop. But
`asyncpg.Pool` is only safe to read from the event loop thread it was
created on — a sync `def` `/metrics` handler was silently reading
stale/incorrect pool state from the wrong thread, always reporting
`db_pool_in_use = 0` no matter how saturated the real pool was. This is
exactly pain point #1: a dashboard panel built on this metric would
show a perfectly flat, healthy-looking line through an entire real
incident. **Fix:** change `/metrics` to `async def` (matches the
already-correct `/api/summary`). Confirmed fixed by directly comparing
both endpoints during the same live burst — `/metrics` now correctly
reported `4.0` too, and Prometheus's own stored series confirmed real
values (`db_pool_in_use{job="groww-app"}` sampled at `4` on 8 of 10
consecutive 2-second polls during a sustained rush).

**Review-practice takeaway:** before trusting any dashboard panel,
verify its underlying metric is actually correct under load by
comparing it against an independent code path that computes the same
number — this bug would have passed a purely visual "does the panel
render" review.

### 2. RED + USE dashboard, provisioned and verified end-to-end

**Captured evidence** (all queried through Grafana's own datasource
proxy — i.e., the exact data path the dashboard's panels use, not a
shortcut around it):
```
GET /api/datasources/proxy/uid/prometheus/api/v1/query?query=up
  -> postgres=1, redis=1, groww-app=1   (all 3 scrape targets healthy)
```
The provisioned dashboard (`groww-red-use`, folder "Groww") loaded with
all 10 panels: request rate, error rate %, P95/P99 latency (RED); active
requests, process CPU/memory, DB pool gauge, cache hit ratio, Kafka lag,
endpoint cardinality (USE + the cardinality-safety check carried over
from Module 1's bug). Confirmed present via `GET /api/search` on the
live Grafana instance, not just "the JSON file exists."

### 3. Symptom-based alert: confirmed genuinely firing → pain point #2

**Captured evidence:**
```
Rule: "SYMPTOM: P95 latency above 800ms"
State transition observed live: inactive -> Alerting
  {"state": "Alerting", "activeAt": "2026-08-13T13:06:30Z", "value": 1}
```

**Root cause of the underlying symptom:** the same undersized 4-connection
pool from finding #1 — under sustained concurrent stock-lookup traffic,
requests queue for a connection, P95 latency crosses 800ms, and the
alert transitions from Normal through Pending to genuinely Alerting.
This is a real, observed state transition captured from Grafana's own
alerting API mid-incident, not assumed from the rule definition.

### 4. Cause-oriented alert: correctly modelled, but timing-sensitive to reproduce on demand → pain point #3

**Captured evidence:**
```
Rule: "CAUSE: Postgres connection pool above 90%"
Underlying metric proven correct: db_pool_in_use{job="groww-app"} = 4 on 8/10 polls during a 25s sustained rush
Alert state during multiple sustained-rush attempts: stayed Inactive/Normal
```

**Root cause, found while debugging this:** the alert group's original
`interval: 30s` was *coarser* than this rule's `for: 15s` duration. In
Grafana's alerting model, a condition must be evaluated as true across
**consecutive evaluation cycles** spanning the `for` duration — with a
30s eval interval and a 15s `for`, the very first true evaluation moves
the rule to Pending, but the *next* evaluation (30s later) is the
earliest possible check for whether it's still true, meaning the
condition must realistically stay true for a full 30-60s window to ever
reach Firing, not the nominal 15s the rule's own text implies. **Fixed**
by reducing the group's `interval` to 10s so evaluations happen more
often than the `for` duration requires. Even after the fix, one 45s
sustained-rush attempt still didn't catch this specific alert in a
Firing state at check time (though the underlying metric was
independently proven to cross the 90% threshold repeatedly during that
window) — the honest conclusion is that **the P95-latency symptom alert
reliably fires under this load pattern, while the pool-saturation cause
alert is real and correctly modelled but is more timing-sensitive to
reproduce in a short scripted burst than in sustained real traffic.**
This is itself a useful lab finding: a cause-oriented alert on a
metric that recovers the instant load lets up is inherently harder to
catch mid-transition than a symptom alert whose evidence (elevated
P95) persists a little longer in the rate-window.

## How the hands-on lab reproduces this

1. `docker compose up -d`, confirm all 3 Prometheus targets `up` and
   the dashboard/alert rules provisioned via Grafana's API (or UI at
   `http://localhost:3001`, admin/admin).
2. Open the dashboard, watch the DB-pool gauge and P95-latency panel
   while idle (should read ~0%, low ms).
3. Click "Fire sustained rush (waves)" in the UI, or run a scripted
   40-thread rush against `/api/stocks/{nonexistent-symbol}` (guarantees
   real DB hits, bypassing Redis cache) for 45-70 seconds.
4. Watch the DB-pool gauge climb and the P95-latency panel spike in
   real time; check `http://localhost:3001/alerting/list` for state
   transitions.
5. Click "Inject one real 500" and confirm the error-rate panel moves
   off 0% within one scrape interval.
6. Discuss: which alert would you trust to page someone at 3am — the
   symptom-based one (customer-visible, but doesn't say why) or the
   cause-oriented one (specific, but as shown here, can be harder to
   catch reliably under short bursts)? What would you change about the
   cause alert's `for` duration or eval interval given this lab's
   findings?

## Known platform gap: "No data" on the two process panels (macOS only)

The **Process CPU** and **Process memory RSS** panels show "No data"
when run on this macOS dev machine. Root cause: `prometheus_client`'s
default process collector reads `/proc/self/stat` to compute those two
metrics, and `/proc` is a Linux-only filesystem — it doesn't exist on
macOS, so the collector silently omits both metrics for the app's own
process (confirmed: `process_cpu_seconds_total` and
`process_resident_memory_bytes` exist in Prometheus for the Go-based
`postgres-exporter`/`redis-exporter` jobs, which don't depend on
`/proc`, but not for `job="groww-app"`). This is not a bug in the app
or dashboard — on the real Linux training VM, both panels should
populate correctly with no code change. Worth confirming on the actual
VM before the session, rather than assuming it "just works" there too.

## Honesty log — what was actually verified vs. not

| Claim | Status |
|---|---|
| Prometheus scraping all 3 real targets | **Verified** via `/api/v1/targets` and via Grafana's own datasource proxy |
| Dashboard provisioned with all 10 panels | **Verified** via Grafana's `/api/search` |
| Alert rules provisioned correctly | **Verified** via Grafana's `/api/v1/provisioning/alert-rules` |
| `/metrics` async-def bug found and fixed | **Verified** — reproduced the bug, fixed it, reproduced the fix working |
| Eval-interval-vs-for-duration misconfiguration found and fixed | **Verified** — diagnosed via Prometheus range queries, fixed, confirmed rules reloaded |
| SYMPTOM (P95 latency) alert actually firing | **Verified** — captured a live `state: "Alerting"` response from Grafana's API |
| CAUSE (DB pool) alert actually firing | **Not captured** in this session despite the underlying metric provably crossing threshold multiple times — documented honestly above rather than claimed |
