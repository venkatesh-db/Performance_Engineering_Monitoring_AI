# Hands-on lab prompt for Claude — Day 3 / Module 1

```
I instrumented a Licious-style order API with real Prometheus metrics
and ran real PromQL queries against a real Prometheus server. Here's
what I captured:

CARDINALITY BUG (found and fixed):
Before fix: endpoint label = "/api/products/3", "/api/products/5", etc
  (one series per product ID)
After fix: endpoint label = "/api/products/{product_id}" (one series total)

DB POOL SATURATION (pool capped at 4 connections):
Idle: db_pool_in_use=0/4
Mid-burst (50 concurrent product lookups): db_pool_in_use=4/4 (100%)

RATE-WINDOW SENSITIVITY (same query, different timing/window):
Right after a 2s/60-request burst, queried with [5m]: histogram_quantile(...) = NaN
Right after a 15s sustained burst, queried with [1m]: histogram_quantile(...) = 0.477s

CLEAN SIGNALS:
Cache hit ratio: 75.4%
Error rate (5xx): 0% (empty result set -- zero matching series)

Acting as a performance tester writing the Module 1 lab deliverable, do
the following:
1. Explain in one paragraph, for a teammate who's never seen
   Prometheus, why the pre-fix cardinality bug would eventually degrade
   or crash a real Prometheus server as the product catalog grows, even
   though each individual query "worked."
2. Explain the NaN result precisely: what does rate() actually compute
   over a [5m] window when all the meaningful traffic happened in the
   first 2 seconds, and why does histogram_quantile() propagate that as
   NaN rather than some other value?
3. Given the db_pool_in_use=4/4 finding, write the PromQL alerting rule
   you'd add (as a promql expression, not just prose) to page someone
   BEFORE pool exhaustion causes user-facing errors, and justify the
   threshold you chose.
4. A support agent insists "the error-rate dashboard shows 0%, so
   nothing is wrong" while a customer reports a failed checkout. Write
   the two most likely explanations consistent with a genuinely correct
   0%-error Prometheus reading, and what each would look like in a trace
   (tie back to the Day 2 Module 4 OpenTelemetry lab if you've done it).

Cite my actual numbers throughout, not generic PromQL advice.
```

## Notes
- Attach `main.py` and `CASE_STUDY.md` so Claude reasons from the real
  middleware code (the exact bug and fix), not a hypothetical.
- If you have your own Grafana/Prometheus screenshots from running the
  lab, describe what you see and ask Claude to reconcile it against the
  numbers above.
