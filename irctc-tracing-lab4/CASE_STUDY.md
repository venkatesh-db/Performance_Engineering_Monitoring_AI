# IRCTC Use Case — OpenTelemetry & Jaeger Pain Points and Root Cause
### Day 2 / Module 4: OpenTelemetry and Jaeger

## Scenario

IRCTC's payment flow crosses four boundaries in one logical transaction:
an HTTP request, a Redis idempotency check, a PostgreSQL charge, and a
Kafka event that a separate consumer process turns into an SMS
notification. A performance tester is asked to find which single step
is actually slow when customers report "payment took forever" — using
one real, followable trace instead of guessing from four separate
dashboards.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "Payment confirmation email/SMS came way after I already saw 'success' in the app" | The notification happens in a different process, on a different timeline, from the HTTP response |
| 2 | "Sometimes payment is instant, sometimes it takes over half a second, no pattern" | One specific span, not the whole request, is intermittently slow |
| 3 | "The tracing dashboard shows way more traces than we expected from our sample rate config" | A sampling-config bug makes cost/volume assumptions wrong |
| 4 | "A payment over ₹1,00,000 got silently blocked with no clear reason in the logs" | An error condition needs to be visible in the trace, not just a generic 402 |

## Root-cause identification, mapped to Module 4 topics

### 1. Context propagation across HTTP and Kafka → pain point #1

**Captured evidence** (real trace, `irctc-payment-api` and
`irctc-payment-consumer` are two separate OS processes):

```
[irctc-payment-api]      POST /api/payment          58,320us
[irctc-payment-api]        cache-check-idempotency    1,409us
[irctc-payment-api]        fraud-check               51,074us
[irctc-payment-api]        db-charge                  2,402us
[irctc-payment-api]        kafka-publish              2,603us
[irctc-payment-consumer]   notify-customer          151,253us   <- different PROCESS, same trace_id
```

**Root cause / mechanism:** `main.py`'s `kafka-publish` span calls
OpenTelemetry's `inject(headers)`, which writes a `traceparent` header
containing the current trace_id and span_id into the Kafka message.
`consumer.py`, running as a completely separate process consuming that
message later, calls `extract(headers)` to reconstruct that context and
starts `notify-customer` as its child — Jaeger shows it nested under
the exact same trace, in a different service, correctly ordered after
`kafka-publish`. This is the direct, provable explanation for pain
point #1: the notification is a genuinely separate, later step in the
same logical transaction, not a bug — and now it's *visible* as such,
instead of looking like an unrelated delay.

### 2. Identifying the slowest span / critical path → pain point #2

**Captured evidence**, same request shape, `slow_db=true`:

```
POST /api/payment      684,577us  (total)
  cache-check-idempotency  2,698us
  fraud-check              50,880us
  db-charge               617,719us   <- 90% of total request time
  kafka-publish            10,546us
```

**Root cause:** `db-charge` dominates the trace when a simulated lock
contention path is triggered — 617ms out of a 685ms total request, i.e.
the critical path is almost entirely this one span. A tester looking at
end-to-end latency alone would only know "payment is slow sometimes";
the trace makes it immediate and unambiguous which specific span, in
which specific dependency, is responsible — directly answering pain
point #2 without needing separate Postgres-side correlation.

### 3. Sampling strategy — a real bug found while building this lab → pain point #3

**What actually happened, in order:**

1. First implementation: a custom `Sampler` subclass, used directly
   (not wrapped in anything), toggled to 20%.
2. Fired 40 requests. Expected ~8 traces in Jaeger. **Got 38.**
3. Root cause, found by direct experiment: OpenTelemetry's SDK calls
   `should_sample()` for **every span**, not just the root — verified
   directly:
   ```
   SAMPLER CALLED for root-span
   SAMPLER CALLED for child-span
   ```
   With ~8 spans per request (HTTP root + 2 ASGI sub-spans + 4 manual
   spans + occasionally more), the probability that **at least one**
   independently rolls "sample" at 20% each is `1 - 0.8^8 ≈ 83%` — which
   is almost exactly the ~90-95% actually observed.
4. **Fix:** wrap the sampler in `ParentBased(...)` — the standard
   OpenTelemetry pattern, which makes the sampler decision ONLY at the
   root span and has every child span inherit that decision rather than
   re-rolling. Re-ran the identical 40-request test:

| Sample rate | Traces landed (of 40) |
|---|---|
| 1.0 (always on), before fix | 40 |
| 0.2, **before** the `ParentBased` fix | 38 (should have been ~8) |
| 0.2, **after** the `ParentBased` fix | **5** (expected ~8, within normal variance) |
| 1.0, after fix (sanity check) | 40 |

**This is pain point #3's real mechanism, and a genuinely common
mistake**: any custom or third-party sampler that isn't wrapped in
`ParentBased` will silently sample far more than its configured rate,
which corrupts both trace-volume cost estimates and any "X% sampled"
assumption downstream teams make about the data.

### 4. Span status and events on failure → pain point #4

**Captured evidence** (real Jaeger tag query for `error=true` on the
`fraud-check` span):

```json
{
  "otel.status_code": "ERROR",
  "otel.status_description": "HTTPException: 402: Payment blocked by fraud check",
  "payment.amount": 999,
  "error": true
}
```

**Root cause / fix:** `fraud-check`'s manual span explicitly calls
`span.set_status(Status(StatusCode.ERROR, "..."))` and
`span.add_event("fraud check failed", {"reason": "amount_exceeds_threshold"})`
before the endpoint raises `HTTPException(402, ...)`. Without this,
Jaeger would show a span that merely *contains* a 402-raising request
with no dedicated error marker on the span most responsible — a tester
would have to guess which of the 4 manual spans caused the failure.
With it, filtering Jaeger by `error=true` goes directly to the
responsible span and its structured reason, resolving pain point #4.

## How the hands-on lab reproduces this

1. Start `main.py`, then `python -u consumer.py` in a second terminal
   (the `-u` flag avoids Python's stdout buffering when redirected).
2. Click "Pay" in the UI, open the returned trace link in Jaeger,
   confirm `notify-customer` appears under `irctc-payment-consumer`.
3. Check "simulate slow DB span," pay again, and identify `db-charge`
   as the dominant span in the new trace.
4. Set sample rate to 0.2, fire a batch of 40, and count how many
   traces actually appear in Jaeger for `irctc-payment-api` in that
   window — discuss why it should be close to 8, not close to 40.
5. Fire a payment with `force_fraud_check_fail=true`, and in Jaeger
   filter by `error=true` to find the exact span and reason.
6. Correlate: if you ran a load test in an earlier module against a
   similar payment endpoint, compare its aggregate P95 latency finding
   against what a single representative trace shows about *which* span
   contributes most to that P95 — this is the "correlate trace evidence
   with the Day 1 load-test result" lab step.

## Honesty notes

- Every number above is real, captured from this repo's running
  processes and a real Jaeger instance (`docker run
  jaegertracing/all-in-one:1.57`, OTLP/HTTP on :4318, UI on :16686) —
  none of it is illustrative or assumed.
- The sampling section documents an actual bug this build hit and
  fixed, in order, rather than presenting the corrected code as if it
  were right the first time.
- `consumer.py`'s terminal output is buffered when redirected to a log
  file and may appear empty even though it's working — the proof used
  throughout this document is Jaeger's own trace data, not the
  consumer's stdout.
