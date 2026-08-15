# Flipkart Use Case — Loki & Structured Logging Pain Points and Root Cause
### Day 3 / Module 3: Loki and Structured Logging

## Scenario

Flipkart's order-and-payment flow spans a FastAPI order service and a
Kafka-based payment consumer. A performance tester is asked to make one
customer's order findable end-to-end — across logs, metrics, and
traces — using nothing but the IDs returned from the API, and to
demonstrate why Loki's label design matters before it becomes a real
production incident.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "Support couldn't find my order's logs even with the order ID I gave them" | No shared correlation ID across services, or the ID buried in unstructured text |
| 2 | "My payment took forever and nobody noticed" | Slow payment events exist in logs but aren't queryable without the right LogQL |
| 3 | "We found a customer's phone number in plaintext in our logs" | A masking gap that "looks" complete (email is masked) but isn't |
| 4 | "Loki got slow and expensive practically overnight" | Uncontrolled label cardinality, silently, before anyone measured it |

## Root-cause identification, mapped to Module 3 topics

### 1. Loki label cardinality — the real, measured cost of getting this wrong → pain point #4

**Captured evidence** (pushed directly to a real Loki instance, then
queried its real `/loki/api/v1/series` endpoint):

```
20 log events with transaction_id as a LOKI LABEL  -> 20 distinct streams
20 log events with transaction_id in the JSON BODY -> 1 stream
```

**Root cause:** Loki creates one storage stream per unique label
combination. A label like `transaction_id` (or `customer_id`,
`trace_id`) has effectively unbounded cardinality — every request
creates a brand-new stream that Loki must index and store separately
forever. At real Flipkart traffic volumes (millions of orders/day),
this is exactly pain point #4: a Loki cluster that looked fine in
testing (low volume, low cardinality by accident) degrades sharply once
real per-request IDs start being used as labels. The fix, applied
throughout this lab's actual app code (`logging_lib.py`): labels stay
fixed to `job`, `service`, `level` — a small, closed set — and every
per-request ID (`trace_id`, `correlation_id`, `customer_id`) lives in
the JSON log body, queryable via LogQL's `| json` parser instead.

### 2. End-to-end correlation across logs, metrics, and traces → pain point #1

**Captured evidence** (one real order, placed live):
```
POST /api/orders -> order_id=1, correlation_id=100d2e29-..., trace_id=e195eaa3ca6220262bccb6f6dae15640

Loki query {job="flipkart"} returned 3 real log lines for this exact
trace_id/correlation_id pair: "request completed", "order placed",
and the order_details line -- across the SAME request.

Jaeger query for trace e195eaa3ca6220262bccb6f6dae15640 -> 2 real spans returned.
```

**Root cause of pain point #1 (when this is done WRONG):** if a
service logs freeform text ("processing order for customer") with no
structured, consistently-named ID field, support has no reliable way
to jump from "the customer's order number" to "the exact log lines for
that request." This lab's fix: every log line is JSON with a
`trace_id` and `correlation_id` field, generated once per request in
FastAPI middleware, propagated into Kafka message headers via
OpenTelemetry's `inject()`/`extract()`, and carried through to the
consumer's logs — so the *same two IDs* work as the search key in Loki
*and* as the lookup key in Jaeger, with no separate ID mapping needed.

### 3. Finding slow/failed events with LogQL → pain point #2

**Captured evidence** (20 real orders processed by the real Kafka
consumer, then queried back from Loki):
```
Consumer log output: 4 payments logged as "(slow, ...)" with durations 1848-2829ms
LogQL {job="flipkart", service="flipkart-payment-consumer"} |= "slow"
  -> 4 matching log lines (exact match to the consumer's own count)
```

**Root cause of pain point #2 (when this capability is missing):**
without structured fields and a reachable query language, "slow
payment" is invisible until a customer complains — there's no way to
proactively ask "how many payments took longer than 1 second in the
last hour" short of grepping raw text files across N consumer
instances. With structured JSON + LogQL, the exact query
`{job="flipkart", service="flipkart-payment-consumer"} | json |
duration_ms > 1000` finds every slow event across every consumer
instance in one query, and the "Slow payment events" panel in the
provisioned Grafana dashboard shows this live.

### 4. Sensitive-data masking — a real, planted gap → pain point #3

**Captured evidence** (real log line from a real order placement):
```json
{"event": "order_details", "order_id": 1,
 "customer_email_masked": "t***@example.com",
 "customer_phone_UNMASKED": "9876543210"}
```
```
LogQL {job="flipkart"} |= "customer_phone_UNMASKED" -> 1 matching log line found
```

**Root cause:** `main.py` masks `customer_email` via `mask_email()`
before logging, but never runs `customer_phone` through the equivalent
`mask_phone()` helper that already exists in `logging_lib.py` — a
realistic "we remembered the obvious field and missed the second one"
gap, not a contrived example. This is exactly pain point #3's
mechanism, and it's findable with a single LogQL substring search
against real, running infrastructure — which is the point of the
hands-on lab: sensitive-data leaks in logs are usually *findable*
before they're a real incident, if someone actually runs the query.

## How the hands-on lab reproduces this

1. `docker compose up -d` (Loki, Prometheus, Grafana — also needs the
   shared Jaeger and Kafka containers already running from earlier
   modules: `docker start irctc-jaeger kafka-kafka-1`).
2. Start `main.py` and `consumer.py`. Place an order via the UI; note
   the returned `correlation_id` and `trace_id`.
3. In Grafana Explore (Loki datasource), run
   `{job="flipkart"} | json | correlation_id="<paste the ID>"` — find
   every log line for that one request across both services.
4. Click the "TraceID" derived-field link on any log line with a
   `trace_id` field — Grafana opens the exact matching trace in Jaeger.
5. Run `{job="flipkart"} |= "customer_phone_UNMASKED"` — find the
   planted masking gap, then locate and fix the missing `mask_phone()`
   call in `main.py`, re-run, and confirm the query returns nothing new.
6. Run `{job="flipkart", service="flipkart-payment-consumer"} |= "slow"`
   to find slow payment events; discuss what threshold and alert you'd
   build from this LogQL pattern using Module 2's alerting skills.
7. Reproduce the cardinality comparison yourself (see
   `cardinality_demo.py`) and discuss why `customer_id`/`order_id`
   should never be Loki labels, even though they're perfectly
   reasonable Postgres or Prometheus label/index choices.

## Honesty log — what was actually verified vs. not

| Claim | Status |
|---|---|
| Loki cardinality demo (20 streams vs. 1) | **Real**, queried via Loki's own `/loki/api/v1/series` |
| Correlation across Loki + Jaeger for one live order | **Real** — same trace_id, 3 log lines + 2 spans, independently queried from both systems |
| Slow-payment LogQL query count (4) | **Real**, matched the consumer's own log output exactly |
| Unmasked-phone bug findable via LogQL | **Real**, 1 matching line, contents shown above |
| Grafana provisioning (3 datasources, 1 dashboard) | **Real**, verified via Grafana's own `/api/search` and `/api/datasources` |
| Loki -> Jaeger derived-field link config | **Real**, verified via Grafana's datasource API, not just "the YAML looks right" |
