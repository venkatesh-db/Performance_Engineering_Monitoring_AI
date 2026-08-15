# Paytm Use Case — ELK Stack & Cross-Signal Incident Analysis
### Day 3 / Module 4: ELK Stack and Cross-Signal Incident Analysis

## Scenario

Paytm's UPI payment flow spans an order/payment API and a Kafka-based
bank-settlement consumer. A performance tester ships the SAME
structured log event to both Loki and Elasticsearch simultaneously, so
they can genuinely compare the LogQL/Grafana investigation workflow
against the KQL/Kibana one on identical data — and, along the way,
personally hits and diagnoses a real cross-signal data-loss bug.

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "My payment failed and support couldn't tell me why" | No structured, filterable failure reason across services |
| 2 | "The team said 'no errors in the dashboard' but I definitely got charged" | One observability backend can silently lose data while another doesn't — and nobody would know without cross-checking |
| 3 | "We couldn't tell if it was insufficient balance, a bank timeout, or a wrong PIN" | Aggregating failures by cause requires the right tool for the job |

## Root-cause identification, mapped to Module 4 topics

### 1. A real, self-inflicted bug: wrong Loki port silently dropped every log for a while → pain point #2

**Captured evidence, in the order it was actually found:**
```
1st test payment: Elasticsearch found 2 docs, Jaeger found 3 spans, Loki found 0 lines
Direct Loki push test (exact app code path) via Python: worked fine (HTTP 204)
Root cause found: logging_lib.py's LOKI_PUSH_URL was copied from another
  lab project and hardcoded to port 3100 -- but THIS project's docker-compose
  maps Loki to host port 3101, so every push silently failed and was
  swallowed by the handler's `with suppress(Exception)` (logging must
  never crash the request path -- but that also means a misconfigured
  endpoint fails invisibly).
Fixed: LOKI_PUSH_URL -> 3101. Re-tested: Loki found the log immediately.
```

**A second bug found while fixing the first:** even after fixing the
port, one batch of 25 test payments produced `0 ERROR lines in Loki`
while Elasticsearch correctly showed `7 total ERROR docs`. Root cause:
an old consumer process (started *before* the port fix) was still
running in the same Kafka consumer group alongside the newly-started,
fixed one — some settlement failures landed on the stale process
(silently failing to reach Loki on the old port), while successes
happened to land on the fixed one. Killing the stale duplicate process
and re-testing cleanly gave consistent results: **2 ERROR lines in
Loki, matching the 2 real failures from that specific clean run.**

**Why this matters for pain point #2:** this is the exact real-world
failure mode the module warns about — one observability backend
(Elasticsearch, in this case) can keep working perfectly while another
(Loki) silently loses data, and nothing in either system's own UI would
tell you the other one is broken. The only way this was caught was by
cross-checking the SAME event across both systems and noticing the
counts didn't match — precisely the "cross-signal incident analysis"
skill this module teaches, applied here to catch a bug in the lab
setup itself, not just a simulated incident.

### 2. Elasticsearch aggregation vs. Loki full-text search — the real trade-off → pain point #3

**Captured evidence** (same 7 real settlement failures, queried both ways):

```
Elasticsearch (Kibana-style terms aggregation on message.keyword):
  "settlement FAILED: INSUFFICIENT_BALANCE" -> 3
  "settlement FAILED: BANK_TIMEOUT" -> 2
  "settlement FAILED: INVALID_UPI_PIN" -> 2
  (one query, one result set, broken down by reason)

Loki (LogQL {job="paytm", level="ERROR"}):
  returns matching LOG LINES you can read and grep further,
  but grouping/counting by an extracted field requires
  | json | line_format or | pattern gymnastics -- noticeably
  more friction for this specific kind of question.
```

**Root cause of the trade-off:** Loki only indexes labels (by design,
for cost/performance) and treats everything else as opaque text to be
grep'd at query time. Elasticsearch indexes every field of every
document, which is exactly why the terms aggregation above is a single
simple query — at the real cost of indexing overhead for every field,
every document, forever (until index lifecycle management rolls old
indices off). This is pain point #3's resolution: "which UPI failure
reasons are trending up this hour" is an Elasticsearch-shaped question;
"show me every log line for this one transaction" is equally fast in
either, but Loki does it lighter.

### 3. Correlation across three signals, verified independently → pain point #1

**Captured evidence** (one real payment, all three systems queried
independently, not assumed from each other):
```
POST /api/payments -> upi_ref=UPIF017A6538431, trace_id=982083e8de0584cd14692cea4fff7c30
Loki:          2 log lines found for {job="paytm"} in the 30s window right after
Elasticsearch: 2 documents found matching upi_ref
Jaeger:        3 spans found for the trace_id (after allowing ~5s for
               OpenTelemetry's BatchSpanProcessor export interval --
               querying immediately after the request returned 0 spans,
               a real, reproducible timing gap worth knowing about)
```

**Root cause of pain point #1 (when this capability is missing):** a
support engineer with only a UPI reference number needs ONE reliable
path to every log line and the full trace for that transaction. This
lab's design (`upi_ref` and `trace_id`/`correlation_id` present in
every structured log line, propagated through Kafka headers via
OpenTelemetry context injection) makes that path exist — verified here
independently in all three backends, not just asserted from the code.

## How the hands-on lab reproduces this

1. `docker compose up -d` (Elasticsearch, Kibana, Loki, Prometheus,
   Grafana — also needs shared Jaeger + Kafka:
   `docker start irctc-jaeger kafka-kafka-1`). Elasticsearch/Kibana take
   longest to become healthy — check `curl localhost:9200` and
   `curl localhost:5601/api/status` before starting the app.
2. In Kibana (`http://localhost:5601`), create a Data View for
   `paytm-logs-*` under Stack Management → Data Views — this is the
   guided "ingestion, parsing, indexing" step for the module.
3. Start `main.py` and `consumer.py`. Make several payments via the UI.
4. In Kibana Discover, search `upi_ref: "<paste a real ref>"` — find
   the transaction. In Grafana Explore (Loki), run
   `{job="paytm"} |= "<same ref>"` — find the same transaction the
   other way.
5. Build a Kibana visualization: terms aggregation on `message.keyword`
   filtered to `level: "ERROR"` — reproduce the 3-reasons breakdown
   above with your own data.
6. Try the equivalent breakdown in LogQL and note the friction
   difference for yourself — this is the "Loki versus ELK: appropriate
   use cases and trade-offs" topic, experienced directly rather than
   just read about.
7. Build a short incident timeline for one failed settlement: request
   timestamp (Prometheus latency panel) → log line (Loki or Kibana,
   your choice) → trace spans (Jaeger) → the specific failure reason —
   in that order, with real timestamps from each system.

## Honesty log — what was actually verified vs. not

| Claim | Status |
|---|---|
| Elasticsearch + Kibana genuinely healthy and reachable | **Verified** via `/` cluster info and `/api/status` |
| Same log event in both Loki and Elasticsearch | **Verified**, after finding and fixing two real bugs (wrong port, stale duplicate process) documented above in full, not glossed over |
| Elasticsearch terms aggregation on real failure data | **Verified**, real counts shown above |
| Cross-signal correlation (Loki + Elasticsearch + Jaeger, one transaction) | **Verified**, each system queried independently |
| Grafana provisioning (3 datasources + 1 dashboard) | **Verified** via Grafana's own `/api/search` and `/api/datasources` |
| Jaeger span export timing | **Verified** — first query immediately after a request found 0 spans (real, reproducible BatchSpanProcessor delay), a second query ~5s later found 3 |
