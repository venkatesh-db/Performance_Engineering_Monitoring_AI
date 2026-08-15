# Hands-on lab prompt for Claude — Day 3 / Module 4

```
I ran a real Elasticsearch + Kibana + Loki + Grafana + Jaeger stack
against a Paytm-style UPI payment API, shipping the SAME structured
logs to both Loki and Elasticsearch simultaneously. Here's what happened:

BUG FOUND #1: logging_lib.py's Loki push URL was hardcoded to the wrong
port (copied from another project), so every Loki push silently failed
while Elasticsearch kept receiving data correctly -- first test showed
Elasticsearch: 2 docs, Jaeger: 3 spans, Loki: 0 lines for the identical
transaction. Fixed the port; Loki started receiving data.

BUG FOUND #2: even after the fix, one batch of 25 payments showed
Elasticsearch with 7 ERROR docs but Loki with 0 ERROR lines. Root cause:
a stale duplicate consumer process (started before the port fix) was
still in the same Kafka consumer group, silently eating some of the
failure events on the broken old config. Killed the stale process,
re-ran cleanly: Loki then showed 2 ERROR lines matching the 2 real
failures from that specific clean run.

ELASTICSEARCH VS LOKI TRADE-OFF (same 7 real failures):
Elasticsearch terms aggregation: INSUFFICIENT_BALANCE=3, BANK_TIMEOUT=2,
  INVALID_UPI_PIN=2 -- one query.
Loki: found the same lines via full-text search, but grouping/counting
  by extracted reason needed more LogQL gymnastics.

CROSS-SIGNAL CORRELATION (one real payment):
upi_ref=UPIF017A6538431, trace_id=982083e8de0584cd14692cea4fff7c30
Loki: 2 lines. Elasticsearch: 2 docs. Jaeger: 0 spans immediately after
  the request, 3 spans ~5s later (BatchSpanProcessor export delay).

Acting as a performance tester writing the Module 4 lab deliverable
(the "cross-signal incident timeline"), do the following:
1. Write the incident timeline for BUG #1 as if it were a real
   production incident report: what was observed, what was the
   blast radius (which signal was affected, which wasn't), what was
   the root cause, what was the fix, what would you add to prevent
   recurrence (hint: the exception was silently suppressed).
2. For BUG #2: propose a concrete operational safeguard (not "be more
   careful") that would have caught the stale duplicate consumer
   process before it silently dropped data.
3. Given the real aggregation-friction difference between Loki and
   Elasticsearch, write a one-paragraph decision rule for a new
   service at Paytm: when should its logs go to Loki, when to
   Elasticsearch, and when (if ever) to both, citing my actual evidence.
4. Explain the Jaeger 0-then-3-spans timing gap precisely: what is
   BatchSpanProcessor actually doing between "the HTTP response
   returned" and "the trace became queryable," and what would you tell
   a teammate who says "the trace is missing" 1 second after a request
   completes?

Cite my actual findings throughout, not generic ELK/Loki advice.
```

## Notes
- Attach `main.py`, `logging_lib.py`, `consumer.py`, and `CASE_STUDY.md`
  so Claude reasons from the real bugs and real fixes, not a
  hypothetical clean setup.
- If you reproduce the Kibana terms-aggregation visualization yourself,
  paste a screenshot description or the raw JSON and ask Claude to
  compare it against the numbers above.
