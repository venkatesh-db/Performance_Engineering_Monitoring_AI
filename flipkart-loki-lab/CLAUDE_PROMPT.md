# Hands-on lab prompt for Claude — Day 3 / Module 3

```
I ran a real Loki + Prometheus + Grafana + Jaeger stack against a
Flipkart-style order/payment API and captured this evidence:

CARDINALITY DEMO (real Loki /loki/api/v1/series query):
transaction_id as a Loki LABEL: 20 events -> 20 distinct streams
transaction_id in the JSON BODY: 20 events -> 1 stream

END-TO-END CORRELATION (one real order):
trace_id=e195eaa3ca6220262bccb6f6dae15640
Loki {job="flipkart"} query for this trace_id: 3 log lines found
  (across the order API's request-completed, order-placed, and
  order-details log statements)
Jaeger query for the same trace_id: 2 real spans found

SLOW-PAYMENT DETECTION:
20 orders processed by the real Kafka consumer -> 4 logged as "(slow, ...)"
  with durations 1848-2829ms
LogQL {job="flipkart", service="flipkart-payment-consumer"} |= "slow"
  -> 4 matching lines (exact match)

MASKING GAP FOUND VIA LOGQL:
Real log line: {"customer_email_masked": "t***@example.com",
  "customer_phone_UNMASKED": "9876543210"}
LogQL {job="flipkart"} |= "customer_phone_UNMASKED" -> 1 matching line

Acting as a performance tester writing the Module 3 lab deliverable, do
the following:
1. Extrapolate the cardinality finding to real Flipkart scale: if this
   service handles 5 million orders/day and someone put order_id as a
   Loki label instead of trace_id, roughly how many streams would that
   create per day, and why does that number alone justify the label
   design rule regardless of any specific storage cost figure?
2. For the end-to-end correlation: write the exact LogQL query you'd
   hand to a support engineer who only has a customer's order number
   (not the trace_id or correlation_id) -- what has to be true about
   the log body for that query to work, and what would you fix if it
   didn't?
3. For the masking gap: propose a way to catch "we masked one sensitive
   field and missed another" systematically, BEFORE it reaches
   production logs, rather than relying on someone running the right
   LogQL search after the fact.
4. Write the "log/dashboard review checklist" this lab's four findings
   suggest should be standard practice before shipping any new
   structured-logging change -- concrete items tied to these findings,
   not generic "review your logs" advice.

Cite my actual numbers throughout.
```

## Notes
- Attach `main.py`, `logging_lib.py`, `consumer.py`, and `CASE_STUDY.md`
  so Claude reasons from the real label design and the real planted bug,
  not assumptions about typical logging setups.
- Run `cardinality_demo.py` yourself against the live Loki instance to
  reproduce the 20-streams-vs-1 finding before asking Claude to
  extrapolate from it.
