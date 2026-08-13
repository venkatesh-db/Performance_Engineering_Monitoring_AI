# Hands-on lab prompt for Claude — Day 2 / Module 4

```
I instrumented an IRCTC-style payment API with real OpenTelemetry,
exported to a real Jaeger instance. Here's what I captured:

CONTEXT PROPAGATION (two separate processes, same trace_id):
[irctc-payment-api]      POST /api/payment          58,320us
  cache-check-idempotency  1,409us
  fraud-check              51,074us
  db-charge                 2,402us
  kafka-publish             2,603us
[irctc-payment-consumer]   notify-customer         151,253us  <- different process

CRITICAL PATH (slow_db=true):
POST /api/payment total: 684,577us
  db-charge: 617,719us  (90% of total)

SAMPLING BUG (found and fixed):
- Custom Sampler, NOT wrapped in ParentBased, set to 20%: 38/40 requests
  still produced traces (should be ~8) -- because the SDK calls
  should_sample() for every span, not just the root, so ~8 independent
  spans per trace each rolling 20% gives P(at least one hits) = 1-0.8^8 = 83%.
- After wrapping in ParentBased(sampler): 5/40 traces landed at rate=0.2
  (expected ~8, within normal binomial variance). Rate=1.0 still gives 40/40.

ERROR SPAN (fraud-check failure):
otel.status_code=ERROR, otel.status_description="HTTPException: 402: ...",
payment.amount=999

Acting as a performance tester writing the Day 2 / Module 4 lab report,
do the following:
1. Explain the context-propagation mechanism precisely: what specific
   piece of data moved from the producer process to the consumer
   process, and through which transport, to make notify-customer show
   up under the same trace_id as the original HTTP request.
2. For the critical-path finding: if db-charge is 90% of total request
   time, what should a performance tester's next diagnostic step be
   (which module's tools would you reach for), and why is knowing this
   from a trace faster than starting from Postgres-side metrics alone?
3. For the sampling bug: write the one-paragraph explanation you'd give
   a teammate who wants to write their own custom sampler, of exactly
   when ParentBased is required and what silently breaks without it.
4. For the error span: propose one additional attribute or event you'd
   add to fraud-check to make a future on-call engineer's triage even
   faster than what's captured here.
5. Referencing the Day 1 module 3 lab's load-test numbers (P95 latency
   from JMeter/k6), explain how you'd use ONE representative trace like
   this to explain WHY the P95 number was what it was, rather than just
   reporting the number itself.

Cite my actual numbers and the real bug I found -- not generic OTel advice.
```

## Notes
- Attach `main.py`, `consumer.py`, and `CASE_STUDY.md`.
- The `ParentBased` bug is real and specific to this session's build —
  if you're teaching this module again, it's worth deliberately
  reproducing (skip the `ParentBased` wrapper first) so participants
  see the wrong sample count before the fix, exactly as this lab did.
