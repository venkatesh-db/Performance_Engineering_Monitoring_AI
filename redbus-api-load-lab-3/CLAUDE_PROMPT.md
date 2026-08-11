# Prompt to give Claude for this lab

```
I load-tested a redBus-style payment API with k6, using two executors
against the identical backend (gateway concurrency capacity = 8,
~0.15s per transaction, so a real throughput ceiling of roughly 53 req/s):

VU-based (closed model, constant-vus, 20 VUs, 20s):
- checks: 100% pass, infra_error=0
- payment_latency_ms: P95=462ms (threshold: <2000ms, PASSED)
- biz_success=60, biz_declined=992

Arrival-rate (open model, constant-arrival-rate, 70 req/s, 20s):
- checks: 100% pass, infra_error=0
- payment_latency_ms: P95=2,691ms (threshold: <2000ms, FAILED)
- dropped_iterations=170 (k6 could not schedule these at the requested rate)

I also have a JMeter .jmx implementing the same journey (login -> extract
token -> pay, with CSV-parameterized customers and unique transaction_ids)
but have not run it yet.

Acting as a performance tester, do the following:
1. Explain why infra_error stayed at 0 in both runs even though the
   arrival-rate run clearly failed its SLA — what do dropped_iterations
   and P95 latency tell you that the error-rate metric alone doesn't?
2. Given the implied throughput ceiling (~53 req/s), recommend a safe
   target arrival rate for redBus's real traffic, and a stress-test plan
   to find the actual breaking point beyond it.
3. Tell me what result I should expect from running the equivalent JMeter
   test at the same load levels, and what would indicate the two tools'
   scripts are NOT actually equivalent if the numbers diverge significantly.
4. Point out the test-data risk visible in the VU-based run's decline rate
   (992 declined vs 60 success) and what should change before trusting
   that number as a "business decline rate" metric.

Cite the numbers I gave you specifically.
```

## Notes
- Attach `main.py`, `k6/payment_test.js`, `jmeter/payment_test.jmx`, and
  `CASE_STUDY.md` so Claude reasons from the actual test design, not a
  generic description of JMeter/k6.
- If you get real JMeter numbers, paste them and ask Claude to complete
  the cross-tool comparison this case study explicitly left open.
