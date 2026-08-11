# Prompt to give Claude for this lab

Use this prompt (in Claude Code, or claude.ai with the project files attached)
to have Claude analyze results you capture during the lab and produce the
pain-point / root-cause writeup a performance tester would deliver.

```
I ran a load test against a redBus-style bus-search and seat-selection API
(FastAPI + SQLite demo in this project). The lab has three deliberately
injected performance defects: a serialized single DB connection, a missing
index on the search query, and an N+1 query pattern in seat-map loading.

Here is what I captured from Locust (paste your actual numbers):
- Baseline (10 users): RPS=__, P50=__ms, P95=__ms, P99=__ms, errors=__%
- Peak (200 users): RPS=__, P50=__ms, P95=__ms, P99=__ms, errors=__%
- /api/seats/[id] response time for a 20-seat bus vs a 60-seat bus: __ms vs __ms

Acting as a performance tester, do the following:
1. Diagnose whether the system is CPU-bound, saturated on a shared resource,
   or query-cost-bound, using the RPS/latency relationship (apply Little's
   Law: L = λW) and the P50 vs P95/P99 divergence.
2. Identify which of the three known root causes (DB serialization, missing
   index, N+1 query) each symptom maps to, and explain the causal chain from
   root cause to the customer-visible pain point.
3. Rank the fixes by expected impact vs. effort, and state what metric you'd
   expect to move (and by roughly how much) after each fix.
4. Write a 5-bullet summary suitable for a stakeholder who doesn't read
   latency graphs, connecting technical root cause to customer complaint.

Be specific and reference the actual numbers I gave you — don't give generic
performance-testing advice.
```

## Notes for using this with Claude
- Attach or paste the contents of `main.py`, `CASE_STUDY.md`, and your Locust
  CSV/HTML report so Claude reasons from real evidence, not assumptions.
- If you fix one of the three bugs and re-run, re-paste the new numbers and
  ask Claude to confirm which root cause was resolved and which remain.
