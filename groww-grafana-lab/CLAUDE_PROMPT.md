# Hands-on lab prompt for Claude — Day 3 / Module 2

```
I provisioned a real Grafana + Prometheus stack for a Groww-style
trading API and ran real load against it. Here's what I captured:

BUG #1 (found and fixed): /metrics was a sync `def` FastAPI endpoint,
which runs in a threadpool thread separate from the event loop where
asyncpg.Pool actually lives. It silently always reported db_pool_in_use=0
even during real saturation (proven: /api/summary, an async def reading
the same pool, correctly showed 4/4 during the same burst). Fixed by
making /metrics `async def`.

BUG #2 (found and fixed): the alert group's eval interval (30s) was
coarser than the DB-pool alert's `for: 15s` duration, making it
effectively require ~30-60s of sustained truth to ever fire, not the
nominal 15s. Fixed by reducing the group interval to 10s.

RESULTS AFTER BOTH FIXES:
- SYMPTOM alert ("P95 latency above 800ms"): confirmed transitioning to
  real "Alerting" state via Grafana's API during a sustained 40-thread rush.
- CAUSE alert ("DB pool above 90%"): the underlying metric was
  independently proven to cross 90% repeatedly (8/10 polls at exactly
  4/4 during a 25s rush), but the alert itself was not observed in a
  Firing state in this session -- it stayed Pending/Normal at check time.

Acting as a performance tester writing the Module 2 lab deliverable
(dashboard + alert review), do the following:
1. Explain bug #1 in terms a reviewer unfamiliar with asyncio would
   understand: why does "the code looks like it computes the right
   thing" not guarantee a dashboard panel is trustworthy here?
2. For bug #2: propose the general rule of thumb for how an alert
   group's evaluation interval should relate to its rules' `for`
   durations, and what symptom you'd look for in a real production
   alert config to catch this class of misconfiguration before an
   incident, not during one.
3. Given that the SYMPTOM alert reliably fires but the CAUSE alert is
   harder to catch on short scripted bursts, propose a concrete change
   to the CAUSE alert (different query, different for/interval, or a
   different metric entirely) that would make it at least as reliable
   as the SYMPTOM alert for the SAME underlying incident.
4. Write the "alert review practice" checklist this lab's two bugs
   suggest should be standard before shipping any new Grafana alert
   rule -- concrete, not generic ("test your alerts" is too vague).

Cite my actual findings, not generic Grafana/Prometheus advice.
```

## Notes
- Attach `main.py`, `grafana/provisioning/alerting/rules.yml`, and
  `CASE_STUDY.md` so Claude reasons from the actual bug and actual rule
  YAML, not assumptions about typical FastAPI/Grafana setups.
- The case study is explicit that the CAUSE alert's firing was NOT
  captured live in this session, only its underlying metric condition —
  if you get it to fire on a re-run, paste the real state transition and
  ask Claude to update the analysis with it.
