# Hands-on lab prompt for Claude — Day 2 / Module 3

```
I ran a deep dependency-analysis lab across PostgreSQL, Kafka, and Redis
for an IRCTC-style availability service. Here's what I captured:

POSTGRES (real EXPLAIN + pg_stat_user_indexes):
- idx_availability_train_date: idx_scan=2, size=352KB (the composite index actually used)
- idx_availability_is_active: idx_scan=0, size=64KB (a low-selectivity index, ~95% of rows are true)

KAFKA (same 150-msg burst, same 20ms/msg processing delay, only partition/consumer count changed):
- 1 partition, 1 consumer: max lag = 471ms
- 3 partitions, 3 consumers: max lag = 26ms

REDIS (isolated instance, maxmemory=3MB, allkeys-lru):
- After ~15,000 distinct-key lookups: used_memory hit the 3MB cap, evicted_keys=1003
- The "hot" key (80% of normal traffic) was evicted too, despite being logically hot

POSTGRES BACKPRESSURE (pool capped at 3 connections, 30 concurrent callers needing a 300ms query each):
- Naive immediate retry, no cap: 165 total attempts, 30/30 succeeded, 5.5x amplification
- Backoff, capped at 3 tries: 81 total attempts, 9/30 succeeded, 2.7x amplification

Acting as a performance tester writing the Day 2 dependency-analysis
report, do the following:
1. For the Postgres finding: write the exact one-sentence justification
   you'd give a DBA who says "but the index might help someday" for why
   idx_scan=0 is sufficient evidence to drop it now, not "someday."
2. For the Kafka finding: explain why adding MORE consumer instances to
   the 1-partition topic would NOT have fixed the lag, and what the
   actual constraint is.
3. For the Redis finding: propose a concrete fix for pain point #3
   (hot key evicted anyway) and explain why simply increasing maxmemory
   is not a real fix, only a delay of the same failure mode.
4. For the backpressure finding: this is a genuine tradeoff, not a clean
   win for either strategy -- recommend a THIRD approach for IRCTC's
   real Tatkal traffic that improves on both the 100%-success/5.5x-load
   naive approach and the 2.7x-load/30%-success backoff approach.
5. Rank all four findings by which one you'd fix first given limited
   engineering time before the next Tatkal window, and justify with the
   specific numbers above, not general priority-setting advice.

Cite my actual captured numbers throughout.
```

## Notes
- Attach `main.py` and `CASE_STUDY.md` so Claude reasons from the real
  queries, real topic configs, and real retry logic in this repo.
- `CASE_STUDY.md` is explicit about two real dead-ends hit while
  capturing the Kafka numbers (a stale-message timing artifact, and a
  `client_id` collision bug that silently broke the 3-consumer group) —
  worth reading if you want the full debugging story, not just the
  final clean numbers.
