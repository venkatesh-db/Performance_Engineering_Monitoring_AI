# Performance Engineering Lab — Working Kit

A payment API with four deliberate defects, plus JMeter plans to find them.

```
lab/
├── app/main.py            FastAPI payment API (defects toggleable by env var)
├── app/Dockerfile
├── docker-compose.yml     Postgres + Redis + API
├── data/customers.csv     680 rows, weighted skew (20 heavy customers)
├── data/gen_data.py
├── plans/payment.jmx      Full journey: login → pay → status → history
├── plans/jdbc.jmx         Direct Postgres, for the isolation lab
├── run.sh                 One command for every test
└── watch.sh               Live dependency monitor
```

---

## The four planted defects

| # | Defect | Where | Toggle | Day |
|---|---|---|---|---|
| 1 | N+1 query | `GET /customers/{id}/transactions` | `FIX_NPLUS1` | D2 M3 |
| 2 | Blocking `time.sleep()` in async path | fraud check | `ASYNC_FRAUD` | D2 M2 |
| 3 | No cache on hot read | `GET /customers/{id}` | `USE_CACHE` | D1 M4 |
| 4 | Pool of 5 | asyncpg pool | `POOL_MAX` | D1 M4 |

Every fix is an env var, so before/after runs use an **identical workload**. That is the whole point.

---

## Setup

```bash
export LAB=~/perf-lab          # wherever you unpack this
export JM=/path/to/apache-jmeter/bin/jmeter

cd $LAB
docker compose up -d --build
curl localhost:8000/health     # {"status":"ok",...}
curl localhost:8000/config     # shows which defects are active
```

**Postgres driver** — required for `jdbc.jmx`:
```bash
cp ~/Downloads/postgresql-42.7.*.jar /path/to/apache-jmeter/lib/
```

**Heap** — edit `apache-jmeter/bin/jmeter`:
```
HEAP="-Xms1g -Xmx4g"
```

---

# DAY 1

## M1 — Baseline

```bash
# the floor: one user, no contention
./run.sh payment floor -Jthreads=1 -Jduration=60 -Jthinktime=0

# ten users
./run.sh payment base10 -Jthreads=10 -Jramp=10 -Jduration=180
```

While it runs, second terminal:
```bash
top -o cpu     # JMeter must stay under 70% or the result is about your laptop
```

**Record:** floor p99, 10-user p99, injector CPU.

**Teaching point:** open the Statistics table and put Average next to 99th percentile. The gap is the lesson.

---

## M2 — Workload modelling

```bash
./run.sh payment s1-baseline -Jthreads=1  -Jduration=60
./run.sh payment s2-load     -Jthreads=50 -Jramp=120 -Jduration=600
./run.sh payment s3-stress   -Jthreads=150 -Jramp=60 -Jduration=300
./run.sh payment s4-spike    -Jthreads=200 -Jramp=5  -Jduration=180
```

**Cohort deliverable:** at what thread count does p99 first breach 1000ms?

Note `s4-spike` ramps 200 users in 5 seconds — cold pool, cold cache, worst case. That is the festival-open / market-open shape.

---

## M3 — REST API testing (the 3-hour core)

**Step 1 — validate with one thread, in the GUI:**

Open `plans/payment.jmx`, add a **View Results Tree**, set threads=1, run.

Check the **Request** tab on each sampler:

| Sampler | Must show |
|---|---|
| 1 Login | returns `access_token` |
| 2 Create Payment | header `Bearer tok_abc...`, not `${authToken}` |
| 3 Check Status | `/payments/TXN4F2A...`, a real ID |
| 4 Transaction History | `/customers/CUST0007/transactions` |

**Step 2 — prove an assertion can fail.**

Change the "Assert Completed" pattern to `COMPLETEDXX`, run, confirm red, change back.
An assertion you have never seen fail is one you cannot trust.

**Step 3 — delete listeners, run headless:**
```bash
./run.sh payment m3-load -Jthreads=50 -Jramp=60 -Jduration=600
```

**Step 4 — the error-validation lesson.**

Look at the response bodies. Three distinct outcomes, all HTTP 200:

```json
{"status":"SUCCESS","transactionId":"TXN..."}       ← pass
{"status":"DECLINED","code":"INSUFFICIENT_FUNDS"}   ← business, NOT an error
{"status":"DECLINED","code":"DOWNSTREAM_TIMEOUT"}   ← your defect
```

The plan asserts on `"status"` — deliberately permissive, so declines don't inflate the error rate. Ask the cohort to add a second assertion that catches only the third case.

---

## M4 — Database, Kafka, Redis

### Part A — app load, watch dependencies

Terminal 1:
```bash
./run.sh payment m4-app -Jthreads=50 -Jduration=300
```

Terminal 2:
```bash
./watch.sh
```

Watch pool waiters climb. `POOL_MAX=5` against 50 threads is the whole demonstration.

### Part B — the cache experiment

```bash
./run.sh payment cache-off -Jthreads=30 -Jduration=180

USE_CACHE=true docker compose up -d api
sleep 5 && curl localhost:8000/config     # confirm the toggle took

./run.sh payment cache-on  -Jthreads=30 -Jduration=180
```

Compare the two reports side by side. Identical load, one variable changed.

### Part C — the pool experiment

```bash
POOL_MAX=5  docker compose up -d api && sleep 5
./run.sh payment pool-05 -Jthreads=50 -Jduration=180

POOL_MAX=30 docker compose up -d api && sleep 5
./run.sh payment pool-30 -Jthreads=50 -Jduration=180
```

**The signature to teach:** with a starved pool, queries stay fast but the API is slow. Requests spend their time *waiting for a connection*, not executing SQL. The slow-query log is clean while users time out.

### Part D — JDBC isolation

```bash
./run.sh jdbc db-05 -Jthreads=5  -Jduration=120
./run.sh jdbc db-20 -Jthreads=20 -Jduration=120
./run.sh jdbc db-50 -Jthreads=50 -Jduration=120

# starve the JMeter-side pool and watch the same signature appear
./run.sh jdbc db-starve -Jthreads=50 -Jpoolsize=5 -Jduration=120
```

**Index impact:**
```bash
psql -h localhost -U postgres -d payments -c \
  "CREATE INDEX idx_txn_cust ON transactions(customer_id);"
./run.sh jdbc db-indexed -Jthreads=20 -Jduration=120

psql -h localhost -U postgres -d payments -c "DROP INDEX idx_txn_cust;"
./run.sh jdbc db-noindex -Jthreads=20 -Jduration=120
```

### Part E — the comparison that makes the module

```bash
pgbench -h localhost -U postgres -c 20 -j 4 -T 60 payments
redis-benchmark -n 200000 -c 50 -t get,set -d 512
```

Put four numbers side by side:

| Source | Result |
|---|---|
| pgbench | ~8,000 TPS |
| redis-benchmark | ~120,000 ops/s |
| JMeter JDBC (query alone) | ~8 ms |
| **JMeter full API** | **~400 TPS** |

Every component passed. The system delivered 400.
**The ceiling lives in the gaps between components** — pool limits, lock hold time, network hops — and no component benchmark can find it.

---

# DAY 2 — load runs, isn't studied

**One rule: identical load across every run**, or before/after proves nothing.

```bash
D2="-Jthreads=30 -Jramp=30 -Jduration=300"
```

## M1 — Linux analysis

```bash
./run.sh payment d2-linux $D2 &

vmstat 1 320  > results/vmstat.log  &
iostat -x 1 320 > results/iostat.log &
```

Diagnose from files afterwards. You cannot investigate a spike interactively.

**In `vmstat`:** `r` above core count = CPU-bound. `si`/`so` above zero = swapping, run invalid.

## M2 — Python profiling (defect 2)

```bash
./run.sh payment d2-before $D2 &
sleep 30
docker compose exec api py-spy record --pid 1 --duration 60 --output /tmp/before.svg
docker compose cp api:/tmp/before.svg ./results/
```

You will see `time.sleep` dominating. Then:

```bash
ASYNC_FRAUD=true docker compose up -d api && sleep 5
./run.sh payment d2-after $D2
```

**Compare the two JMeter reports, not the flame graphs.** The flame graph says *where*; the report says *how much*.

**The signature of defect 2:** low CPU, poor throughput, and it gets *worse* with more concurrency. That combination means the event loop is blocked.

## M3 — Dependency analysis (defect 1)

```bash
psql -h localhost -U postgres -d payments -c \
  "SELECT pg_stat_statements_reset();"

./run.sh payment d2-nplus1 $D2

psql -h localhost -U postgres -d payments -c \
  "SELECT calls, round(mean_exec_time::numeric,2) AS ms, left(query,60)
   FROM pg_stat_statements ORDER BY calls DESC LIMIT 5;"
```

The customer-name lookup will show a **huge call count** with a tiny mean time. That is the N+1 signature — every query is fast, there are just hundreds of them.

```bash
FIX_NPLUS1=true docker compose up -d api && sleep 5
./run.sh payment d2-nplus1-fixed $D2
```

**Teaching point:** count queries per request, don't just time them. If the count scales with result size, it's an N+1.

## M4 — OpenTelemetry

```bash
./run.sh payment d2-trace -Jthreads=20 -Jduration=180
```

The plan already sends `X-Test-Run: d2-trace` on every request. Filter Jaeger by that attribute to find your own traffic.

---

# DAY 3 — load creates what the dashboards show

## M1 — Prometheus
```bash
./run.sh payment d3-metrics -Jthreads=40 -Jduration=600
```
Under 2 minutes gives sparse data and `rate()` returns nothing useful.

## M2 — Grafana: the best exercise of the day

**Deliberately breach the SLO and confirm the alert fires.**

```bash
./run.sh payment d3-breach -Jthreads=200 -Jramp=10 -Jduration=300
```

Then ask four questions:
- Did the alert fire?
- How long did it take?
- Did it route to the right place?
- Did the message say anything actionable?

Most teams discover their alerting is broken during a real incident. This is how you find out first.

## M3 / M4 — Loki and ELK

```bash
./run.sh payment d3-logs -Jthreads=30 -Jduration=300
cat results/timeline.txt      # exact UTC start/end, written by run.sh
```

Correlation depends on that timestamp. `run.sh` records it automatically.

---

# DAY 4

## M1 — Tuning: fix all four, one at a time

```bash
D4="-Jthreads=50 -Jramp=60 -Jduration=600"

./run.sh payment d4-00-baseline $D4

USE_CACHE=true docker compose up -d api && sleep 5
./run.sh payment d4-01-cache $D4

FIX_NPLUS1=true docker compose up -d api && sleep 5
./run.sh payment d4-02-nplus1 $D4

ASYNC_FRAUD=true docker compose up -d api && sleep 5
./run.sh payment d4-03-async $D4

POOL_MAX=30 docker compose up -d api && sleep 5
./run.sh payment d4-04-pool $D4
```

**Fill this in from the five reports:**

| Run | Throughput | p99 | Error % | Delta |
|---|---|---|---|---|
| baseline | | | | — |
| + cache | | | | |
| + N+1 fix | | | | |
| + async | | | | |
| + pool | | | | |

**One variable per run.** Change three and you have learned nothing about causality.

## M2 — Incident analysis

```bash
./run.sh payment d4-incident -Jthreads=60 -Jduration=900 &

sleep 240
# trainer injects mid-run:
POOL_MAX=2 docker compose up -d api
```

Participants see the symptom appear live, then build the timeline.

## M4 — Capstone + capacity

```bash
./run.sh payment capstone-final -Jthreads=50 -Jduration=900
```

**The arithmetic:**
```
Measured steady-state throughput   = ____ TPS
Peak observed demand               = ____ TPS
With 30% safety margin  demand/0.7 = ____
With 25% growth         × 1.25     = ____
Required ceiling                   = ____
Verdict: current ceiling ___ required
```

**CI gate:**
```bash
$JM -n -t plans/payment.jmx -l ci.jtl \
    -Jthreads=20 -Jduration=120 -Jdatadir=$PWD/data

ERRORS=$(awk -F, 'NR>1 && $8=="false"' ci.jtl | wc -l)
TOTAL=$(awk 'NR>1' ci.jtl | wc -l)
RATE=$(echo "scale=4; $ERRORS/$TOTAL" | bc)
echo "error rate: $RATE"
(( $(echo "$RATE > 0.01" | bc) )) && { echo "GATE FAILED"; exit 1; }
```

For real CI gating k6 is cleaner — thresholds exit 99 on breach with no post-processing. Worth showing both.

---

# The five rules to repeat all week

1. **Delete listeners before any real run.** They make the generator the bottleneck.
2. **Validate with 1 thread first.** Then scale.
3. **Prove every assertion can fail** before trusting it.
4. **One variable per run.** Otherwise causality is unknowable.
5. **Check injector health first.** Above 70% CPU and the result is about your laptop.

---

# Reference

```bash
./run.sh payment <label> -Jthreads=N -Jramp=N -Jduration=N -Jthinktime=N
./run.sh jdbc    <label> -Jthreads=N -Jpoolsize=N

open reports/<label>/index.html
open reports/d4-00-baseline/index.html reports/d4-04-pool/index.html   # side by side
```

**Report columns that matter:** 99th pct, Error %, Throughput.
Ignore Average — it hides the tail, which is where your users live.
