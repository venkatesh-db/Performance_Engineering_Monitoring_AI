# redBus Use Case — REST API Load Testing Pain Points & Root-Cause Analysis
### Module 3: REST API Load Testing with JMeter and k6

## Scenario

redBus's payment API ([main.py](main.py)) needs load-test coverage before
launch. A performance tester builds equivalent test plans in **k6**
(executed here) and **JMeter** (test plan provided; JMeter isn't
installed in this environment, so its results below are *expected*
behavior from the equivalent script, not a captured run — flagged
explicitly so this doesn't get presented as real data it isn't).

## Customer-facing pain points

| # | Complaint | Where it shows up |
|---|---|---|
| 1 | "My payment was declined but I definitely had a balance" | Confusing DECLINED vs. real error in test/monitoring reports |
| 2 | "The load test passed, but during the actual sale, response times crawled past 2 seconds" | k6/JMeter VU-based run reports clean; real (open) traffic doesn't |
| 3 | "Same test, two different tools, two different verdicts" | JMeter and k6 configured inconsistently give non-reproducible results |

## Root-cause identification, mapped to Module 3 topics

### 1. Error validation: business decline vs. real infra failure → pain point #1

**Captured evidence** (k6, VU-based, 20 VUs, 20s, `k6/payment_test.js`):

```
checks_succeeded: 100.00% (2104/2104)
biz_success: 60      biz_declined: 992      infra_error: 0
```

**Root cause:** `/api/payments` deliberately returns HTTP 200 with
`status: "DECLINED"` for insufficient balance (a valid business outcome)
and only uses HTTP 503 for a real gateway-capacity failure. The test
script's `check()` explicitly asserts `status` is one of
`["SUCCESS","DECLINED"]` and tracks them with **separate custom metrics**
(`biz_success`, `biz_declined`, `infra_error`) rather than treating any
non-SUCCESS response as a failure. Without this separation, a report
would either (a) show a scary 94% "failure" rate that's actually normal
business behavior, or (b) hide a real 503 inside a bucket of "expected"
non-200s. This is Module 3's "error validation" topic in one number.

**Secondary finding (test-data lesson, carried over from Module 2):**
the high decline rate itself is partly a test artifact — the CSV pool
has only 10 customers, and the in-memory balance never resets between
runs, so by iteration ~60 most customers are already drained. A
production-representative test needs either a larger customer pool or
a balance-reset step between runs — the same "test-data reuse and
cleanup" discipline from Module 2 applies directly to Module 3 scripts.

### 2. Arrival-rate vs. VU-based execution → pain point #2

**Captured evidence**, same backend, same journey, only the executor changed:

| Executor | Load | P95 latency | P99 latency | Threshold | Dropped iterations |
|---|---|---|---|---|---|
| `constant-vus` (closed, 20 VUs) | self-throttling | 461ms | 464ms | ✓ PASS (<2000ms) | 0 |
| `constant-arrival-rate` (open, 70 req/s) | fixed, independent of response time | **2,969ms** | **3,459ms** | **✗ FAIL** | **166** |

(Captured live via the lab console's "k6 run history" table — click the
buttons yourself to reproduce.)

**Root cause:** the payment gateway simulator has a fixed concurrency
capacity of 8 (`GATEWAY_CAPACITY = 8` in `main.py`), giving a real
throughput ceiling of roughly 8 ÷ 0.15s ≈ 53 req/s. The VU-based
executor never generates more concurrent demand than 20 VUs looping, so
it never gets near that ceiling and reports a clean pass. The
arrival-rate executor fires 70 journeys/sec **regardless of how long
each one takes** — once offered load exceeds the ~53 req/s ceiling,
requests queue, k6 itself starts dropping iterations it can't schedule
(170 of them), and the ones that do run see P95 balloon past the 2s
threshold. This is the direct, tool-level version of the closed-vs-open
workload-model lesson from Module 2 — same finding, now demonstrated as
a **built-in k6 executor choice** rather than a custom script.

### 3. Correlation & parameterization → pain point #3 (cross-tool reproducibility)

Both `k6/payment_test.js` and `jmeter/payment_test.jmx` implement the
*same* correlation pattern: read `customer_id`/`amount` from
`test_data/customers.csv`, POST to `/api/customers/login`, extract the
token from the JSON response (`$.token` in JMeter's JSONPostProcessor;
`loginRes.json('token')` in k6), and thread it into the
`Authorization: Bearer` header on `/api/payments`. Both also generate a
unique `transaction_id` per iteration (JMeter: `${__threadNum}` +
loop index + `${__Random(...)}`; k6: `${__VU}-${__ITER}-random`) to
avoid the 409-duplicate test-data bug from Module 2.

**Why this matters for reproducibility:** if one tool's script skips
correlation (reuses one token) or reuses one `transaction_id`, its
results will disagree with the other tool's — not because the API
behaves differently, but because the *test* is different. Before
comparing JMeter vs. k6 numbers, verify both scripts implement identical
correlation, parameterization, and think-time/pacing — otherwise you're
comparing two different experiments, not two tools.

## How the hands-on lab reproduces this

The UI at `http://127.0.0.1:8002` is a working lab console, not just a
demo screen — it triggers real k6 subprocesses and shows real thresholds:

1. Section 1: confirm the manual flow (login → pay → see SUCCESS/DECLINED),
   try `CUST010` with amount 500 for DECLINED, repeat a `transaction_id`
   for the 409 duplicate rejection.
2. Section 2: click **"Run 20 VUs / 20s"** (closed model) — watch it run
   for real (~20s) and populate the history table with a PASS.
3. Restart `main.py` to reset in-memory balances, then click
   **"Run at rate / 20s"** with rate=70 (open model, exceeds the
   gateway's ~53 req/s ceiling) — watch the same table row turn **FAIL**
   with P95/P99 and dropped-iteration counts, on the identical journey.
4. Section 4: run `jmeter -n -t jmeter/payment_test.jmx -l results.jtl -e
   -o report/` yourself (JMeter isn't installed in this dev environment,
   so this step must be run separately), read the Summary Report's P95 /
   error rate / throughput, and type them into the form to add a row to
   the comparison table alongside the k6 runs.
5. Discuss: does JMeter's closed-model run agree with k6's closed-model
   run? What arrival rate would JMeter need (non-GUI, possibly
   distributed) to reproduce the same overload signature k6 showed at
   70 req/s?

## Honesty note on what was actually run

- **k6 results above are real, captured runs** against the FastAPI app
  in this repo (see terminal output timestamps in this session).
- **JMeter was not executed** — it isn't installed in this environment.
  `jmeter/payment_test.jmx` is a complete, valid test plan implementing
  the same journey, but its numbers in class should come from an actual
  run, not be assumed to match k6's.
