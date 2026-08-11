// redBus payment API load test in k6.
// Demonstrates: correlation (login -> token -> payment), CSV-driven
// parameterization, unique transaction_id per iteration, checks/thresholds,
// custom metrics that separate business DECLINED from real errors, and
// both VU-based and arrival-rate executors for direct comparison.
//
// Run VU-based scenario only:
//   k6 run k6/payment_test.js --env SCENARIO=vus
// Run arrival-rate scenario only:
//   k6 run k6/payment_test.js --env SCENARIO=arrival
// Run both (as defined below) and produce an HTML-friendly JSON summary:
//   k6 run k6/payment_test.js --summary-export=k6/summary.json

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8002';

const successCount = new Counter('biz_success');
const declinedCount = new Counter('biz_declined');
const errorCount = new Counter('infra_error');
const paymentLatency = new Trend('payment_latency_ms');

const customers = new SharedArray('customers', function () {
  const csv = open('../test_data/customers.csv');
  const lines = csv.trim().split('\n').slice(1);
  return lines.map((line) => {
    const [customer_id, amount] = line.split(',');
    return { customer_id, amount: parseInt(amount, 10) };
  });
});

const scenarioName = __ENV.SCENARIO || 'vus';

const SCENARIOS = {
  vus: {
    executor: 'constant-vus',
    vus: 20,
    duration: '20s',
    exec: 'runJourney',
    tags: { model: 'closed-vu-based' },
  },
  arrival: {
    executor: 'constant-arrival-rate',
    rate: Number(__ENV.RATE || 15),
    timeUnit: '1s',
    duration: '20s',
    preAllocatedVUs: 50,
    maxVUs: 200,
    exec: 'runJourney',
    tags: { model: 'open-arrival-rate' },
  },
};

export const options = {
  scenarios: { [scenarioName]: SCENARIOS[scenarioName] },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  thresholds: {
    'infra_error': ['count<5'],              // real failures must stay rare
    'payment_latency_ms': ['p(95)<2000'],     // latency SLA
    'checks': ['rate>0.95'],                  // overall check pass rate
  },
};

export function runJourney() {
  const row = customers[Math.floor(Math.random() * customers.length)];
  const txnId = `TXN-${__VU}-${__ITER}-${Math.random().toString(36).slice(2, 8)}`;

  const loginRes = http.post(
    `${BASE_URL}/api/customers/login`,
    JSON.stringify({ customer_id: row.customer_id }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'login' } }
  );
  check(loginRes, { 'login succeeded': (r) => r.status === 200 });
  const token = loginRes.json('token');

  const t0 = Date.now();
  const payRes = http.post(
    `${BASE_URL}/api/payments`,
    JSON.stringify({ transaction_id: txnId, amount: row.amount }),
    {
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      tags: { name: 'payment' },
    }
  );
  paymentLatency.add(Date.now() - t0);

  // Error validation: separate a valid business decline from a real failure.
  if (payRes.status === 200) {
    const status = payRes.json('status');
    check(payRes, { 'payment response has known status': () => ['SUCCESS', 'DECLINED'].includes(status) });
    if (status === 'SUCCESS') successCount.add(1);
    else declinedCount.add(1);
  } else if (payRes.status === 409) {
    // duplicate transaction_id -- a test-data bug, not counted as infra error
    check(payRes, { 'duplicate txn correctly rejected': (r) => r.status === 409 });
  } else {
    errorCount.add(1);
    check(payRes, { 'no unexpected server error': () => false });
  }
}
