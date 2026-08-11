"""
redBus-style payment REST API for Module 3 (JMeter / k6 load testing).

Deliberately built so a load test must get three things right, matching
the module's topics:

1. Correlation & auth parameterization: /api/payments requires a bearer
   token obtained per-customer from /api/customers/login. A script that
   hardcodes one token will collide across "customers".

2. Parameterization of transaction_id and amount: transaction_id must be
   unique (idempotency) -- reusing one from a CSV row across iterations
   produces a 409, which is a *test-data* bug, not a payment defect.

3. Business-valid negative outcome vs. real failure: a customer with
   insufficient balance gets HTTP 200 + status="DECLINED" (a valid
   business response the test must assert on, not treat as an error).
   A gateway-capacity overload gets HTTP 503 (a real infra failure).
   Load-test assertions that don't separate these two will either hide
   real errors inside "declines" or panic over expected declines.
"""
import threading
import time
import uuid
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import loadtest_runner

app = FastAPI(title="redBus Payment API Load Lab")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

TOKENS: dict[str, str] = {}          # token -> customer_id
SEEN_TXN_IDS: set[str] = set()       # idempotency guard
BALANCES: dict[str, int] = {}        # customer_id -> balance (seeded lazily)

GATEWAY_CAPACITY = 8
_gateway_semaphore = threading.Semaphore(GATEWAY_CAPACITY)


class LoginRequest(BaseModel):
    customer_id: str


class PaymentRequest(BaseModel):
    transaction_id: str
    amount: int


def _balance_for(customer_id: str) -> int:
    if customer_id not in BALANCES:
        # Deterministic seed: customers ending in "0" are low-balance to
        # reliably exercise the DECLINED business path in the lab.
        BALANCES[customer_id] = 100 if customer_id.endswith("0") else 5000
    return BALANCES[customer_id]


def _require_token(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    token = authorization.removeprefix("Bearer ")
    customer_id = TOKENS.get(token)
    if not customer_id:
        raise HTTPException(401, "invalid token")
    return customer_id


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.post("/api/customers/login")
def login(req: LoginRequest):
    token = uuid.uuid4().hex
    TOKENS[token] = req.customer_id
    return {"token": token, "customer_id": req.customer_id}


@app.post("/api/payments")
def pay(req: PaymentRequest, authorization: str | None = Header(None)):
    customer_id = _require_token(authorization)

    if req.transaction_id in SEEN_TXN_IDS:
        raise HTTPException(409, f"duplicate transaction_id {req.transaction_id}")
    SEEN_TXN_IDS.add(req.transaction_id)

    acquired = _gateway_semaphore.acquire(timeout=2)
    if not acquired:
        # Real infra failure -- must NOT be conflated with a business decline.
        raise HTTPException(503, "payment gateway busy, try again")
    try:
        time.sleep(0.15)
        balance = _balance_for(customer_id)
        if req.amount > balance:
            # Valid business outcome -- HTTP 200, not an error.
            return {
                "transaction_id": req.transaction_id,
                "status": "DECLINED",
                "reason": "insufficient balance",
            }
        BALANCES[customer_id] = balance - req.amount
        return {"transaction_id": req.transaction_id, "status": "SUCCESS", "amount": req.amount}
    finally:
        _gateway_semaphore.release()


@app.get("/api/health")
def health():
    return {"status": "ok", "gateway_capacity": GATEWAY_CAPACITY, "txns_seen": len(SEEN_TXN_IDS)}


# --- hands-on lab console: trigger real k6 runs from the browser ---------

class LoadTestRunRequest(BaseModel):
    scenario: str
    base_url: str
    rate: int | None = None


class JMeterEntryRequest(BaseModel):
    label: str
    p95_ms: float
    error_rate_pct: float
    throughput_rps: float


@app.post("/api/loadtest/run")
def start_loadtest(req: LoadTestRunRequest):
    try:
        run = loadtest_runner.start_run(req.scenario, req.rate, req.base_url)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return asdict(run)


@app.get("/api/loadtest/{run_id}")
def get_loadtest(run_id: str):
    run = loadtest_runner.get_run(run_id)
    if not run:
        raise HTTPException(404, "run not found")
    return asdict(run)


@app.get("/api/loadtest")
def list_loadtests():
    return [asdict(r) for r in loadtest_runner.list_runs()]


@app.post("/api/loadtest/jmeter")
def add_jmeter_entry(req: JMeterEntryRequest):
    entry = loadtest_runner.record_jmeter_entry(
        req.label, req.p95_ms, req.error_rate_pct, req.throughput_rps
    )
    return asdict(entry)


@app.get("/api/loadtest/jmeter/all")
def list_jmeter_entries():
    return [asdict(e) for e in loadtest_runner.list_jmeter_entries()]
