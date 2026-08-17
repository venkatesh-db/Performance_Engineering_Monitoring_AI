"""
Payment API — training lab target.

Deliberately contains four performance defects for the cohort to find:
  1. N+1 query in GET /customers/{id}/transactions
  2. Blocking time.sleep() inside an async path (fraud check)
  3. No cache on the hot customer lookup
  4. Undersized connection pool

Run:  uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
"""
import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

DB_DSN = os.getenv("DB_DSN", "postgresql://postgres:postgres@localhost:5432/payments")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Deliberately small. Day 1 M4 experiment raises this.
POOL_MIN = int(os.getenv("POOL_MIN", "2"))
POOL_MAX = int(os.getenv("POOL_MAX", "5"))

# Toggles so the cohort can prove a fix with an identical workload.
USE_CACHE = os.getenv("USE_CACHE", "false").lower() == "true"
FIX_NPLUS1 = os.getenv("FIX_NPLUS1", "false").lower() == "true"
ASYNC_FRAUD = os.getenv("ASYNC_FRAUD", "false").lower() == "true"

state = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["db"] = await asyncpg.create_pool(DB_DSN, min_size=POOL_MIN, max_size=POOL_MAX)
    state["redis"] = aioredis.from_url(REDIS_URL, decode_responses=True)
    await init_schema()
    yield
    await state["db"].close()
    await state["redis"].aclose()


app = FastAPI(title="Payment API — Perf Lab", lifespan=lifespan)


async def init_schema():
    async with state["db"].acquire() as c:
        await c.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                balance NUMERIC(14,2) NOT NULL DEFAULT 100000,
                tier TEXT NOT NULL DEFAULT 'STANDARD'
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount NUMERIC(14,2) NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        n = await c.fetchval("SELECT count(*) FROM customers")
        if n == 0:
            rows = [(f"CUST{i:04d}", f"Customer {i}", 100000, "STANDARD") for i in range(1, 501)]
            await c.executemany(
                "INSERT INTO customers (id,name,balance,tier) VALUES ($1,$2,$3,$4)", rows
            )


# ---------------------------------------------------------------- models

class PaymentRequest(BaseModel):
    customerId: str
    amount: float
    idempotencyKey: str


class TokenRequest(BaseModel):
    username: str
    password: str


# ---------------------------------------------------------------- auth

@app.post("/auth/token")
async def login(req: TokenRequest):
    if req.password != "test123":
        raise HTTPException(status_code=401, detail="bad credentials")
    token = f"tok_{uuid.uuid4().hex}"
    await state["redis"].setex(f"session:{token}", 900, req.username)
    return {"access_token": token, "token_type": "bearer", "expires_in": 900}


async def require_token(authorization: str | None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await state["redis"].get(f"session:{token}")
    if not user:
        raise HTTPException(status_code=401, detail="token expired")
    return user


# ---------------------------------------------------------------- health

@app.get("/health")
async def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/config")
async def config():
    """Shows which defects are currently active. Useful for before/after runs."""
    return {
        "pool_max": POOL_MAX,
        "use_cache": USE_CACHE,
        "fix_nplus1": FIX_NPLUS1,
        "async_fraud": ASYNC_FRAUD,
    }


# ---------------------------------------------------------------- customer

@app.get("/customers/{customer_id}")
async def get_customer(customer_id: str):
    """DEFECT 3: cache is off by default, so every read hits Postgres."""
    if USE_CACHE:
        cached = await state["redis"].get(f"cust:{customer_id}")
        if cached:
            return {"source": "cache", "data": cached}

    async with state["db"].acquire() as c:
        row = await c.fetchrow("SELECT * FROM customers WHERE id=$1", customer_id)
    if not row:
        raise HTTPException(status_code=404, detail="customer not found")

    payload = {"id": row["id"], "name": row["name"],
               "balance": float(row["balance"]), "tier": row["tier"]}
    if USE_CACHE:
        await state["redis"].setex(f"cust:{customer_id}", 60, str(payload))
    return {"source": "db", "data": payload}


@app.get("/customers/{customer_id}/transactions")
async def list_transactions(customer_id: str, limit: int = 20):
    """DEFECT 1: N+1. One query for the list, then one per row."""
    async with state["db"].acquire() as c:
        txns = await c.fetch(
            "SELECT * FROM transactions WHERE customer_id=$1 "
            "ORDER BY created_at DESC LIMIT $2", customer_id, limit)

        if FIX_NPLUS1:
            # batched: one extra query total
            ids = list({t["customer_id"] for t in txns})
            custs = await c.fetch("SELECT id,name FROM customers WHERE id = ANY($1)", ids)
            lookup = {r["id"]: r["name"] for r in custs}
            result = [{"id": t["id"], "amount": float(t["amount"]),
                       "status": t["status"], "customerName": lookup.get(t["customer_id"])}
                      for t in txns]
        else:
            # N+1: one query per transaction
            result = []
            for t in txns:
                cust = await c.fetchrow("SELECT name FROM customers WHERE id=$1",
                                        t["customer_id"])
                result.append({"id": t["id"], "amount": float(t["amount"]),
                               "status": t["status"],
                               "customerName": cust["name"] if cust else None})

    return {"customerId": customer_id, "count": len(result), "transactions": result}


# ---------------------------------------------------------------- fraud

async def fraud_check(amount: float):
    """DEFECT 2: blocking sleep freezes the whole event loop."""
    if ASYNC_FRAUD:
        await asyncio.sleep(0.05)      # correct: yields to the loop
    else:
        time.sleep(0.05)               # wrong: blocks every concurrent request
    return "HIGH" if amount > 50000 else "LOW"


# ---------------------------------------------------------------- payments

@app.post("/payments")
async def create_payment(req: PaymentRequest, authorization: str = Header(None)):
    await require_token(authorization)

    # idempotency guard — same key returns the original, never a duplicate write
    existing = await state["redis"].get(f"idem:{req.idempotencyKey}")
    if existing:
        return {"transactionId": existing, "status": "SUCCESS", "duplicate": True}

    risk = await fraud_check(req.amount)

    async with state["db"].acquire() as c:
        cust = await c.fetchrow("SELECT balance FROM customers WHERE id=$1", req.customerId)
        if not cust:
            raise HTTPException(status_code=404, detail="customer not found")

        # NOTE: business decline, not a system error. Still HTTP 200.
        if float(cust["balance"]) < req.amount:
            return {"transactionId": None, "status": "DECLINED",
                    "code": "INSUFFICIENT_FUNDS", "risk": risk}

        txn_id = f"TXN{uuid.uuid4().hex[:16].upper()}"
        async with c.transaction():
            await c.execute(
                "UPDATE customers SET balance = balance - $1 WHERE id = $2",
                req.amount, req.customerId)
            await c.execute(
                "INSERT INTO transactions (id,customer_id,amount,status) "
                "VALUES ($1,$2,$3,$4)", txn_id, req.customerId, req.amount, "COMPLETED")

    await state["redis"].setex(f"idem:{req.idempotencyKey}", 3600, txn_id)
    return {"transactionId": txn_id, "status": "SUCCESS", "risk": risk, "duplicate": False}


@app.get("/payments/{txn_id}")
async def get_payment(txn_id: str):
    async with state["db"].acquire() as c:
        row = await c.fetchrow("SELECT * FROM transactions WHERE id=$1", txn_id)
    if not row:
        raise HTTPException(status_code=404, detail="transaction not found")
    return {"transactionId": row["id"], "customerId": row["customer_id"],
            "amount": float(row["amount"]), "status": row["status"]}
