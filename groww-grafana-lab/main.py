"""
Groww-style trading order API for Day 3 / Module 2: Grafana Dashboards
and Alerts.

Real prometheus_client instrumentation, feeding a real Prometheus that
a real Grafana (provisioned via grafana/provisioning/) builds RED/USE
dashboards and alert rules from. See grafana/README.md for how the
dashboard and alerts are wired.

Two bugs were found and fixed in the Licious Prometheus lab (Day 3 /
Module 1) that this app avoids from the start -- worth calling out
explicitly since they're exactly the kind of mistake this module's
"dashboard and alert review practices" topic is about catching before
they reach a dashboard:

1. Endpoint label cardinality: the route path must be read AFTER
   call_next() (Starlette only populates request.scope["route"] with
   the templated path during routing), otherwise every distinct
   product/stock ID becomes its own permanent time series.
2. The request counter must increment in a `finally` block, not after
   a bare try/except that re-raises -- otherwise every 5xx response
   silently never gets counted, and an "error rate" panel/alert built
   on that counter will always read 0% no matter how broken the app is.
"""
import asyncio
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

PG_DSN = "postgresql://localhost/groww_lab"
REDIS_URL = "redis://localhost:6379/0"
POOL_MAX_SIZE = 4
CACHE_TTL_SECONDS = 5  # short TTL -- LTP is meant to look "live"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "order.events"
PROMETHEUS_URL = "http://localhost:9091"

HTTP_REQUESTS = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
HTTP_ACTIVE = Gauge("http_requests_active", "Requests currently being processed")

DB_POOL_MAX = Gauge("db_pool_max_size", "Configured max pool size")
DB_POOL_IN_USE = Gauge("db_pool_in_use", "Postgres connections currently checked out")

CACHE_HITS = Counter("cache_hits_total", "Redis cache hits", ["endpoint"])
CACHE_MISSES = Counter("cache_misses_total", "Redis cache misses", ["endpoint"])

KAFKA_PUBLISHED = Counter("kafka_events_published_total", "Order events published to Kafka")
KAFKA_PUBLISH_ERRORS = Counter("kafka_events_publish_errors_total", "Order events that failed to publish")
KAFKA_CONSUMED = Counter("kafka_events_consumed_total", "Order events consumed")
KAFKA_CONSUMER_LAG = Gauge("kafka_consumer_lag_seconds", "Most recent observed consumer lag")

pg_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None
kafka_producer = None
kafka_error: str | None = None
_consumer_stop = threading.Event()


def _kafka_consumer_loop() -> None:
    from kafka import KafkaConsumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="groww-metrics-consumer",
            auto_offset_reset="latest", value_deserializer=lambda v: json.loads(v.decode()),
            api_version=(3, 7, 0), consumer_timeout_ms=1000,
        )
    except Exception:
        return
    while not _consumer_stop.is_set():
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp_records in records.values():
                for record in tp_records:
                    lag = time.time() - record.value.get("produced_at", time.time())
                    KAFKA_CONSUMER_LAG.set(lag)
                    KAFKA_CONSUMED.inc()
                    time.sleep(0.05)
        except Exception:
            time.sleep(1)
    consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client, kafka_producer, kafka_error
    pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=POOL_MAX_SIZE)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    DB_POOL_MAX.set(POOL_MAX_SIZE)
    try:
        from kafka import KafkaProducer
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode(),
            request_timeout_ms=2000, api_version=(3, 7, 0),
        )
    except Exception as e:  # noqa: BLE001 -- Kafka is optional infra for this lab
        kafka_error = str(e)

    if kafka_producer:
        threading.Thread(target=_kafka_consumer_loop, daemon=True).start()

    yield
    _consumer_stop.set()
    await pg_pool.close()
    await redis_client.aclose()
    if kafka_producer:
        kafka_producer.close()


app = FastAPI(title="Groww Trading Grafana Lab", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    
    if request.url.path in ("/metrics", "/favicon.ico"):
        return await call_next(request)
    HTTP_ACTIVE.inc()
    t0 = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        HTTP_ACTIVE.dec()
        route = request.scope.get("route")
        endpoint = route.path if route else request.url.path
        HTTP_LATENCY.labels(request.method, endpoint).observe(time.perf_counter() - t0)
        HTTP_REQUESTS.labels(request.method, endpoint, str(status)).inc()


@app.get("/metrics")
async def metrics():
    # Must be async def, not a plain def: FastAPI runs sync def endpoints
    # in a separate threadpool worker thread, but asyncpg.Pool is only
    # safe to read from the event loop thread it was created on. A sync
    # def here silently read stale/incorrect pool state under load --
    # found by comparing against /api/summary (already async def, and
    # correct) while both were queried during the same real burst.
    if pg_pool:
        DB_POOL_IN_USE.set(pg_pool.get_size() - pg_pool.get_idle_size())
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


class OrderRequest(BaseModel):
    symbol: str
    side: str
    qty: int
    customer_id: str


@app.get("/api/stocks")
async def list_stocks():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, name, sector, ltp FROM stocks ORDER BY symbol")
    return [{"symbol": r["symbol"], "name": r["name"], "sector": r["sector"], "ltp": float(r["ltp"])} for r in rows]


@app.get("/api/stocks/{symbol}")
async def get_stock(symbol: str):
    cache_key = f"ltp:{symbol}"
    cached = await redis_client.get(cache_key)
    if cached:
        CACHE_HITS.labels("get_stock").inc()
        return {"cached": True, **json.loads(cached)}

    CACHE_MISSES.labels("get_stock").inc()
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT symbol, name, sector, ltp FROM stocks WHERE symbol = $1", symbol)
        await asyncio.sleep(0.08)  # simulated market-data-fetch cost while holding the connection
    if not row:
        raise HTTPException(404, "symbol not found")
    result = {"symbol": row["symbol"], "name": row["name"], "sector": row["sector"], "ltp": float(row["ltp"])}
    await redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL_SECONDS)
    return {"cached": False, **result}


@app.post("/api/orders")
async def place_order(req: OrderRequest):
    if req.side not in ("BUY", "SELL"):
        raise HTTPException(400, "side must be BUY or SELL")
    async with pg_pool.acquire() as conn:
        stock = await conn.fetchrow("SELECT ltp FROM stocks WHERE symbol = $1", req.symbol)
        if not stock:
            raise HTTPException(404, "symbol not found")
        row = await conn.fetchrow(
            "INSERT INTO orders (symbol, side, qty, price, customer_id) VALUES ($1, $2, $3, $4, $5) "
            "RETURNING id, created_at",
            req.symbol, req.side, req.qty, stock["ltp"], req.customer_id,
        )

    kafka_published = False
    if kafka_producer:
        try:
            kafka_producer.send(KAFKA_TOPIC, {
                "order_id": row["id"], "symbol": req.symbol, "side": req.side,
                "qty": req.qty, "customer_id": req.customer_id, "produced_at": time.time(),
            })
            kafka_producer.flush(timeout=2)
            kafka_published = True
            KAFKA_PUBLISHED.inc()
        except Exception:
            KAFKA_PUBLISH_ERRORS.inc()

    return {"order_id": row["id"], "created_at": row["created_at"].isoformat(), "kafka_published": kafka_published}


@app.get("/api/summary")
async def summary():
    return {
        "db_pool_in_use": pg_pool.get_size() - pg_pool.get_idle_size(),
        "db_pool_max": POOL_MAX_SIZE,
        "kafka_available": kafka_producer is not None,
        "kafka_error": kafka_error,
    }


def _prom_instant_query(query: str) -> float | None:
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError):
        return None
    results = data.get("data", {}).get("result", [])
    if not results:
        return None
    try:
        f = float(results[0]["value"][1])
    except (TypeError, ValueError):
        return None
    return None if f != f else f  # filter NaN


PROM_DASHBOARD_QUERIES = {
    "request_rate_per_sec": "sum(rate(http_requests_total[1m]))",
    "error_rate_pct": '100 * sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))',
    "p95_latency_ms": "1000 * histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[1m])) by (le))",
    "active_requests": "http_requests_active",
    "db_pool_pct": "100 * db_pool_in_use / db_pool_max_size",
    "cache_hit_ratio_pct": "100 * sum(rate(cache_hits_total[5m])) / (sum(rate(cache_hits_total[5m])) + sum(rate(cache_misses_total[5m])))",
    "kafka_lag_seconds": "kafka_consumer_lag_seconds",
}


@app.get("/api/dashboard-metrics")
async def dashboard_metrics():
    loop = asyncio.get_running_loop()
    results = await asyncio.gather(*[
        loop.run_in_executor(None, _prom_instant_query, q) for q in PROM_DASHBOARD_QUERIES.values()
    ])
    values = dict(zip(PROM_DASHBOARD_QUERIES.keys(), results))
    return {"prometheus_reachable": any(v is not None for v in values.values()), "metrics": values}


@app.post("/api/admin/reset")
async def reset():
    await redis_client.flushdb()
    return {"reset": True}


@app.post("/api/admin/inject-error")
async def inject_error():
    """Deliberately triggers a real 500 (integer overflow in the DB
    driver) -- used to prove the error-rate panel/alert actually reacts
    to real errors instead of just looking plausible."""
    async with pg_pool.acquire() as conn:
        await conn.fetchrow("SELECT * FROM orders WHERE id = $1", 99999999999999999999)
    return {"should_not_reach_here": True}
