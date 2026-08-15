"""
Licious-style order API for Day 3 / Module 1: Prometheus Metrics and PromQL.

Real prometheus_client instrumentation (no simulation) covering every
hands-on-lab bullet:
- automatic HTTP metrics via ASGI middleware: request rate, error rate,
  P95/P99 latency, active requests
- manual business metrics: DB pool usage, Redis hit ratio, Kafka
  consumer lag, Kafka events published/consumed
- /metrics exposes them in real Prometheus exposition format, scraped
  by a real Prometheus server (see prometheus/docker-compose.yml)

Two real-load scenarios to make the metrics tell an actual story:
1. A "flash sale" surge (many concurrent orders against an undersized
   DB pool) -> visible in db_pool_in_use, request latency P95/P99, and
   http_requests_total{status="5xx"} if the pool queue times out.
2. A cold cache burst (many distinct product lookups) -> visible in
   the cache hit ratio dropping in real time.
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

PG_DSN = "postgresql://localhost/licious_lab"
REDIS_URL = "redis://localhost:6379/0"
POOL_MAX_SIZE = 4
CACHE_TTL_SECONDS = 20
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "order.placed"
PROMETHEUS_URL = "http://localhost:9090"

# --- Prometheus metrics -----------------------------------------------

HTTP_REQUESTS = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"]
)
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
            KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="licious-metrics-consumer",
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
                    time.sleep(0.05)  # simulated per-event processing cost
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

    consumer_thread = None
    if kafka_producer:
        consumer_thread = threading.Thread(target=_kafka_consumer_loop, daemon=True)
        consumer_thread.start()

    yield
    _consumer_stop.set()
    await pg_pool.close()
    await redis_client.aclose()
    if kafka_producer:
        kafka_producer.close()


app = FastAPI(title="Licious Prometheus Lab", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    if request.url.path in ("/metrics", "/favicon.ico"):
        return await call_next(request)
    HTTP_ACTIVE.inc()
    t0 = time.perf_counter()
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        status = 500
        raise
    finally:
        HTTP_ACTIVE.dec()
        # Route matching only populates request.scope["route"] DURING
        # call_next() -- reading it beforehand silently falls back to the
        # raw URL path (e.g. "/api/products/3") instead of the templated
        # route ("/api/products/{product_id}"), which is exactly the
        # unbounded-cardinality mistake this module warns about: every
        # distinct product_id would become its own time series forever.
        route = request.scope.get("route")
        endpoint = route.path if route else request.url.path
        HTTP_LATENCY.labels(request.method, endpoint).observe(time.perf_counter() - t0)
        # BUG (found + fixed while verifying this lab live): this counter
        # increment used to sit AFTER the try/except/finally block, so on
        # any unhandled exception the `raise` above propagated out of the
        # function before that line ever ran -- 5xx responses were being
        # timed (latency histogram) but never counted (request counter),
        # which silently broke every error-rate PromQL query forever.
        # Moving it into `finally` guarantees it always records.
        HTTP_REQUESTS.labels(request.method, endpoint, str(status)).inc()
    return response


@app.get("/metrics")
def metrics():
    if pg_pool:
        DB_POOL_IN_USE.set(pg_pool.get_size() - pg_pool.get_idle_size())
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


class OrderRequest(BaseModel):
    product_id: int
    customer_id: str
    qty: int = 1


@app.get("/api/products/{product_id}")
async def get_product(product_id: int):
    cache_key = f"product:{product_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        CACHE_HITS.labels("get_product").inc()
        return {"cached": True, **json.loads(cached)}

    CACHE_MISSES.labels("get_product").inc()
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, category, price, stock_qty FROM products WHERE id = $1", product_id
        )
        await asyncio.sleep(0.1)  # simulated app-level work while holding the connection
    if not row:
        raise HTTPException(404, "product not found")
    result = {"id": row["id"], "name": row["name"], "category": row["category"],
              "price": row["price"], "stock_qty": row["stock_qty"]}
    await redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL_SECONDS)
    return {"cached": False, **result}


@app.post("/api/orders")
async def place_order(req: OrderRequest):
    async with pg_pool.acquire() as conn:
        product = await conn.fetchrow("SELECT id, price FROM products WHERE id = $1", req.product_id)
        if not product:
            raise HTTPException(404, "product not found")
        row = await conn.fetchrow(
            "INSERT INTO orders (product_id, customer_id, qty) VALUES ($1, $2, $3) RETURNING id, created_at",
            req.product_id, req.customer_id, req.qty,
        )

    kafka_published = False
    if kafka_producer:
        try:
            kafka_producer.send(KAFKA_TOPIC, {
                "order_id": row["id"], "product_id": req.product_id,
                "customer_id": req.customer_id, "qty": req.qty, "produced_at": time.time(),
            })
            kafka_producer.flush(timeout=2)
            kafka_published = True
            KAFKA_PUBLISHED.inc()
        except Exception:
            KAFKA_PUBLISH_ERRORS.inc()

    return {"order_id": row["id"], "created_at": row["created_at"].isoformat(), "kafka_published": kafka_published}


@app.get("/api/summary")
async def summary():
    """Human-readable snapshot of the same numbers /metrics exposes, for the UI panel."""
    return {
        "db_pool_in_use": pg_pool.get_size() - pg_pool.get_idle_size(),
        "db_pool_max": POOL_MAX_SIZE,
        "kafka_available": kafka_producer is not None,
        "kafka_error": kafka_error,
    }


def _prom_instant_query(query: str) -> float | None:
    """Blocking HTTP call to Prometheus's own query API. Run in an
    executor -- server-side, so it isn't subject to the browser's
    cross-origin/sandbox restrictions that block client-side fetches to
    a different port."""
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError):
        return None
    results = data.get("data", {}).get("result", [])
    if not results:
        return None
    value = results[0]["value"][1]
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f  # filter out NaN


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
    """Real PromQL results, queried server-side, rendered directly in
    the storefront UI's performance panel -- no need to leave the app
    or hit sandbox/CORS issues opening Prometheus's own UI."""
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
