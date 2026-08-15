"""
Flipkart-style order API for Day 3 / Module 3: Loki and Structured
Logging. Real OpenTelemetry tracing (exported to Jaeger), real
Prometheus metrics, and real structured JSON logs shipped to a real
Loki instance -- all three signals carry the same trace_id/correlation_id
so a single request is findable across all of them.

Deliberate bug for the hands-on lab to find (see CASE_STUDY.md):
customer_email is masked before logging, but customer_phone is NOT --
a realistic "we masked the obvious field and missed another" gap,
discoverable with a real LogQL query, not just described.
"""
import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

from logging_lib import mask_email, mask_phone, setup_logging

PG_DSN = "postgresql://localhost/flipkart_lab"
REDIS_URL = "redis://localhost:6379/0"
POOL_MAX_SIZE = 4
CACHE_TTL_SECONDS = 15
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "flipkart.order.events"
OTLP_ENDPOINT = "http://localhost:4318/v1/traces"  # reuses the shared Jaeger from Day 2 / Module 4

resource = Resource(attributes={SERVICE_NAME: "flipkart-order-api"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("flipkart-order-api")

logger = setup_logging("flipkart-order-api")

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

pg_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None
kafka_producer = None
kafka_error: str | None = None


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
    yield
    await pg_pool.close()
    await redis_client.aclose()
    if kafka_producer:
        kafka_producer.close()


app = FastAPI(title="Flipkart Loki Logging Lab", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    if request.url.path in ("/metrics", "/favicon.ico"):
        return await call_next(request)

    correlation_id = request.headers.get("x-correlation-id", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id

    with tracer.start_as_current_span(f"{request.method} {request.url.path}") as span:
        trace_id = format(span.get_span_context().trace_id, "032x")
        request.state.trace_id = trace_id
        span.set_attribute("correlation_id", correlation_id)

        HTTP_ACTIVE.inc()
        t0 = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-correlation-id"] = correlation_id
            response.headers["x-trace-id"] = trace_id
            return response
        finally:
            HTTP_ACTIVE.dec()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            route = request.scope.get("route")
            endpoint = route.path if route else request.url.path
            HTTP_LATENCY.labels(request.method, endpoint).observe(elapsed_ms / 1000)
            HTTP_REQUESTS.labels(request.method, endpoint, str(status)).inc()
            logger.info(
                "request completed",
                extra={
                    "trace_id": trace_id, "correlation_id": correlation_id,
                    "endpoint": endpoint, "duration_ms": round(elapsed_ms, 1), "status": status,
                },
            )


@app.get("/metrics")
async def metrics():
    if pg_pool:
        DB_POOL_IN_USE.set(pg_pool.get_size() - pg_pool.get_idle_size())
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


class OrderRequest(BaseModel):
    product_id: int
    customer_id: str
    customer_email: str
    customer_phone: str
    qty: int = 1


@app.get("/api/products")
async def list_products():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, category, price, stock_qty FROM products ORDER BY id")
    return [{"id": r["id"], "name": r["name"], "category": r["category"], "price": r["price"], "stock_qty": r["stock_qty"]} for r in rows]


@app.get("/api/products/{product_id}")
async def get_product(product_id: int, request: Request):
    cache_key = f"product:{product_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        CACHE_HITS.labels("get_product").inc()
        return {"cached": True, **json.loads(cached)}

    CACHE_MISSES.labels("get_product").inc()
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, name, category, price, stock_qty FROM products WHERE id = $1", product_id)
        await asyncio.sleep(0.07)
    if not row:
        raise HTTPException(404, "product not found")
    result = {"id": row["id"], "name": row["name"], "category": row["category"], "price": row["price"], "stock_qty": row["stock_qty"]}
    await redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL_SECONDS)
    return {"cached": False, **result}


@app.post("/api/orders")
async def place_order(req: OrderRequest, request: Request):
    correlation_id = request.state.correlation_id
    trace_id = request.state.trace_id

    with tracer.start_as_current_span("place_order") as span:
        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("product_id", req.product_id)

        async with pg_pool.acquire() as conn:
            product = await conn.fetchrow("SELECT id, price FROM products WHERE id = $1", req.product_id)
            if not product:
                logger.warning(
                    "order failed: product not found",
                    extra={"trace_id": trace_id, "correlation_id": correlation_id, "customer_id": req.customer_id},
                )
                raise HTTPException(404, "product not found")
            row = await conn.fetchrow(
                "INSERT INTO orders (product_id, customer_id, customer_email, qty) VALUES ($1, $2, $3, $4) "
                "RETURNING id, created_at",
                req.product_id, req.customer_id, req.customer_email, req.qty,
            )

        # BUG (deliberate, for the hands-on lab): customer_email IS masked
        # before logging, but customer_phone is logged in full below --
        # find it with a real LogQL query, see CASE_STUDY.md.
        logger.info(
            "order placed",
            extra={
                "trace_id": trace_id, "correlation_id": correlation_id, "customer_id": req.customer_id,
            },
        )
        logger.info(
            json.dumps({
                "event": "order_details", "order_id": row["id"],
                "customer_email_masked": mask_email(req.customer_email),
                "customer_phone_UNMASKED": req.customer_phone,  # <- the planted gap
            }),
            extra={"trace_id": trace_id, "correlation_id": correlation_id, "customer_id": req.customer_id},
        )

        kafka_published = False
        if kafka_producer:
            try:
                headers = []
                carrier: dict[str, str] = {}
                inject(carrier)  # propagate the current trace context into Kafka headers
                for k, v in carrier.items():
                    headers.append((k, v.encode()))
                headers.append(("correlation_id", correlation_id.encode()))

                kafka_producer.send(
                    KAFKA_TOPIC,
                    value={
                        "order_id": row["id"], "product_id": req.product_id, "customer_id": req.customer_id,
                        "qty": req.qty, "correlation_id": correlation_id, "produced_at": time.time(),
                    },
                    headers=headers,
                )
                kafka_producer.flush(timeout=2)
                kafka_published = True
                KAFKA_PUBLISHED.inc()
            except Exception as e:
                logger.error(
                    f"kafka publish failed: {e}",
                    extra={"trace_id": trace_id, "correlation_id": correlation_id},
                )

    return {
        "order_id": row["id"], "created_at": row["created_at"].isoformat(),
        "kafka_published": kafka_published, "correlation_id": correlation_id, "trace_id": trace_id,
    }


@app.get("/api/summary")
async def summary():
    return {
        "db_pool_in_use": pg_pool.get_size() - pg_pool.get_idle_size(),
        "db_pool_max": POOL_MAX_SIZE,
        "kafka_available": kafka_producer is not None,
        "kafka_error": kafka_error,
    }


@app.post("/api/admin/reset")
async def reset():
    await redis_client.flushdb()
    return {"reset": True}
