"""
Paytm-style UPI payment API for Day 3 / Module 4: ELK Stack and
Cross-Signal Incident Analysis. Real OpenTelemetry tracing (shared
Jaeger), real Prometheus metrics, and real structured JSON logs shipped
to BOTH Loki and Elasticsearch simultaneously -- see logging_lib.py for
why both, and CASE_STUDY.md for the guided Loki-vs-Kibana comparison.
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

from logging_lib import setup_logging

PG_DSN = "postgresql://localhost/paytm_lab"
REDIS_URL = "redis://localhost:6379/0"
POOL_MAX_SIZE = 4
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "paytm.payment.events"
OTLP_ENDPOINT = "http://localhost:4318/v1/traces"  # shared Jaeger from Day 2 / Module 4

resource = Resource(attributes={SERVICE_NAME: "paytm-payment-api"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("paytm-payment-api")

logger = setup_logging("paytm-payment-api")

HTTP_REQUESTS = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
HTTP_ACTIVE = Gauge("http_requests_active", "Requests currently being processed")
DB_POOL_MAX = Gauge("db_pool_max_size", "Configured max pool size")
DB_POOL_IN_USE = Gauge("db_pool_in_use", "Postgres connections currently checked out")
KAFKA_PUBLISHED = Counter("kafka_events_published_total", "Payment events published to Kafka")

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
    except Exception as e:  # noqa: BLE001
        kafka_error = str(e)
    yield
    await pg_pool.close()
    await redis_client.aclose()
    if kafka_producer:
        kafka_producer.close()


app = FastAPI(title="Paytm ELK Incident Analysis Lab", lifespan=lifespan)
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
                extra={"trace_id": trace_id, "correlation_id": correlation_id,
                       "endpoint": endpoint, "duration_ms": round(elapsed_ms, 1), "status": status},
            )


@app.get("/metrics")
async def metrics():
    if pg_pool:
        DB_POOL_IN_USE.set(pg_pool.get_size() - pg_pool.get_idle_size())
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


class PaymentRequest(BaseModel):
    merchant_id: int
    customer_id: str
    amount: int


@app.get("/api/merchants")
async def list_merchants():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, category FROM merchants ORDER BY id")
    return [{"id": r["id"], "name": r["name"], "category": r["category"]} for r in rows]


@app.post("/api/payments")
async def make_payment(req: PaymentRequest, request: Request):
    correlation_id = request.state.correlation_id
    trace_id = request.state.trace_id
    upi_ref = f"UPI{uuid.uuid4().hex[:12].upper()}"

    with tracer.start_as_current_span("make_payment") as span:
        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("merchant_id", req.merchant_id)

        async with pg_pool.acquire() as conn:
            merchant = await conn.fetchrow("SELECT id, name FROM merchants WHERE id = $1", req.merchant_id)
            if not merchant:
                raise HTTPException(404, "merchant not found")
            row = await conn.fetchrow(
                "INSERT INTO payments (merchant_id, customer_id, amount, upi_ref) VALUES ($1, $2, $3, $4) "
                "RETURNING id, created_at",
                req.merchant_id, req.customer_id, req.amount, upi_ref,
            )

        logger.info(
            "payment initiated",
            extra={
                "trace_id": trace_id, "correlation_id": correlation_id, "customer_id": req.customer_id,
                "merchant_id": req.merchant_id, "upi_ref": upi_ref,
            },
        )

        kafka_published = False
        if kafka_producer:
            try:
                headers = []
                carrier: dict[str, str] = {}
                inject(carrier)
                for k, v in carrier.items():
                    headers.append((k, v.encode()))
                headers.append(("correlation_id", correlation_id.encode()))

                kafka_producer.send(
                    KAFKA_TOPIC,
                    value={
                        "payment_id": row["id"], "merchant_id": req.merchant_id, "customer_id": req.customer_id,
                        "amount": req.amount, "upi_ref": upi_ref, "correlation_id": correlation_id,
                        "produced_at": time.time(),
                    },
                    headers=headers,
                )
                kafka_producer.flush(timeout=2)
                kafka_published = True
                KAFKA_PUBLISHED.inc()
            except Exception as e:
                logger.error(f"kafka publish failed: {e}", extra={"trace_id": trace_id, "correlation_id": correlation_id})

    return {
        "payment_id": row["id"], "upi_ref": upi_ref, "created_at": row["created_at"].isoformat(),
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
