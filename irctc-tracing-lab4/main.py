"""
IRCTC-style payment API for Day 2 / Module 4: real OpenTelemetry
instrumentation, exported via OTLP to a real Jaeger instance
(localhost:16686), with manual spans across Postgres, Redis, and Kafka,
context propagation over a Kafka message header, and a toggleable
sampler for the sampling-strategy topic.

Real infra:
- PostgreSQL: reuses `irctc_dependency_lab` DB (Module 3), adds a
  `payments` table.
- Redis: reuses the isolated container on :6380 (Module 3).
- Kafka: reuses the broker on :9092 (Module 4/3), new topic
  `payment.events`. Trace context is injected into the Kafka message's
  headers so `consumer.py` can continue the SAME trace in a separate
  process -- this is "context propagation across HTTP and Kafka."
- Jaeger: OTLP/HTTP exporter to localhost:4318/v1/traces; view traces
  at http://localhost:16686.
"""
import asyncio
import json
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import Decision, ParentBased, Sampler, SamplingResult
from opentelemetry.trace import SpanKind, Status, StatusCode

PG_DSN = "postgresql://localhost/irctc_dependency_lab"
REDIS_URL = "redis://localhost:6380/0"
KAFKA_BOOTSTRAP = "localhost:9092"
PAYMENT_TOPIC = "payment.events"
OTLP_ENDPOINT = "http://localhost:4318/v1/traces"

# --- toggleable sampler for the "sampling strategy" topic -----------------
# A real opentelemetry Sampler whose rate can change at runtime via the API,
# so the lab can compare "how many traces actually reach Jaeger" at
# different rates without restarting the process.
CURRENT_SAMPLE_RATE = 1.0


class ToggleableSampler(Sampler):
    """Only ever consulted for ROOT spans when wrapped in ParentBased --
    without that wrapper, the SDK calls should_sample() for EVERY span,
    not just roots, so child spans independently re-roll the dice and a
    "20%" rate ends up sampling nearly every trace (verified: with ~8
    spans per request, P(at least one of 8 independent 20% rolls hits)
    is ~83%, which is exactly the bug this lab hit and fixed)."""
    def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None):
        if random.random() < CURRENT_SAMPLE_RATE:
            return SamplingResult(Decision.RECORD_AND_SAMPLE, attributes, trace_state)
        return SamplingResult(Decision.DROP, None, trace_state)

    def get_description(self) -> str:
        return "ToggleableSampler"


resource = Resource.create({"service.name": "irctc-payment-api"})
provider = TracerProvider(resource=resource, sampler=ParentBased(ToggleableSampler()))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("irctc.payment")

pg_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None
kafka_producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client, kafka_producer
    pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=5)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS payments (id SERIAL PRIMARY KEY, train_no TEXT, "
            "amount INTEGER, status TEXT, created_at TIMESTAMPTZ DEFAULT now())"
        )
    try:
        from kafka import KafkaProducer
        kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, api_version=(3, 7, 0))
    except Exception:
        kafka_producer = None
    yield
    await pg_pool.close()
    await redis_client.aclose()
    if kafka_producer:
        kafka_producer.close()


app = FastAPI(title="IRCTC Payment Tracing Lab", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)  # automatic instrumentation
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


class PaymentRequest(BaseModel):
    train_no: str
    amount: int
    slow_db: bool = False
    force_fraud_check_fail: bool = False


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.post("/api/payment")
async def make_payment(req: PaymentRequest):
    current_span = trace.get_current_span()
    trace_id = format(current_span.get_span_context().trace_id, "032x")

    # --- manual span 1: idempotency cache check (Redis) ---
    with tracer.start_as_current_span("cache-check-idempotency", kind=SpanKind.CLIENT) as span:
        cache_key = f"payment-lock:{req.train_no}:{req.amount}"
        span.set_attribute("cache.key", cache_key)
        existing = await redis_client.get(cache_key)
        span.set_attribute("cache.hit", existing is not None)
        if existing:
            span.add_event("duplicate payment blocked")
            return {"trace_id": trace_id, "status": "DUPLICATE_BLOCKED"}
        await redis_client.set(cache_key, "1", ex=10)

    # --- manual span 2: fraud check (attributes + events + status) ---
    with tracer.start_as_current_span("fraud-check") as span:
        span.set_attribute("payment.amount", req.amount)
        span.add_event("fraud check started")
        await asyncio.sleep(0.05)
        if req.force_fraud_check_fail or req.amount > 100000:
            span.set_status(Status(StatusCode.ERROR, "fraud check failed: amount exceeds threshold"))
            span.add_event("fraud check failed", {"reason": "amount_exceeds_threshold"})
            raise HTTPException(402, "Payment blocked by fraud check")
        span.add_event("fraud check passed")

    # --- manual span 3: charge in Postgres (the deliberate slow span) ---
    with tracer.start_as_current_span("db-charge", kind=SpanKind.CLIENT) as span:
        span.set_attribute("db.system", "postgresql")
        span.set_attribute("db.statement", "INSERT INTO payments ...")
        if req.slow_db:
            span.add_event("simulated lock contention")
            await asyncio.sleep(0.6)  # the deliberate critical-path bottleneck
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO payments (train_no, amount, status) VALUES ($1, $2, 'CHARGED') RETURNING id",
                req.train_no, req.amount,
            )
        payment_id = row["id"]
        span.set_attribute("payment.id", payment_id)

    # --- manual span 4: publish to Kafka WITH trace-context propagation ---
    with tracer.start_as_current_span("kafka-publish", kind=SpanKind.PRODUCER) as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination", PAYMENT_TOPIC)
        headers: dict[str, str] = {}
        inject(headers)  # writes traceparent into `headers` -- THIS is context propagation
        kafka_headers = [(k, v.encode()) for k, v in headers.items()]
        kafka_published = False
        if kafka_producer:
            try:
                kafka_producer.send(
                    PAYMENT_TOPIC,
                    value=json.dumps({"payment_id": payment_id, "train_no": req.train_no, "amount": req.amount}).encode(),
                    headers=kafka_headers,
                )
                kafka_producer.flush(timeout=2)
                kafka_published = True
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
        span.set_attribute("kafka.published", kafka_published)

    return {"trace_id": trace_id, "payment_id": payment_id, "status": "SUCCESS", "kafka_published": kafka_published}


@app.get("/api/tracing/sample-rate")
def get_sample_rate():
    return {"sample_rate": CURRENT_SAMPLE_RATE}


@app.post("/api/tracing/sample-rate")
def set_sample_rate(rate: float):
    global CURRENT_SAMPLE_RATE
    CURRENT_SAMPLE_RATE = max(0.0, min(1.0, rate))
    return {"sample_rate": CURRENT_SAMPLE_RATE}


@app.post("/api/admin/reset")
async def reset():
    async with pg_pool.acquire() as conn:
        await conn.execute("DELETE FROM payments")
    await redis_client.flushdb()
    return {"reset": True}
