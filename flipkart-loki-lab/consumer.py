"""
Kafka consumer for Day 3 / Module 3 -- simulates a payment-processing
service. Continues the SAME trace the FastAPI producer started (via
OpenTelemetry context propagated through Kafka message headers), and
logs structured JSON to the same Loki instance under the same
correlation_id -- so one order is findable end-to-end: FastAPI logs ->
Kafka producer log -> this consumer's logs -> the Jaeger trace, all via
trace_id/correlation_id, none of which are Loki labels.

~15% of "payments" are deliberately slow (simulating a flaky payment
gateway) and ~5% deliberately fail -- real, findable-via-LogQL events
for the "find slow or failed payment events" hands-on bullet.
"""
import json
import random
import time

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from logging_lib import setup_logging

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "flipkart.order.events"
OTLP_ENDPOINT = "http://localhost:4318/v1/traces"

resource = Resource(attributes={SERVICE_NAME: "flipkart-payment-consumer"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("flipkart-payment-consumer")

logger = setup_logging("flipkart-payment-consumer")


def process_payment(order_id: int, correlation_id: str, trace_id: str) -> None:
    roll = random.random()
    if roll < 0.05:
        logger.error(
            "payment FAILED: gateway declined",
            extra={"trace_id": trace_id, "correlation_id": correlation_id},
        )
        return
    delay = random.uniform(1.5, 3.0) if roll < 0.20 else random.uniform(0.05, 0.2)
    time.sleep(delay)
    level = "slow" if delay > 1.0 else "normal"
    logger.info(
        f"payment processed ({level}, {delay*1000:.0f}ms)",
        extra={"trace_id": trace_id, "correlation_id": correlation_id, "duration_ms": round(delay * 1000, 1)},
    )


def main() -> None:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="flipkart-payment-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode()),
        api_version=(3, 7, 0),
    )
    print(f"consuming {KAFKA_TOPIC}...")
    for msg in consumer:
        headers = {k: v.decode() for k, v in (msg.headers or [])}
        ctx = extract(headers)
        with tracer.start_as_current_span("process_payment", context=ctx) as span:
            trace_id = format(span.get_span_context().trace_id, "032x")
            correlation_id = headers.get("correlation_id", "unknown")
            span.set_attribute("correlation_id", correlation_id)
            process_payment(msg.value["order_id"], correlation_id, trace_id)


if __name__ == "__main__":
    main()
