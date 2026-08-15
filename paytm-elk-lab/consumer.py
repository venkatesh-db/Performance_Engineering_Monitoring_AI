"""
Kafka consumer for Day 3 / Module 4 -- simulates the bank-settlement
step of a UPI payment. Continues the SAME trace the API started (via
OpenTelemetry context in Kafka headers), logs structured JSON to BOTH
Loki and Elasticsearch under the same correlation_id.

~12% of settlements deliberately fail with one of three realistic,
DISTINCT reasons (INSUFFICIENT_BALANCE / BANK_TIMEOUT / INVALID_UPI_PIN)
-- for the "investigate a failed payment transaction" hands-on bullet,
so the lab has more than one failure signature to filter for, not just
a single generic "error".
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
KAFKA_TOPIC = "paytm.payment.events"
OTLP_ENDPOINT = "http://localhost:4318/v1/traces"

resource = Resource(attributes={SERVICE_NAME: "paytm-settlement-consumer"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("paytm-settlement-consumer")

logger = setup_logging("paytm-settlement-consumer")

FAILURE_REASONS = ["INSUFFICIENT_BALANCE", "BANK_TIMEOUT", "INVALID_UPI_PIN"]


def process_settlement(upi_ref: str, correlation_id: str, trace_id: str) -> None:
    roll = random.random()
    if roll < 0.12:
        reason = random.choice(FAILURE_REASONS)
        delay = random.uniform(2.0, 4.0) if reason == "BANK_TIMEOUT" else random.uniform(0.1, 0.3)
        time.sleep(delay)
        logger.error(
            f"settlement FAILED: {reason}",
            extra={
                "trace_id": trace_id, "correlation_id": correlation_id,
                "upi_ref": upi_ref, "duration_ms": round(delay * 1000, 1),
            },
        )
        return
    delay = random.uniform(0.1, 0.4)
    time.sleep(delay)
    logger.info(
        "settlement completed",
        extra={"trace_id": trace_id, "correlation_id": correlation_id, "upi_ref": upi_ref, "duration_ms": round(delay * 1000, 1)},
    )


def main() -> None:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="paytm-settlement-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode()),
        api_version=(3, 7, 0),
    )
    print(f"consuming {KAFKA_TOPIC}...")
    for msg in consumer:
        headers = {k: v.decode() for k, v in (msg.headers or [])}
        ctx = extract(headers)
        with tracer.start_as_current_span("process_settlement", context=ctx) as span:
            trace_id = format(span.get_span_context().trace_id, "032x")
            correlation_id = headers.get("correlation_id", "unknown")
            span.set_attribute("correlation_id", correlation_id)
            process_settlement(msg.value["upi_ref"], correlation_id, trace_id)


if __name__ == "__main__":
    main()
