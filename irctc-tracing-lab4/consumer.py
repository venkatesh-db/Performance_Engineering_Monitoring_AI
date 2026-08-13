"""
Payment-event consumer for Day 2 / Module 4's context-propagation topic.

Extracts the `traceparent` written into the Kafka message's HEADERS by
main.py's `inject(headers)` call, and starts its own span as a CHILD of
that same trace -- so in Jaeger, "notify-customer" (this process) shows
up nested under the SAME trace_id as the original HTTP request that
triggered the payment, even though it ran later, in a different
process, consuming from a queue. This is what "context propagation
across Kafka" actually means in practice, demonstrated for real.

Run: python consumer.py
"""
import json
import time

from kafka import KafkaConsumer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind

resource = Resource.create({"service.name": "irctc-payment-consumer"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("irctc.payment.consumer")


def main():
    consumer = KafkaConsumer(
        "payment.events",
        bootstrap_servers="localhost:9092",
        group_id="payment-notifier",
        auto_offset_reset="latest",
        api_version=(3, 7, 0),
    )
    print("Listening on payment.events for new payment events (Ctrl+C to stop)...")
    for msg in consumer:
        headers = {k: v.decode() for k, v in (msg.headers or [])}
        ctx = extract(headers)  # reconstructs the trace context from the message headers
        payload = json.loads(msg.value.decode())

        with tracer.start_as_current_span("notify-customer", context=ctx, kind=SpanKind.CONSUMER) as span:
            span.set_attribute("payment.id", payload["payment_id"])
            span.set_attribute("messaging.system", "kafka")
            span.add_event("sending SMS notification")
            time.sleep(0.15)  # simulated SMS gateway call
            span.add_event("notification sent")
            print(f"notified customer for payment_id={payload['payment_id']} train_no={payload['train_no']}")


if __name__ == "__main__":
    main()
