"""
Shared structured-logging + Loki-shipping module for Day 3 / Module 3
(Loki and Structured Logging), used by both main.py and consumer.py.

Deliberate design choices matching the module's topics:

1. Loki labels stay LOW cardinality on purpose: only `job`, `service`,
   `level`. transaction_id / trace_id / customer_id are NEVER used as
   Loki labels -- they go in the JSON log body instead, queryable via
   LogQL's `| json` parser. Putting a per-request ID in a label creates
   one Loki stream per value, which is the classic cardinality mistake
   this module warns about. See CASE_STUDY.md for a real, captured
   comparison of what happens if you get this wrong.

2. Every log line carries trace_id (from the active OpenTelemetry span)
   and a correlation_id (transaction/order-level), so a single request
   can be found across FastAPI logs, Kafka-consumer logs, Jaeger traces,
   and Prometheus metrics via the same two IDs.

3. Sensitive fields (email, phone) are masked before logging -- with
   one deliberately-planted gap (see main.py) to demonstrate finding an
   unmasked field via a real Loki query, not just describing the idea.
"""
import json
import logging
import time
import urllib.request
from contextlib import suppress

LOKI_PUSH_URL = "http://localhost:3100/loki/api/v1/push"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.time(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for key in ("trace_id", "correlation_id", "customer_id", "endpoint", "duration_ms", "status"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload)


class LokiHandler(logging.Handler):
    """Minimal, dependency-free handler that pushes each log line to
    Loki's HTTP push API individually. Real production code would
    batch these -- kept simple and synchronous here so the lab's HTTP
    calls to Loki are easy to see and reason about."""

    def __init__(self, service: str, job: str = "flipkart"):
        super().__init__()
        self.service = service
        self.job = job

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            stream = {
                "stream": {"job": self.job, "service": self.service, "level": record.levelname},
                "values": [[str(int(time.time() * 1e9)), line]],
            }
            body = json.dumps({"streams": [stream]}).encode()
            req = urllib.request.Request(
                LOKI_PUSH_URL, data=body, headers={"Content-Type": "application/json"}, method="POST"
            )
            with suppress(Exception):
                urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # logging must never crash the request path


def setup_logging(service: str) -> logging.Logger:
    logger = logging.getLogger(service)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(JsonFormatter())
    logger.addHandler(console)

    loki = LokiHandler(service=service)
    loki.setFormatter(JsonFormatter())
    logger.addHandler(loki)

    return logger


def mask_email(email: str) -> str:
    """user@example.com -> u***@example.com"""
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    return f"{local[0]}***@{domain}" if local else f"***@{domain}"


def mask_phone(phone: str) -> str:
    """9876543210 -> ******3210"""
    return "*" * max(0, len(phone) - 4) + phone[-4:] if len(phone) >= 4 else "****"
