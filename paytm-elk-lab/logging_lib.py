"""
Shared structured-logging module for Day 3 / Module 4 (ELK Stack and
Cross-Signal Incident Analysis). Every log line is shipped to BOTH a
real Loki instance AND a real Elasticsearch instance simultaneously --
same event, same JSON body, same trace_id/correlation_id -- so the lab
can genuinely compare the Loki/Grafana investigation workflow against
the ELK/Kibana one on identical data, not two different datasets.

Design choices matching this module's topics:

1. Elasticsearch index is date-based (`paytm-logs-YYYY.MM.DD`) -- the
   standard ELK index-lifecycle pattern (one index per day, so old
   indices can be rolled off/deleted independently of new ones).
2. Loki labels stay low-cardinality (job/service/level), same
   discipline as Day 3 / Module 3 -- this module's "Loki versus ELK"
   comparison only makes sense if both are configured correctly, not
   if one is deliberately crippled.
3. Elasticsearch, by contrast, is INTENDED to index high-cardinality
   fields like trace_id and correlation_id -- that's the actual
   "appropriate use case" difference the module wants: Loki indexes
   only labels and does full-text search within streams; Elasticsearch
   indexes every field, at real storage/compute cost, for structured
   query flexibility Loki doesn't offer.
"""
import json
import logging
import time
import urllib.request
from contextlib import suppress
from datetime import datetime, timezone

LOKI_PUSH_URL = "http://localhost:3101/loki/api/v1/push"  # this project maps Loki to host port 3101, not the container-internal 3100
ES_URL = "http://localhost:9200"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.time(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for key in ("trace_id", "correlation_id", "customer_id", "merchant_id", "endpoint", "duration_ms", "status", "upi_ref"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload)


class LokiHandler(logging.Handler):
    def __init__(self, service: str, job: str = "paytm"):
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
            pass


class ElasticsearchHandler(logging.Handler):
    """Indexes each log line as its own Elasticsearch document, into a
    date-based daily index -- unlike Loki, every field here becomes
    independently searchable/filterable in Kibana, including
    trace_id/correlation_id."""

    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            doc = json.loads(line)
            doc["service"] = self.service
            index = f"paytm-logs-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"
            body = json.dumps(doc).encode()
            req = urllib.request.Request(
                f"{ES_URL}/{index}/_doc", data=body, headers={"Content-Type": "application/json"}, method="POST"
            )
            with suppress(Exception):
                urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass


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

    es = ElasticsearchHandler(service=service)
    es.setFormatter(JsonFormatter())
    logger.addHandler(es)

    return logger
