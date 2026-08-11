"""
redBus-style search + booking API for Module 4 (database, Kafka, Redis).

Real PostgreSQL and real Redis (both required to run this app). Kafka
publishing is best-effort and explicitly reported in the response --
if no broker is reachable, the API still works but says so, rather
than silently swallowing the failure.

Deliberate teaching defects, matching the module's topics:

1. Undersized connection pool (POOL_MAX_SIZE = 3): a handful of
   concurrent search requests exhausts it, and later callers queue
   waiting for a free connection -- directly observable via
   /api/health's pool stats.
2. No index on (source, destination, travel_date): every search is a
   full table scan against a 4,000-row catalog.
3. Cache-aside on /api/buses/search with a short TTL, so cached vs.
   uncached response time is directly comparable (hands-on lab step 3).
4. Kafka publish on every booking, with consumer lag reproducible by
   running consumer.py with a slow per-message processing delay
   against a producer.py that fires faster than the consumer drains.
"""
import asyncio
import json
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

PG_DSN = "postgresql://localhost/redbus_lab"
REDIS_URL = "redis://localhost:6379/0"
POOL_MAX_SIZE = 3
CACHE_TTL_SECONDS = 30
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "booking.created"

pg_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None
kafka_producer = None
kafka_error: str | None = None

# Live feed of consumed booking events, for the UI's "what actually went
# through Kafka" panel -- populated by a background consumer thread so
# participants can watch real messages and real consumer lag, not just a
# published/not-published flag on the producer side.
KAFKA_FEED: deque = deque(maxlen=50)
_kafka_feed_lock = threading.Lock()
_kafka_consumer_stop = threading.Event()


def _kafka_feed_consumer() -> None:
    from kafka import KafkaConsumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="lab-ui-feed",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode()),
            api_version=(3, 7, 0),
            consumer_timeout_ms=1000,
        )
    except Exception:
        return
    while not _kafka_consumer_stop.is_set():
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp_records in records.values():
                for record in tp_records:
                    consumed_at = time.time()
                    produced_at = record.value.get("produced_at", consumed_at)
                    with _kafka_feed_lock:
                        KAFKA_FEED.append({
                            "offset": record.offset,
                            "booking_id": record.value.get("booking_id"),
                            "customer_id": record.value.get("customer_id"),
                            "seat_no": record.value.get("seat_no"),
                            "lag_ms": round((consumed_at - produced_at) * 1000, 1),
                        })
        except Exception:
            time.sleep(1)
    consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client, kafka_producer, kafka_error
    pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=POOL_MAX_SIZE)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        from kafka import KafkaProducer
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            request_timeout_ms=2000,
            # kafka-python's automatic API-version probe hangs indefinitely
            # against some broker versions (observed against apache/kafka
            # 3.7.0) -- pinning it explicitly avoids the hang entirely.
            api_version=(3, 7, 0),
        )
    except Exception as e:  # noqa: BLE001 -- Kafka is optional infra for this lab
        kafka_error = str(e)

    feed_thread = None
    if kafka_producer:
        feed_thread = threading.Thread(target=_kafka_feed_consumer, daemon=True)
        feed_thread.start()

    yield
    _kafka_consumer_stop.set()
    await pg_pool.close()
    await redis_client.aclose()
    if kafka_producer:
        kafka_producer.close()


app = FastAPI(title="redBus Data-Tier Lab", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


class BookingRequest(BaseModel):
    bus_id: int
    customer_id: str
    seat_no: str


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/buses/search")
async def search(source: str = "Bangalore", destination: str = "Chennai", travel_date: str = "2026-08-15"):
    cache_key = f"search:{source}:{destination}:{travel_date}"
    t0 = time.perf_counter()

    cached = await redis_client.get(cache_key)
    if cached:
        return {
            "cached": True,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
            **json.loads(cached),
        }

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, operator, source, destination, travel_date, departure, fare "
            "FROM buses WHERE source = $1 AND destination = $2 AND travel_date = $3",
            source, destination, date.fromisoformat(travel_date),
        )
        # Simulated app-level work done while still holding the connection
        # (e.g. serialization, a second dependent query) -- makes the
        # undersized pool (POOL_MAX_SIZE=3) exhaust under realistic load
        # instead of only under extreme concurrency.
        await asyncio.sleep(0.15)

    result = {
        "count": len(rows),
        "buses": [
            {
                "id": r["id"], "operator": r["operator"], "source": r["source"],
                "destination": r["destination"], "travel_date": str(r["travel_date"]),
                "departure": r["departure"], "fare": r["fare"],
            }
            for r in rows
        ],
    }
    await redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL_SECONDS)
    return {"cached": False, "latency_ms": round((time.perf_counter() - t0) * 1000, 2), **result}


@app.post("/api/bookings")
async def create_booking(req: BookingRequest):
    async with pg_pool.acquire() as conn:
        bus = await conn.fetchrow("SELECT id FROM buses WHERE id = $1", req.bus_id)
        if not bus:
            raise HTTPException(404, "bus not found")
        row = await conn.fetchrow(
            "INSERT INTO bookings (bus_id, customer_id, seat_no) VALUES ($1, $2, $3) "
            "RETURNING id, created_at",
            req.bus_id, req.customer_id, req.seat_no,
        )

    kafka_published = False
    kafka_publish_error = kafka_error
    if kafka_producer:
        try:
            kafka_producer.send(KAFKA_TOPIC, {
                "booking_id": row["id"], "bus_id": req.bus_id,
                "customer_id": req.customer_id, "seat_no": req.seat_no,
                "produced_at": time.time(),
            })
            kafka_producer.flush(timeout=2)
            kafka_published = True
        except Exception as e:  # noqa: BLE001 -- reported to caller, not swallowed
            kafka_publish_error = str(e)

    return {
        "booking_id": row["id"],
        "created_at": row["created_at"].isoformat(),
        "kafka_published": kafka_published,
        "kafka_error": kafka_publish_error,
    }


@app.delete("/api/cache/search")
async def clear_search_cache(source: str, destination: str, travel_date: str):
    """Dev-only helper so the UI can reliably demo an uncached call before a cached one."""
    cache_key = f"search:{source}:{destination}:{travel_date}"
    deleted = await redis_client.delete(cache_key)
    return {"deleted": bool(deleted)}


@app.get("/api/kafka/feed")
async def kafka_feed():
    with _kafka_feed_lock:
        return list(reversed(KAFKA_FEED))


@app.get("/api/health")
async def health():
    pool_stats = {
        "max_size": pg_pool.get_max_size(),
        "current_size": pg_pool.get_size(),
        "idle": pg_pool.get_idle_size(),
        "in_use": pg_pool.get_size() - pg_pool.get_idle_size(),
    }
    try:
        redis_ok = await redis_client.ping()
    except Exception:
        redis_ok = False
    return {
        "status": "ok",
        "postgres_pool": pool_stats,
        "redis_ok": redis_ok,
        "kafka_available": kafka_producer is not None,
        "kafka_error": kafka_error,
    }
