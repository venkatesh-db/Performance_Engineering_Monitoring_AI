"""
IRCTC-style seat-availability dependency-analysis lab for Day 2 /
Module 3: PostgreSQL EXPLAIN evidence + index selectivity, Kafka
partition parallelism + consumer lag, Redis hot keys + eviction, and
backpressure/retry amplification.

Real infra, no simulation:
- PostgreSQL: `irctc_dependency_lab` DB, 50,000-row `availability` table.
- Redis: a DEDICATED isolated container on :6380 (maxmemory=3MB,
  allkeys-lru) -- NOT the host's real Redis -- so the eviction demo
  can't affect anything else running on this machine.
- Kafka: reuses the broker from Module 4's docker-compose (localhost:9092),
  with two topics of different partition counts to demonstrate
  parallelism's effect on consumer lag.

Four deliberate, evidence-producing scenarios:

1. PostgreSQL: a redundant, low-selectivity index on `is_active`
   (~95% true) that the planner ignores for selective queries (provable
   via EXPLAIN) but that still costs every INSERT/UPDATE to maintain
   (provable via timed bulk writes with the index present vs. dropped).

2. Kafka: `availability.events.p1` (1 partition) vs
   `availability.events.p3` (3 partitions) -- same producer rate, same
   slow-consumer delay, different achievable parallelism and therefore
   different consumer lag.

3. Redis: a hot-key access pattern (most checks hit train "12000") plus
   a tiny maxmemory budget, so eviction and hit-ratio degradation are
   real and observable via Redis's own INFO stats -- not fabricated.

4. Backpressure: an undersized Postgres pool plus a naive-retry client
   path that amplifies load on failure, vs. a backoff+capped-retry path
   that doesn't.
"""
import asyncio
import json
import random
import time
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PG_DSN = "postgresql://localhost/irctc_dependency_lab"
REDIS_URL = "redis://localhost:6380/0"  # dedicated isolated container, NOT the host's real Redis
POOL_MAX_SIZE = 3
CACHE_TTL_SECONDS = 20
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_P1 = "availability.events.p1"
TOPIC_P3 = "availability.events.p3"

pg_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client
    pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=POOL_MAX_SIZE)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    yield
    await pg_pool.close()
    await redis_client.aclose()


app = FastAPI(title="IRCTC Dependency Analysis Lab", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


# --- 1. PostgreSQL: EXPLAIN evidence + index selectivity -----------------

@app.get("/api/availability/{train_no}")
async def get_availability(train_no: str, travel_date: str = "2026-08-20"):
    cache_key = f"avail:{train_no}:{travel_date}"
    t0 = time.perf_counter()
    cached = await redis_client.get(cache_key)
    if cached:
        return {"cached": True, "latency_ms": round((time.perf_counter() - t0) * 1000, 2), **json.loads(cached)}

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT train_no, travel_date, seats_available, is_active FROM availability "
            "WHERE train_no = $1 AND travel_date = $2 LIMIT 1",
            train_no, date.fromisoformat(travel_date),
        )
    result = {"train_no": train_no, "seats_available": row["seats_available"] if row else None,
              "found": row is not None}
    await redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL_SECONDS)
    return {"cached": False, "latency_ms": round((time.perf_counter() - t0) * 1000, 2), **result}


@app.get("/api/admin/explain")
async def explain_query(train_no: str = "12000", travel_date: str = "2026-08-20", filter_active: bool = False):
    """Real EXPLAIN (ANALYZE, FORMAT JSON) -- not a canned example."""
    query = ("SELECT train_no, travel_date, seats_available FROM availability "
             "WHERE train_no = $1 AND travel_date = $2")
    if filter_active:
        query += " AND is_active = true"
    async with pg_pool.acquire() as conn:
        plan = await conn.fetchval(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}", train_no, date.fromisoformat(travel_date))
    plan_json = json.loads(plan)[0]
    node = plan_json["Plan"]
    return {
        "query": query,
        "node_type": node["Node Type"],
        "index_name": node.get("Index Name"),
        "planning_time_ms": plan_json.get("Planning Time"),
        "execution_time_ms": plan_json.get("Execution Time"),
        "actual_rows": node.get("Actual Rows"),
    }


@app.post("/api/admin/index/{action}")
async def toggle_low_selectivity_index(action: str):
    if action not in ("create", "drop"):
        raise HTTPException(400, "action must be create or drop")
    async with pg_pool.acquire() as conn:
        if action == "create":
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_availability_is_active ON availability (is_active)")
        else:
            await conn.execute("DROP INDEX IF EXISTS idx_availability_is_active")
    return {"index_present": action == "create"}


@app.post("/api/admin/bulk-update")
async def bulk_update(rows: int = 5000):
    """Times a bulk UPDATE that touches is_active on every row -- the
    write-cost side of the low-selectivity-index story."""
    async with pg_pool.acquire() as conn:
        t0 = time.perf_counter()
        await conn.execute(
            f"UPDATE availability SET is_active = NOT is_active, updated_at = now() "
            f"WHERE id IN (SELECT id FROM availability ORDER BY id LIMIT {int(rows)})"
        )
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        idx_exists = await _index_exists(conn)
    return {"rows_updated": rows, "elapsed_ms": elapsed_ms, "low_selectivity_index_present": idx_exists}


@app.get("/api/admin/index/stats")
async def index_stats():
    """The decisive evidence for 'unnecessary index cost': idx_scan=0
    means the planner has NEVER used this index, even once, while it
    still occupies real disk space and is maintained on every write."""
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT indexrelname, idx_scan, idx_tup_read, pg_relation_size(indexrelid) AS size_bytes "
            "FROM pg_stat_user_indexes WHERE relname = 'availability' ORDER BY indexrelname"
        )
    return [
        {"index": r["indexrelname"], "times_used_by_planner": r["idx_scan"],
         "rows_read_via_index": r["idx_tup_read"], "size_kb": round(r["size_bytes"] / 1024, 1)}
        for r in rows
    ]


async def _index_exists(conn) -> bool:
    return await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_availability_is_active')"
    )


# --- 2. Kafka: partition parallelism + consumer lag -----------------------

@app.post("/api/admin/kafka/produce")
async def kafka_produce(topic: str = TOPIC_P1, count: int = 100, rate: float = 50):
    """Produces `count` availability-check events at `rate`/sec to the
    given topic. Run against p1 and p3 and compare consumer lag with
    kafka/consumer_lag_test.py."""
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        api_version=(3, 7, 0),
    )
    interval = 1.0 / rate if rate > 0 else 0
    futures = []
    t0 = time.time()
    for i in range(count):
        train_no = str(12000 + (i % 200))
        futures.append(producer.send(topic, {"train_no": train_no, "seq": i, "produced_at": time.time()}, key=train_no.encode()))
        if interval:
            await asyncio.sleep(interval)
    sent = errors = 0
    for f in futures:
        try:
            f.get(timeout=5)
            sent += 1
        except Exception:
            errors += 1
    return {"topic": topic, "sent": sent, "errors": errors, "elapsed_s": round(time.time() - t0, 2)}


# --- 3. Redis: hot key + eviction ------------------------------------------

_storm_call_counter = 0


@app.post("/api/admin/redis/hotkey-storm")
async def redis_hotkey_storm(count: int = 200, hot_ratio: float = 0.8):
    """Fires `count` availability lookups, `hot_ratio` of them against
    ONE train (the hot key), rest spread across many distinct, NEVER-
    REPEATED cache keys (to pressure the 3MB maxmemory budget and force
    real evictions -- reusing the same key names across calls would
    just overwrite, not grow, the keyspace)."""
    global _storm_call_counter
    _storm_call_counter += 1
    call_id = _storm_call_counter

    travel_date = "2026-08-20"
    hits = misses = 0
    for i in range(count):
        if random.random() < hot_ratio:
            train_no = "12000"  # the hot key
        else:
            train_no = f"synthetic-{call_id}-{i}"  # globally unique key every call
        cache_key = f"avail:{train_no}:{travel_date}"
        cached = await redis_client.get(cache_key)
        if cached:
            hits += 1
        else:
            misses += 1
            async with pg_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT seats_available FROM availability WHERE train_no = $1 AND travel_date = $2 LIMIT 1",
                    train_no, date.fromisoformat(travel_date),
                )
            await redis_client.set(cache_key, json.dumps({"seats_available": row["seats_available"] if row else None}), ex=CACHE_TTL_SECONDS)
    return {"requests": count, "hits": hits, "misses": misses, "hit_ratio": round(hits / count, 3)}


@app.get("/api/admin/redis/stats")
async def redis_stats():
    info = await redis_client.info()
    return {
        "used_memory_mb": round(info.get("used_memory", 0) / 1e6, 3),
        "maxmemory_mb": round(info.get("maxmemory", 0) / 1e6, 3),
        "keyspace_hits": info.get("keyspace_hits"),
        "keyspace_misses": info.get("keyspace_misses"),
        "evicted_keys": info.get("evicted_keys"),
        "maxmemory_policy": info.get("maxmemory_policy"),
    }


# --- 4. Backpressure / retry amplification --------------------------------

@app.get("/api/admin/pool-stats")
async def pool_stats():
    return {
        "max_size": pg_pool.get_max_size(),
        "current_size": pg_pool.get_size(),
        "idle": pg_pool.get_idle_size(),
        "in_use": pg_pool.get_size() - pg_pool.get_idle_size(),
    }


async def _slow_query():
    async with pg_pool.acquire() as conn:
        await conn.fetchval("SELECT pg_sleep(0.3)")


@app.post("/api/admin/retry-storm")
async def retry_storm(concurrency: int = 10, naive: bool = True):
    """Fires `concurrency` concurrent callers against the undersized
    pool. Naive callers retry immediately with no backoff and no cap on
    a pool-timeout; backoff callers wait with exponential backoff and
    give up after 3 tries. Counts total underlying attempts issued --
    the amplification number."""
    total_attempts = 0
    total_successes = 0
    lock = asyncio.Lock()

    async def naive_caller():
        nonlocal total_attempts, total_successes
        for _ in range(20):  # naive: hammer until it works, no cap in practice
            async with lock:
                total_attempts += 1
            try:
                await asyncio.wait_for(_slow_query(), timeout=0.5)
                async with lock:
                    total_successes += 1
                return
            except asyncio.TimeoutError:
                continue  # immediate retry, no backoff

    async def backoff_caller():
        nonlocal total_attempts, total_successes
        delay = 0.1
        for attempt in range(3):  # capped at 3 tries
            async with lock:
                total_attempts += 1
            try:
                await asyncio.wait_for(_slow_query(), timeout=0.5)
                async with lock:
                    total_successes += 1
                return
            except asyncio.TimeoutError:
                await asyncio.sleep(delay)
                delay *= 2

    t0 = time.time()
    caller = naive_caller if naive else backoff_caller
    await asyncio.gather(*[caller() for _ in range(concurrency)])
    return {
        "mode": "naive" if naive else "backoff",
        "concurrency": concurrency,
        "total_attempts": total_attempts,
        "total_successes": total_successes,
        "amplification_factor": round(total_attempts / concurrency, 2),
        "elapsed_s": round(time.time() - t0, 2),
    }


@app.post("/api/admin/reset")
async def reset():
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE availability SET is_active = true")
    await redis_client.flushdb()
    await redis_client.execute_command("CONFIG", "RESETSTAT")
    return {"reset": True}
