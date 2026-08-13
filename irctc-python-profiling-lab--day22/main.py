"""
IRCTC-style fare estimation + refund history API for Day 2 / Module 2
(Python application profiling: cProfile/pstats, py-spy, tracemalloc,
blocking I/O inside async paths, before/after comparison).

Three deliberate, profiler-findable defects, each with a `fixed=true`
alternate path for the lab's "apply an improvement, re-run, compare" step:

1. CPU hotspot: `_compute_fare_matrix` is a naive O(n^3) recomputation
   with no memoization -- the top frame in any cProfile/py-spy capture
   of /api/fare/estimate. `_compute_fare_matrix_fixed` memoizes it.

2. Blocking I/O inside an async request path: /api/fare/estimate calls
   `time.sleep()` directly (simulating a legacy synchronous rules-engine
   HTTP call) with NO executor hand-off -- this blocks the entire
   asyncio event loop, so every other concurrent request (even a trivial
   health check) stalls behind it. `fixed=true` awaits `asyncio.sleep()`
   instead, freeing the event loop.

3. Memory growth: /api/refund/history appends a full snapshot dict to a
   global list on every call and never trims it -- a tracemalloc target
   that shows a specific line as the top allocator. `fixed=true` caps
   the list at a bounded size.

/api/debug/profile/{start,stop} wrap cProfile.Profile() around a load
window and return the real top-cumulative-time functions. Also see
PYSPY_NOTES.md for live py-spy capture against this process's real PID.
"""
import asyncio
import time
import tracemalloc
import cProfile
import io
import pstats
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="IRCTC Fare & Refund Profiling Lab")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

REFUND_HISTORY: list[dict] = []
_fare_cache: dict[tuple, float] = {}
_profiler: cProfile.Profile | None = None
_tracemalloc_started = False
_tracemalloc_baseline = None


class FareRequest(BaseModel):
    train_no: str
    distance_km: int
    passengers: int
    fixed: bool = False


def _compute_fare_matrix(distance_km: int, passengers: int) -> float:
    """BUG 1: naive O(n^3) recomputation every call -- top cProfile frame."""
    total = 0.0
    for i in range(distance_km):
        for p in range(passengers):
            for tier in range(3):  # sleeper / AC3 / AC2 surcharge tiers
                total += (i * 0.5 + p * 1.2 + tier * 3.7) % 97
    return round(total, 2)


def _compute_fare_matrix_fixed(distance_km: int, passengers: int) -> float:
    """FIX: memoized -- same math, computed once per (distance, passengers)."""
    key = (distance_km, passengers)
    if key in _fare_cache:
        return _fare_cache[key]
    total = _compute_fare_matrix(distance_km, passengers)
    _fare_cache[key] = total
    return total


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/health")
async def health():
    """Trivial endpoint used to prove event-loop-wide blocking: if this
    is slow WHILE a /api/fare/estimate(fixed=false) call is in flight,
    the blocking bug is confirmed, not just "that one endpoint is slow"."""
    return {"status": "ok", "ts": time.time()}


@app.post("/api/fare/estimate")
async def estimate_fare(req: FareRequest):
    t0 = time.perf_counter()

    if req.fixed:
        # FIX: hand blocking legacy-rules-engine call off the event loop,
        # and use asyncio.sleep so it doesn't block anything either way.
        await asyncio.sleep(0.3)
        fare_base = await asyncio.get_running_loop().run_in_executor(
            None, _compute_fare_matrix_fixed, req.distance_km, req.passengers
        )
    else:
        # BUG 2: blocking call directly on the event loop thread.
        time.sleep(0.3)
        # BUG 1: unmemoized O(n^3) recomputation, also on the event loop thread.
        fare_base = _compute_fare_matrix(req.distance_km, req.passengers)

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
    return {
        "train_no": req.train_no, "fare": fare_base * req.passengers,
        "elapsed_ms": elapsed_ms, "mode": "fixed" if req.fixed else "buggy",
    }


@app.get("/api/refund/history")
async def refund_history(fixed: bool = False):
    snapshot = {
        "ts": time.time(),
        "records": [{"pnr": f"PNR{i:06d}", "amount": i * 3.5} for i in range(200)],
    }
    REFUND_HISTORY.append(snapshot)
    if fixed:
        # FIX: bounded history -- keep only the most recent 20 snapshots.
        del REFUND_HISTORY[:-20]
    return {"history_len": len(REFUND_HISTORY), "latest_ts": snapshot["ts"]}


@app.post("/api/debug/profile/start")
def profile_start():
    global _profiler
    _profiler = cProfile.Profile()
    _profiler.enable()
    return {"profiling": True}


@app.post("/api/debug/profile/stop")
def profile_stop():
    global _profiler
    if not _profiler:
        return {"error": "profiler was not running"}
    _profiler.disable()
    stream = io.StringIO()
    stats = pstats.Stats(_profiler, stream=stream).sort_stats("cumulative")
    stats.print_stats(10)
    _profiler = None

    top_functions = []
    for func, (cc, nc, tt, ct, callers) in stats.stats.items():
        top_functions.append({
            "function": f"{func[0].split('/')[-1]}:{func[1]}({func[2]})",
            "ncalls": nc, "cumtime": round(ct, 4), "percall_ms": round((ct / nc) * 1000, 3) if nc else 0,
        })
    top_functions.sort(key=lambda f: f["cumtime"], reverse=True)
    return {"top_functions": top_functions[:20]}


@app.post("/api/debug/tracemalloc/start")
def tracemalloc_start():
    global _tracemalloc_started, _tracemalloc_baseline
    tracemalloc.start()
    _tracemalloc_baseline = tracemalloc.take_snapshot()
    _tracemalloc_started = True
    return {"tracemalloc_started": True}


@app.get("/api/debug/tracemalloc/snapshot")
def tracemalloc_snapshot():
    if not _tracemalloc_started:
        return {"error": "call /api/debug/tracemalloc/start first"}
    current = tracemalloc.take_snapshot()
    top_stats = current.compare_to(_tracemalloc_baseline, "lineno")[:10]
    return {
        "top_allocations": [
            {
                "file_line": str(stat.traceback[0]),
                "size_diff_kb": round(stat.size_diff / 1024, 1),
                "count_diff": stat.count_diff,
            }
            for stat in top_stats
        ]
    }


@app.post("/api/admin/reset")
def reset():
    global _fare_cache
    REFUND_HISTORY.clear()
    _fare_cache = {}
    return {"reset": True}
