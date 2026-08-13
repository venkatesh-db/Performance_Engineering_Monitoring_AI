"""
IRCTC-style ticket booking API for Day 2 / Module 1 (application + Linux
performance analysis: CPU, memory, disk, network/FD, and distinguishing
load-generator vs. application vs. dependency bottlenecks).

Three deliberate, OS-observable defects:

1. CPU-bound seat allocation: /api/book runs a brute-force O(seats^2)
   availability scan on every call -- shows up as a real CPU spike
   (top/htop CPU%, vmstat/pidstat run-queue and %usr) that gets worse
   under concurrent Tatkal-rush load, not just slower one request at a
   time.
2. Unbounded in-memory waitlist + synchronous disk log with fsync on
   every booking: memory grows without bound (visible in top/pidstat -r
   RSS trend) and every booking blocks on a real disk write+fsync
   (visible in iostat -x await/%util, pidstat -d).
3. Leaked sockets to a simulated external SMS/payment gateway on
   /api/pnr/{pnr}: a fraction of calls intentionally never close their
   socket, so open file descriptors and ESTABLISHED/CLOSE_WAIT sockets
   accumulate under load -- visible via lsof -p <pid> / ss (Linux) and
   psutil's cross-platform equivalent exposed at /api/proc-metrics.

/api/proc-metrics exposes the same signals top/vmstat/iostat/pidstat/ss
would show, via psutil, so this also runs somewhere without those Linux
tools installed (this dev box is macOS) -- but the case study and lab
steps are written for the actual Linux training environment.
"""
import asyncio
import hashlib
import os
import socket
import threading
import time
from pathlib import Path

import psutil
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

GATEWAY_PORT = 9500
LOG_PATH = Path(__file__).parent / "booking_log.txt"
SEAT_COUNT = 1200  # tuned so the O(n^2) scan burns real, visible CPU per call

app = FastAPI(title="IRCTC Ticket Booking Profiling Lab")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

PROCESS = psutil.Process(os.getpid())

# BUG 2: unbounded growth -- never trimmed, simulating a waitlist cache
# that's supposed to be cleared nightly but never is.
WAITLIST: list[dict] = []

# BUG 3: sockets we deliberately "forget" to close, simulating a legacy
# blocking gateway-integration bug. Kept referenced so they're not GC'd
# and their FDs stay open, observable via lsof / psutil open_files+connections.
LEAKED_SOCKETS: list[socket.socket] = []

_booking_counter = 0
_counter_lock = threading.Lock()


class BookingRequest(BaseModel):
    train_no: str
    passenger_name: str
    class_type: str = "SL"


def _allocate_seat(train_no: str, passenger_name: str) -> int:
    """BUG 1: brute-force O(n^2) seat scan -- real, deliberate CPU burn."""
    seats = [f"{train_no}-{i}" for i in range(SEAT_COUNT)]
    taken = {s for s in seats if int(hashlib.md5(s.encode()).hexdigest(), 16) % 7 == 0}
    best = None
    for i, seat in enumerate(seats):
        if seat in taken:
            continue
        # Redundant nested comparison against every other seat -- the
        # "brute force" part; a real fix would use a set/index lookup.
        score = sum(1 for other in seats if other != seat and other not in taken)
        if best is None or score > best[1]:
            best = (i, score)
    return best[0] if best else -1


async def _gateway_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await asyncio.sleep(0.3)
    writer.write(b"OK")
    await writer.drain()
    writer.close()


async def _start_gateway():
    server = await asyncio.start_server(_gateway_handler, "127.0.0.1", GATEWAY_PORT)
    return server


@app.on_event("startup")
async def startup():
    app.state.gateway_server = await _start_gateway()


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.post("/api/book")
async def book_ticket(req: BookingRequest):
    global _booking_counter
    t0 = time.perf_counter()

    loop = asyncio.get_running_loop()
    seat = await loop.run_in_executor(None, _allocate_seat, req.train_no, req.passenger_name)
    cpu_ms = round((time.perf_counter() - t0) * 1000, 1)

    with _counter_lock:
        _booking_counter += 1
        pnr = f"PNR{_booking_counter:08d}"

    booking = {
        "pnr": pnr, "train_no": req.train_no, "passenger_name": req.passenger_name,
        "class_type": req.class_type, "seat": seat, "booked_at": time.time(),
    }
    WAITLIST.append(booking)  # BUG 2: never trimmed

    # BUG 2 (disk half): synchronous write + fsync on every booking.
    t1 = time.perf_counter()
    with open(LOG_PATH, "a") as f:
        f.write(f"{pnr},{req.train_no},{req.passenger_name},{seat},{time.time()}\n")
        f.flush()
        os.fsync(f.fileno())
    disk_ms = round((time.perf_counter() - t1) * 1000, 1)

    return {
        "pnr": pnr, "seat": seat if seat >= 0 else "WAITLISTED",
        "cpu_ms": cpu_ms, "disk_ms": disk_ms,
        "waitlist_size": len(WAITLIST),
    }


@app.get("/api/pnr/{pnr}")
async def check_pnr(pnr: str):
    t0 = time.perf_counter()
    loop = asyncio.get_running_loop()

    def _call_gateway():
        sock = socket.create_connection(("127.0.0.1", GATEWAY_PORT), timeout=5)
        sock.recv(2)
        return sock

    sock = await loop.run_in_executor(None, _call_gateway)
    gateway_ms = round((time.perf_counter() - t0) * 1000, 1)

    # BUG 3: ~30% of calls "forget" to close the socket.
    leaked = (hash(pnr) % 10) < 3
    if leaked:
        LEAKED_SOCKETS.append(sock)
    else:
        sock.close()

    match = next((b for b in WAITLIST if b["pnr"] == pnr), None)
    return {
        "pnr": pnr,
        "status": "CONFIRMED" if match and match["seat"] >= 0 else "WAITLISTED" if match else "NOT_FOUND",
        "gateway_ms": gateway_ms,
        "socket_leaked_this_call": leaked,
        "total_leaked_sockets": len(LEAKED_SOCKETS),
    }


@app.get("/api/proc-metrics")
def proc_metrics():
    with PROCESS.oneshot():
        cpu_percent = PROCESS.cpu_percent(interval=0.1)
        mem = PROCESS.memory_info()
        num_threads = PROCESS.num_threads()
        try:
            num_fds = PROCESS.num_fds()
        except AttributeError:
            num_fds = len(PROCESS.open_files()) + len(PROCESS.connections())
        try:
            io = PROCESS.io_counters()
            io_read_mb = round(io.read_bytes / 1e6, 2)
            io_write_mb = round(io.write_bytes / 1e6, 2)
        except (AttributeError, psutil.AccessDenied):
            io_read_mb = io_write_mb = None
        conns_by_status: dict[str, int] = {}
        for c in PROCESS.connections():
            conns_by_status[c.status] = conns_by_status.get(c.status, 0) + 1

    return {
        "cpu_percent": cpu_percent,
        "memory_rss_mb": round(mem.rss / 1e6, 2),
        "num_threads": num_threads,
        "num_fds": num_fds,
        "io_read_mb": io_read_mb,
        "io_write_mb": io_write_mb,
        "connections_by_status": conns_by_status,
        "waitlist_size": len(WAITLIST),
        "leaked_sockets": len(LEAKED_SOCKETS),
    }


@app.post("/api/admin/reset")
def reset():
    WAITLIST.clear()
    for s in LEAKED_SOCKETS:
        try:
            s.close()
        except Exception:
            pass
    LEAKED_SOCKETS.clear()
    if LOG_PATH.exists():
        LOG_PATH.unlink()
    return {"reset": True}
