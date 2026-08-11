"""
redBus-style bus search & seat-selection demo API.

Intentionally contains three classic performance bugs so a performance
tester can reproduce customer pain points and do root-cause analysis:

1. A single shared SQLite connection with a global lock around every
   query, simulating an under-pooled DB layer -> throughput flatlines
   and P95/P99 latency explodes under concurrency (serialization).
2. No index on (source, destination, travel_date) -> full table scan
   per search request -> latency grows with catalog size and CPU
   utilization climbs toward saturation under load.
3. N+1 query pattern in /seats/{bus_id} -> one query per seat instead
   of one batch query -> response time scales linearly with seat count.
"""
import sqlite3
import threading
import time
import random
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DB_PATH = Path(__file__).parent / "redbus_demo.db"
app = FastAPI(title="redBus Performance Lab")

# BUG 1: one shared connection + one global lock => requests serialize.
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_db_lock = threading.Lock()

SEATS_PER_BUS = 40


def _seed():
    cur = _conn.cursor()
    cur.execute("DROP TABLE IF EXISTS buses")
    cur.execute("DROP TABLE IF EXISTS seats")
    # BUG 2: no index on (source, destination, travel_date)
    cur.execute(
        """CREATE TABLE buses (
            id INTEGER PRIMARY KEY,
            operator TEXT, source TEXT, destination TEXT,
            travel_date TEXT, departure TEXT, fare INTEGER
        )"""
    )
    cur.execute(
        """CREATE TABLE seats (
            id INTEGER PRIMARY KEY,
            bus_id INTEGER, seat_no TEXT, is_booked INTEGER
        )"""
    )
    operators = ["VRL", "SRS", "Orange Tours", "KPN", "Kallada"]
    routes = [("Bangalore", "Chennai"), ("Bangalore", "Hyderabad"), ("Pune", "Mumbai")]
    bus_id = 1
    rows = []
    seat_rows = []
    # Bloat the table so the missing index actually hurts (full scan cost).
    for _ in range(4000):
        src, dst = random.choice(routes)
        rows.append((bus_id, random.choice(operators), src, dst,
                     "2026-08-15", f"{random.randint(5,23):02d}:00",
                     random.randint(400, 1500)))
        for s in range(1, SEATS_PER_BUS + 1):
            seat_rows.append((bus_id, f"S{s}", random.random() < 0.3))
        bus_id += 1
    cur.executemany(
        "INSERT INTO buses (id, operator, source, destination, travel_date, departure, fare) VALUES (?,?,?,?,?,?,?)",
        [(i + 1, *r[1:]) for i, r in enumerate(rows)],
    )
    cur.executemany(
        "INSERT INTO seats (bus_id, seat_no, is_booked) VALUES (?,?,?)",
        seat_rows,
    )
    _conn.commit()


_seed()

app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/search")
def search(source: str = "Bangalore", destination: str = "Chennai", travel_date: str = "2026-08-15"):
    with _db_lock:  # BUG 1: serializes every concurrent search request
        time.sleep(0.05)  # simulated per-connection setup overhead
        cur = _conn.cursor()
        # BUG 2: LIKE + no covering index -> full table scan under load
        cur.execute(
            "SELECT id, operator, source, destination, travel_date, departure, fare "
            "FROM buses WHERE source = ? AND destination = ? AND travel_date = ?",
            (source, destination, travel_date),
        )
        rows = cur.fetchall()
    return {
        "count": len(rows),
        "buses": [
            {"id": r[0], "operator": r[1], "source": r[2], "destination": r[3],
             "travel_date": r[4], "departure": r[5], "fare": r[6]}
            for r in rows
        ],
    }


@app.get("/api/seats/{bus_id}")
def seats(bus_id: int):
    with _db_lock:
        cur = _conn.cursor()
        cur.execute("SELECT id FROM seats WHERE bus_id = ?", (bus_id,))
        seat_ids = [r[0] for r in cur.fetchall()]
        if not seat_ids:
            raise HTTPException(404, "bus not found")

        # BUG 3: N+1 -- one round-trip per seat instead of one IN (...) query
        result = []
        for sid in seat_ids:
            cur.execute("SELECT seat_no, is_booked FROM seats WHERE id = ?", (sid,))
            seat_no, is_booked = cur.fetchone()
            result.append({"seat_no": seat_no, "is_booked": bool(is_booked)})
    return {"bus_id": bus_id, "seats": result}


@app.get("/api/health")
def health():
    return {"status": "ok"}
