"""Seed irctc_dependency_lab with a realistic-scale catalog (50,000 rows)
so EXPLAIN ANALYZE and index-cost differences are actually measurable."""
import asyncio
import random
from datetime import date, timedelta

import asyncpg

TRAINS = [f"{12000 + i}" for i in range(200)]  # 200 distinct trains
DATES = [date(2026, 8, 15) + timedelta(days=i) for i in range(30)]  # 30 travel dates


async def main() -> None:
    conn = await asyncpg.connect("postgresql://localhost/irctc_dependency_lab")
    rows = []
    for train_no in TRAINS:
        for d in DATES:
            rows.append((
                train_no, d, random.randint(0, 120),
                random.random() > 0.05,  # ~95% is_active=true -> low selectivity
            ))
    await conn.executemany(
        "INSERT INTO availability (train_no, travel_date, seats_available, is_active) "
        "VALUES ($1, $2, $3, $4)",
        rows,
    )
    print(f"seeded {len(rows)} availability rows across {len(TRAINS)} trains x {len(DATES)} dates")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
