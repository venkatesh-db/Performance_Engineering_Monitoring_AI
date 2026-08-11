"""Seed the redbus_lab Postgres database with a realistic catalog size.

Run once after `createdb redbus_lab && psql redbus_lab -f db/schema.sql`.
"""
import asyncio
import random
from datetime import date

import asyncpg

OPERATORS = ["VRL", "SRS", "Orange Tours", "KPN", "Kallada"]
ROUTES = [("Bangalore", "Chennai"), ("Bangalore", "Hyderabad"), ("Pune", "Mumbai")]


async def main() -> None:
    conn = await asyncpg.connect("postgresql://localhost/redbus_lab")
    rows = []
    for _ in range(4000):
        src, dst = random.choice(ROUTES)
        rows.append((
            random.choice(OPERATORS), src, dst, date(2026, 8, 15),
            f"{random.randint(5, 23):02d}:00", random.randint(400, 1500),
        ))
    await conn.executemany(
        "INSERT INTO buses (operator, source, destination, travel_date, departure, fare) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        rows,
    )
    print(f"seeded {len(rows)} buses")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
