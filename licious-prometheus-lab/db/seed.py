"""Seed licious_lab with a small meat/seafood catalog."""
import asyncio
import random

import asyncpg

CATEGORIES = ["Chicken", "Mutton", "Seafood", "Eggs", "Ready-to-cook"]
NAMES = {
    "Chicken": ["Chicken Curry Cut", "Chicken Breast", "Chicken Wings", "Chicken Mince"],
    "Mutton": ["Mutton Curry Cut", "Mutton Keema", "Mutton Chops"],
    "Seafood": ["Pomfret", "Prawns", "Salmon", "Basa Fillet"],
    "Eggs": ["Farm Eggs (6pc)", "Farm Eggs (12pc)"],
    "Ready-to-cook": ["Chicken Seekh Kebab", "Fish Fingers", "Chicken Nuggets"],
}


async def main() -> None:
    conn = await asyncpg.connect("postgresql://localhost/licious_lab")
    rows = []
    for category, names in NAMES.items():
        for name in names:
            rows.append((name, category, random.randint(150, 700), random.randint(0, 200)))
    await conn.executemany(
        "INSERT INTO products (name, category, price, stock_qty) VALUES ($1, $2, $3, $4)", rows
    )
    print(f"seeded {len(rows)} products")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
