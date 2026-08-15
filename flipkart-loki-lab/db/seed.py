"""Seed flipkart_lab with a small electronics/general catalog."""
import asyncio
import random

import asyncpg

PRODUCTS = [
    ("Wireless Earbuds Pro", "Electronics"),
    ("27-inch 4K Monitor", "Electronics"),
    ("Mechanical Keyboard", "Electronics"),
    ("Running Shoes", "Fashion"),
    ("Cotton T-Shirt Pack", "Fashion"),
    ("Air Fryer 4L", "Home"),
    ("Robot Vacuum Cleaner", "Home"),
    ("Yoga Mat", "Sports"),
    ("Adjustable Dumbbell Set", "Sports"),
    ("Novel: The Silent Patient", "Books"),
    ("Bluetooth Speaker", "Electronics"),
    ("Office Chair", "Home"),
]


async def main() -> None:
    conn = await asyncpg.connect("postgresql://localhost/flipkart_lab")
    rows = [(name, cat, random.randint(299, 24999), random.randint(0, 300)) for name, cat in PRODUCTS]
    await conn.executemany(
        "INSERT INTO products (name, category, price, stock_qty) VALUES ($1, $2, $3, $4)", rows
    )
    print(f"seeded {len(rows)} products")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
