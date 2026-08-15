"""Seed paytm_lab with a small merchant list."""
import asyncio

import asyncpg

MERCHANTS = [
    ("BigBasket Groceries", "Grocery"),
    ("Domino's Pizza", "Food"),
    ("Airtel Prepaid Recharge", "Utilities"),
    ("BESCOM Electricity Bill", "Utilities"),
    ("BookMyShow", "Entertainment"),
    ("Ola Cabs", "Travel"),
    ("Zomato", "Food"),
    ("Croma Electronics", "Shopping"),
    ("Apollo Pharmacy", "Healthcare"),
    ("Metro Card Recharge", "Travel"),
]


async def main() -> None:
    conn = await asyncpg.connect("postgresql://localhost/paytm_lab")
    await conn.executemany("INSERT INTO merchants (name, category) VALUES ($1, $2)", MERCHANTS)
    print(f"seeded {len(MERCHANTS)} merchants")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
