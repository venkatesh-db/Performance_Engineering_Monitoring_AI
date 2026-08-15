"""Seed groww_lab with a small NSE-style stock list."""
import asyncio
import random

import asyncpg

STOCKS = [
    ("RELIANCE", "Reliance Industries", "Energy"),
    ("TCS", "Tata Consultancy Services", "IT"),
    ("INFY", "Infosys", "IT"),
    ("HDFCBANK", "HDFC Bank", "Banking"),
    ("ICICIBANK", "ICICI Bank", "Banking"),
    ("SBIN", "State Bank of India", "Banking"),
    ("ITC", "ITC Limited", "FMCG"),
    ("HINDUNILVR", "Hindustan Unilever", "FMCG"),
    ("BHARTIARTL", "Bharti Airtel", "Telecom"),
    ("WIPRO", "Wipro", "IT"),
    ("TATAMOTORS", "Tata Motors", "Auto"),
    ("MARUTI", "Maruti Suzuki", "Auto"),
    ("SUNPHARMA", "Sun Pharma", "Pharma"),
    ("ADANIENT", "Adani Enterprises", "Diversified"),
    ("ASIANPAINT", "Asian Paints", "Consumer"),
    ("BAJFINANCE", "Bajaj Finance", "NBFC"),
]


async def main() -> None:
    conn = await asyncpg.connect("postgresql://localhost/groww_lab")
    rows = [(sym, name, sector, round(random.uniform(80, 4000), 2)) for sym, name, sector in STOCKS]
    await conn.executemany(
        "INSERT INTO stocks (symbol, name, sector, ltp) VALUES ($1, $2, $3, $4)", rows
    )
    print(f"seeded {len(rows)} stocks")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
