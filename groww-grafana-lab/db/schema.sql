DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS stocks;

CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sector TEXT NOT NULL,
    ltp NUMERIC(10,2) NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty INTEGER NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'FILLED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_stocks_symbol ON stocks (symbol);
