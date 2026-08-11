-- redBus data-tier lab schema.
-- Deliberately NO index on (source, destination, travel_date) -- this is
-- the Module 4 "missing index / slow query" defect, now against a real
-- PostgreSQL instance instead of the SQLite stand-in from Module 1.

DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS buses;

CREATE TABLE buses (
    id SERIAL PRIMARY KEY,
    operator TEXT NOT NULL,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    travel_date DATE NOT NULL,
    departure TEXT NOT NULL,
    fare INTEGER NOT NULL
);

CREATE TABLE bookings (
    id SERIAL PRIMARY KEY,
    bus_id INTEGER NOT NULL REFERENCES buses(id),
    customer_id TEXT NOT NULL,
    seat_no TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
