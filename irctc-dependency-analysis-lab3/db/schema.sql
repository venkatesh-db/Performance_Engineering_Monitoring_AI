-- IRCTC dependency-analysis lab schema.
--
-- availability has a proper index on (train_no, travel_date) -- the
-- column pair every real query filters on -- plus a DELIBERATELY
-- low-selectivity index on is_active (~95% of rows are true) to
-- demonstrate "the performance cost of unnecessary indexes": the
-- planner won't use it for selective queries, but every INSERT/UPDATE
-- still pays to maintain it.

DROP TABLE IF EXISTS availability;

CREATE TABLE availability (
    id SERIAL PRIMARY KEY,
    train_no TEXT NOT NULL,
    travel_date DATE NOT NULL,
    seats_available INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_availability_train_date ON availability (train_no, travel_date);
