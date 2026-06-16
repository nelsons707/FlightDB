CREATE SCHEMA IF NOT EXISTS gold_live;

-- Total flights in air right now (single row)
CREATE TABLE gold_live.summary (
    singleton       INT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    flights_in_air  INT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);
