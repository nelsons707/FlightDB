CREATE SCHEMA IF NOT EXISTS gold_live;

-- Flight phase breakdown derived from vertical_rate:
--   >+2 m/s  → 'climbing'
--   <-2 m/s  → 'descending'
--   else      → 'cruising'
--   null      → 'unknown'
CREATE TABLE gold_live.flight_phases (
    phase           VARCHAR(20)     PRIMARY KEY,
    flight_count    INT             NOT NULL,
    updated_at      TIMESTAMPTZ     NOT NULL
);
