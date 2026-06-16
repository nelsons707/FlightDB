CREATE SCHEMA IF NOT EXISTS gold_live;

-- Top N fastest aircraft right now (N configured in Spark job)
-- Full replace each poll via TRUNCATE + INSERT
CREATE TABLE gold_live.fastest_aircraft (
    rank                INT             PRIMARY KEY,
    icao24              VARCHAR(10)     NOT NULL,
    callsign            VARCHAR(20),
    velocity_ms         DOUBLE PRECISION NOT NULL,
    is_likely_military  BOOLEAN         NOT NULL DEFAULT FALSE,  -- velocity > 290 m/s
    updated_at          TIMESTAMPTZ     NOT NULL
);
