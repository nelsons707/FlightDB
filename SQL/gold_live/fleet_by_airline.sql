CREATE SCHEMA IF NOT EXISTS gold_live;

-- Fleet breakdown by airline (callsign prefix → carrier)
-- e.g. AAL=American, DAL=Delta, UAL=United, SWA=Southwest, N-prefix=General Aviation
CREATE TABLE gold_live.fleet_by_airline (
    carrier         VARCHAR(50)     PRIMARY KEY,
    flight_count    INT             NOT NULL,
    updated_at      TIMESTAMPTZ     NOT NULL
);
