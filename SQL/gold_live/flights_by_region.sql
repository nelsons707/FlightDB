CREATE SCHEMA IF NOT EXISTS gold_live;

-- Flights by US census region: 'Northeast', 'South', 'Midwest', 'West'
CREATE TABLE gold_live.flights_by_region (
    region          VARCHAR(50)     PRIMARY KEY,
    flight_count    INT             NOT NULL,
    updated_at      TIMESTAMPTZ     NOT NULL
);
