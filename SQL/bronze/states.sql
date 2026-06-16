CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE bronze.states (
    icao24          VARCHAR(10)         NOT NULL,
    callsign        VARCHAR(20),
    origin_country  VARCHAR(100),
    time_position   BIGINT,                         -- Unix epoch; null when aircraft not transmitting position
    last_contact    BIGINT,                         -- Unix epoch of last ADS-B contact
    longitude       DOUBLE PRECISION,               -- degrees
    latitude        DOUBLE PRECISION,               -- degrees
    baro_altitude   DOUBLE PRECISION,               -- meters (barometric)
    on_ground       BOOLEAN,
    velocity        DOUBLE PRECISION,               -- m/s ground speed
    true_track      DOUBLE PRECISION,               -- degrees clockwise from north
    vertical_rate   DOUBLE PRECISION,               -- m/s; positive = climbing
    sensors         INTEGER[],                      -- contributing receiver IDs (null for most users)
    geo_altitude    DOUBLE PRECISION,               -- meters (GPS)
    squawk          VARCHAR(4),                     -- Mode C squawk (octal string)
    spi             BOOLEAN,                        -- special purpose indicator
    position_source SMALLINT,                       -- 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
    poll_time       BIGINT          NOT NULL,       -- Unix epoch of the API poll (from states_response["time"])
    poll_date       DATE            NOT NULL,       -- derived from poll_time by Spark; partition key
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (poll_date);

-- Monthly partitions — add new ones before the month begins
CREATE TABLE bronze.states_2026_06 PARTITION OF bronze.states
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE bronze.states_2026_07 PARTITION OF bronze.states
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE bronze.states_2026_08 PARTITION OF bronze.states
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE bronze.states_2026_09 PARTITION OF bronze.states
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE bronze.states_2026_10 PARTITION OF bronze.states
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE bronze.states_2026_11 PARTITION OF bronze.states
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

-- Catch-all for anything outside the defined ranges
CREATE TABLE bronze.states_default PARTITION OF bronze.states DEFAULT;

-- Indexes (Postgres propagates these to each partition automatically)
CREATE INDEX ON bronze.states (icao24, poll_time);
CREATE INDEX ON bronze.states (poll_time);
CREATE INDEX ON bronze.states (squawk) WHERE squawk IN ('7700', '7600', '7500');
