CREATE SCHEMA IF NOT EXISTS gold_live;

-- Active emergency squawks
-- Written via UPSERT (not TRUNCATE) so a single missed poll doesn't clear an active emergency.
-- Dashboard should filter: last_seen_at > NOW() - INTERVAL '10 minutes'
CREATE TABLE gold_live.emergency_squawks (
    icao24          VARCHAR(10)     PRIMARY KEY,
    callsign        VARCHAR(20),
    squawk          VARCHAR(4)      NOT NULL,
    emergency_type  VARCHAR(30)     NOT NULL,   -- 'GENERAL_EMERGENCY', 'RADIO_FAILURE', 'HIJACK'
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    last_seen_at    TIMESTAMPTZ     NOT NULL,
    updated_at      TIMESTAMPTZ     NOT NULL
);
