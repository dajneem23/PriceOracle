-- 1. Metadata Tables (Standard Postgres)
CREATE TABLE sources (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE, -- e.g., 'Bloomberg', 'Reuters', 'BankOfVietnam'
    priority SMALLINT DEFAULT 0 -- Useful if you later want to calculate a 'weighted average' rate
);

CREATE TABLE currency_pairs (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE, -- e.g., 'USDVND'
    base_currency TEXT NOT NULL, -- 'USD'
    quote_currency TEXT NOT NULL -- 'VND'
);

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- 2. The Hypertable (TimescaleDB)
CREATE TABLE fx_ticks (
    time TIMESTAMPTZ NOT NULL,
    pair_id SMALLINT NOT NULL,   -- Foreign key to currency_pairs
    source_id SMALLINT NOT NULL, -- Foreign key to sources
    bid NUMERIC(18, 8) NOT NULL, -- Adjust precision based on your needs
    mid NUMERIC(18, 8) NOT NULL,
    ask NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(18, 8) DEFAULT NULL, -- Optional: volume if available

    
    -- Constraint to ensure we don't duplicate data for the same source/time
    CONSTRAINT fx_ticks_pk PRIMARY KEY (time, pair_id, source_id)
);

-- 3. Convert to Hypertable
-- Partitioning by time is automatic. 
SELECT create_hypertable('fx_ticks', 'time');



-- Enable compression
ALTER TABLE fx_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pair_id, source_id', -- Group by these for columnar compression
    timescaledb.compress_orderby = 'time DESC'
);

-- Policy: Compress chunks older than 7 days
SELECT add_compression_policy('fx_ticks', INTERVAL '7 days');




CREATE MATERIALIZED VIEW fx_candles_1m_by_source
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    pair_id,
    source_id,
    FIRST(bid, time) as open_bid,
    MAX(bid) as high_bid,
    MIN(bid) as low_bid,
    FIRST(mid, time) as open_mid,
    MAX(mid) as high_mid,
    MIN(mid) as low_mid,
    LAST(mid, time) as close_mid,
    AVG(mid) as avg_mid,
    LAST(bid, time) as close_bid,
    AVG(bid) as avg_bid
FROM fx_ticks
GROUP BY bucket, pair_id, source_id;