-- Enable TimescaleDB extension for time-series capabilities
-- depends: 001-create-watermarks-table.sql

-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Convert etl_watermarks to hypertable for better time-series performance
-- This will improve performance for watermark queries over time
SELECT create_hypertable(
    'etl_watermarks', 
    'last_extracted_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- Add retention policy: keep watermarks for 6 months
SELECT add_retention_policy(
    'etl_watermarks', 
    INTERVAL '6 months',
    if_not_exists => TRUE
);

-- Add compression policy for older data (compress data older than 1 week)
SELECT add_compression_policy(
    'etl_watermarks', 
    INTERVAL '1 week',
    if_not_exists => TRUE
);

-- Create additional time-based index for better performance
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_time_status 
ON etl_watermarks (last_extracted_at DESC, last_extraction_status);
