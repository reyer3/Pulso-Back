-- Convert appropriate ETL tables to TimescaleDB hypertables for better time-series performance
-- depends: 002-enable-timescaledb

-- NOTE: etl_watermarks is NOT converted to hypertable because:
-- 1. It's a small metadata table (one record per ETL table)
-- 2. Primary key (id) doesn't include partitioning column (last_extracted_at)
-- 3. Not suitable for time-series partitioning (configuration data, not time-series data)

-- Create schema for ETL time-series data
CREATE SCHEMA IF NOT EXISTS etl_timeseries;

-- Create ETL metrics table as hypertable for monitoring time-series data
CREATE TABLE IF NOT EXISTS etl_timeseries.extraction_metrics (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    table_name TEXT NOT NULL,
    records_processed INTEGER,
    duration_seconds FLOAT,
    memory_used_mb FLOAT,
    cpu_percent FLOAT,
    extraction_id TEXT,
    status TEXT,
    metadata JSONB
);

-- Convert extraction_metrics to hypertable (this is suitable for time-series)
SELECT create_hypertable(
    'etl_timeseries.extraction_metrics', 
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 hour'
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_extraction_metrics_table_time 
    ON etl_timeseries.extraction_metrics (table_name, time DESC);

CREATE INDEX IF NOT EXISTS idx_extraction_metrics_status_time 
    ON etl_timeseries.extraction_metrics (status, time DESC);

-- Add data retention policy (keep data for 30 days)
SELECT add_retention_policy(
    'etl_timeseries.extraction_metrics', 
    INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Verify hypertables were created
SELECT 
    schemaname, 
    tablename, 
    num_dimensions, 
    time_column_name,
    time_interval
FROM timescaledb_information.hypertables;

-- Add comment explaining why etl_watermarks is not a hypertable
COMMENT ON TABLE etl_watermarks IS 'ETL watermark tracking table - NOT a hypertable because it is metadata/configuration data, not time-series data';
