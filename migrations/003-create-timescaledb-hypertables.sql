-- Convert ETL tables to TimescaleDB hypertables for better time-series performance
-- depends: 002-enable-timescaledb

-- ✅ FIXED: Create hypertable for watermarks (time-based partitioning) with data migration
SELECT create_hypertable(
    'etl_watermarks', 
    'last_extracted_at',
    if_not_exists => TRUE,
    migrate_data => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- Create schema for ETL time-series data
CREATE SCHEMA IF NOT EXISTS etl_timeseries;

-- Example: Create ETL metrics table as hypertable for monitoring
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

-- Convert to hypertable
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
