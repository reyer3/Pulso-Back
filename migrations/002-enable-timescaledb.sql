-- Enable TimescaleDB extension for time-series data
-- depends: 001-create-watermarks-table

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Enable pgvector extension for vector operations (if needed)
CREATE EXTENSION IF NOT EXISTS vector;

-- Create TimescaleDB-specific schema
CREATE SCHEMA IF NOT EXISTS timeseries;

-- Convert watermarks table to hypertable for better time-series performance
-- (Optional: only if you want time-series optimization for watermarks)
-- SELECT create_hypertable('etl_watermarks', 'last_extracted_at', if_not_exists => TRUE);

-- Verify extensions are properly loaded
DO $$
BEGIN
    RAISE NOTICE 'TimescaleDB version: %', (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb');
    RAISE NOTICE 'Extensions installed successfully';
END $$;
