-- 001: Create core ETL metadata and monitoring tables in the public schema
-- These tables are project-agnostic.
-- depends: 000-enable-timescaledb.sql

-- etl_watermarks table based on your provided 001-create-watermarks-table.sql
CREATE TABLE IF NOT EXISTS public.etl_watermarks (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    last_extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_extraction_status VARCHAR(20) NOT NULL DEFAULT 'success',
    records_extracted INTEGER DEFAULT 0,
    extraction_duration_seconds FLOAT DEFAULT 0.0,
    error_message TEXT,
    extraction_id VARCHAR(50),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes for etl_watermarks
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_table_name ON public.etl_watermarks(table_name);
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_status ON public.etl_watermarks(last_extraction_status);
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_updated ON public.etl_watermarks(updated_at);

-- Comments for etl_watermarks
COMMENT ON TABLE public.etl_watermarks IS 'Tracks extraction watermarks for incremental ETL processing. Not a hypertable as it is metadata/configuration data.';
COMMENT ON COLUMN public.etl_watermarks.table_name IS 'Name of the table being tracked';
COMMENT ON COLUMN public.etl_watermarks.last_extracted_at IS 'Timestamp of last successful extraction';
COMMENT ON COLUMN public.etl_watermarks.last_extraction_status IS 'Status: success, failed, running';
COMMENT ON COLUMN public.etl_watermarks.records_extracted IS 'Number of records processed in last extraction';
COMMENT ON COLUMN public.etl_watermarks.extraction_duration_seconds IS 'Duration of last extraction in seconds';
COMMENT ON COLUMN public.etl_watermarks.error_message IS 'Error message if extraction failed';
COMMENT ON COLUMN public.etl_watermarks.extraction_id IS 'Unique identifier for extraction run';
COMMENT ON COLUMN public.etl_watermarks.metadata IS 'Additional metadata as JSON';

-- etl_execution_log table based on your provided 002-create-etl-execution-log-table.sql
CREATE TABLE IF NOT EXISTS public.etl_execution_log (
    id SERIAL,
    execution_id UUID NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, started_at) -- Composite primary key including the time column for TimescaleDB
);

-- Convert etl_execution_log to hypertable
SELECT create_hypertable('public.etl_execution_log', 'started_at', if_not_exists => TRUE);

-- Indexes for etl_execution_log
CREATE INDEX IF NOT EXISTS idx_etl_execution_log_pipeline ON public.etl_execution_log (pipeline_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_execution_log_status ON public.etl_execution_log (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_execution_log_execution_id ON public.etl_execution_log (execution_id, started_at DESC);

COMMENT ON TABLE public.etl_execution_log IS 'Global log of all ETL pipeline executions, optimized with TimescaleDB.';

-- extraction_metrics table (previously etl_timeseries.extraction_metrics)
-- Moved to public schema for general monitoring.
CREATE TABLE IF NOT EXISTS public.extraction_metrics (
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

-- Convert extraction_metrics to hypertable
SELECT create_hypertable(
    'public.extraction_metrics',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day' -- Changed from 1 hour to 1 day, adjust if needed
);

-- Indexes for extraction_metrics
CREATE INDEX IF NOT EXISTS idx_extraction_metrics_table_time ON public.extraction_metrics (table_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_extraction_metrics_status_time ON public.extraction_metrics (status, time DESC);

-- Retention policy for extraction_metrics
SELECT add_retention_policy(
    'public.extraction_metrics',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

COMMENT ON TABLE public.extraction_metrics IS 'Time-series metrics for ETL extractions (e.g., record counts, duration), optimized with TimescaleDB.';
