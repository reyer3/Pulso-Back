-- Create ETL execution log table
-- file: 002-create-etl-execution-log-table.sql
-- __depends__ = ["000-enable-timescaledb"]

CREATE TABLE IF NOT EXISTS etl_execution_log (
    id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('etl_execution_log', 'started_at', if_not_exists => TRUE);

-- Create useful indexes
CREATE INDEX IF NOT EXISTS idx_etl_execution_log_pipeline 
ON etl_execution_log (pipeline_name);

CREATE INDEX IF NOT EXISTS idx_etl_execution_log_status 
ON etl_execution_log (status);

CREATE INDEX IF NOT EXISTS idx_etl_execution_log_execution_id 
ON etl_execution_log (execution_id);
