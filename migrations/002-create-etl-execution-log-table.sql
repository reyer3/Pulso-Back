-- ETL execution log for monitoring and debugging
-- depends: 001-create-watermarks-table
-- depends: 000-enable-timescaledb

CREATE TABLE etl_execution_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    execution_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds FLOAT,
    error_message TEXT,
    stack_trace TEXT,
    extraction_mode VARCHAR(20),
    source_query TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_etl_execution_log_execution_id ON etl_execution_log(execution_id);
CREATE INDEX idx_etl_execution_log_table_name ON etl_execution_log(table_name);
CREATE INDEX idx_etl_execution_log_status ON etl_execution_log(status);
CREATE INDEX idx_etl_execution_log_started ON etl_execution_log(started_at);

-- TimescaleDB hypertable
-- This will only succeed if the TimescaleDB extension is installed.
SELECT create_hypertable('etl_execution_log', by_range('started_at', INTERVAL '1 month'), if_not_exists => TRUE);

-- Retention policy
SELECT add_retention_policy('etl_execution_log', INTERVAL '6 months', if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE etl_execution_log IS 'Detailed ETL execution log for monitoring and debugging';
COMMENT ON COLUMN etl_execution_log.execution_id IS 'Unique execution identifier';
COMMENT ON COLUMN etl_execution_log.table_name IS 'Target table name';
COMMENT ON COLUMN etl_execution_log.execution_type IS 'Type of execution (e.g., incremental, full_refresh)';
COMMENT ON COLUMN etl_execution_log.status IS 'Execution status (e.g., running, success, failed)';
COMMENT ON COLUMN etl_execution_log.records_processed IS 'Total records processed during execution';
COMMENT ON COLUMN etl_execution_log.records_inserted IS 'Number of new records inserted';
COMMENT ON COLUMN etl_execution_log.records_updated IS 'Number of existing records updated';
COMMENT ON COLUMN etl_execution_log.records_skipped IS 'Number of records skipped';
COMMENT ON COLUMN etl_execution_log.started_at IS 'Timestamp when the execution started';
COMMENT ON COLUMN etl_execution_log.completed_at IS 'Timestamp when the execution completed';
COMMENT ON COLUMN etl_execution_log.duration_seconds IS 'Total duration of the execution in seconds';
COMMENT ON COLUMN etl_execution_log.error_message IS 'Error message if the execution failed';
COMMENT ON COLUMN etl_execution_log.stack_trace IS 'Full stack trace if an error occurred';
COMMENT ON COLUMN etl_execution_log.extraction_mode IS 'Extraction mode used (e.g., snapshot, incremental)';
COMMENT ON COLUMN etl_execution_log.source_query IS 'The exact SQL query executed on the source database';
COMMENT ON COLUMN etl_execution_log.metadata IS 'Additional metadata related to the execution (JSONB format)';
