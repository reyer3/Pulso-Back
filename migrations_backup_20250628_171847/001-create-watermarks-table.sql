-- Watermark tracking table for ETL incremental processing
-- depends: 000-enable-timescaledb

CREATE TABLE etl_watermarks (
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

-- Performance indexes
CREATE INDEX idx_etl_watermarks_table_name ON etl_watermarks(table_name);
CREATE INDEX idx_etl_watermarks_status ON etl_watermarks(last_extraction_status);
CREATE INDEX idx_etl_watermarks_updated ON etl_watermarks(updated_at);

-- Comments for documentation
COMMENT ON TABLE etl_watermarks IS 'Tracks extraction watermarks for incremental ETL processing';
COMMENT ON COLUMN etl_watermarks.table_name IS 'Name of the table being tracked';
COMMENT ON COLUMN etl_watermarks.last_extracted_at IS 'Timestamp of last successful extraction';
COMMENT ON COLUMN etl_watermarks.last_extraction_status IS 'Status: success, failed, running';
COMMENT ON COLUMN etl_watermarks.records_extracted IS 'Number of records processed in last extraction';
COMMENT ON COLUMN etl_watermarks.extraction_duration_seconds IS 'Duration of last extraction in seconds';
COMMENT ON COLUMN etl_watermarks.error_message IS 'Error message if extraction failed';
COMMENT ON COLUMN etl_watermarks.extraction_id IS 'Unique identifier for extraction run';
COMMENT ON COLUMN etl_watermarks.metadata IS 'Additional metadata as JSON';
