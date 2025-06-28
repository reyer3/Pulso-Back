-- Simple watermark table for ETL tracking
-- depends:

-- Create watermarks table
CREATE TABLE IF NOT EXISTS etl_watermarks (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    last_extracted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_extraction_status VARCHAR(20) NOT NULL DEFAULT 'success',
    records_extracted INTEGER DEFAULT 0,
    extraction_duration_seconds FLOAT DEFAULT 0.0,
    error_message TEXT,
    extraction_id VARCHAR(50),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Basic indexes
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_table_name ON etl_watermarks(table_name);
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_status ON etl_watermarks(last_extraction_status);
