-- 
-- Create simple watermarks table for incremental ETL
-- Migration: 016-create-simple-watermarks-table.sql
-- Description: Creates etl_watermarks_simple table for tracking last extraction dates
--

-- Drop existing complex watermarks table if it exists
DROP TABLE IF EXISTS etl_watermarks CASCADE;

-- Create simplified watermarks table
CREATE TABLE IF NOT EXISTS etl_watermarks_simple (
    table_name VARCHAR(100) PRIMARY KEY,
    last_extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_etl_watermarks_simple_updated 
    ON etl_watermarks_simple(updated_at);

-- Add comments for documentation
COMMENT ON TABLE etl_watermarks_simple IS 'Simple watermarks for incremental ETL - tracks last extraction timestamp per table';
COMMENT ON COLUMN etl_watermarks_simple.table_name IS 'Name of the table being tracked';
COMMENT ON COLUMN etl_watermarks_simple.last_extracted_at IS 'Timestamp of last successful extraction';
COMMENT ON COLUMN etl_watermarks_simple.updated_at IS 'When this watermark was last updated';

-- Insert initial watermarks for main tables (optional, can be done by ETL)
INSERT INTO etl_watermarks_simple (table_name, last_extracted_at) VALUES
    ('calendario', '2025-01-01 00:00:00+00'),
    ('asignaciones', '2025-06-01 00:00:00+00'),
    ('trandeuda', '2025-06-01 00:00:00+00'),
    ('pagos', '2025-06-01 00:00:00+00'),
    ('voicebot_gestiones', '2025-06-01 00:00:00+00'),
    ('mibotair_gestiones', '2025-06-01 00:00:00+00')
ON CONFLICT (table_name) DO NOTHING;
