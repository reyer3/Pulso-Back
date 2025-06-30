-- 018: Create raw_ejecutivos table
-- Stores agent/executive information, linking email to a document number.
CREATE TABLE IF NOT EXISTS raw_ejecutivos (
    correo_name TEXT PRIMARY KEY,
    document TEXT NOT NULL,
    nombre TEXT,
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw_ejecutivos IS 'Staging table for agent/executive data, mapping email to document ID.';
