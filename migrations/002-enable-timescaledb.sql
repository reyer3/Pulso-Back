-- Enable TimescaleDB extension
-- depends: 001-create-watermarks-table

-- Create TimescaleDB extension if available
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Verify TimescaleDB is working
SELECT * FROM timescaledb_information.license;

-- Comment
COMMENT ON EXTENSION timescaledb IS 'TimescaleDB extension for time-series data optimization';
