-- 000: Enable TimescaleDB extension
-- This should be one of the very first migrations to run.

-- Create TimescaleDB extension if available
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Comment
COMMENT ON EXTENSION timescaledb IS 'TimescaleDB extension for time-series data optimization';
