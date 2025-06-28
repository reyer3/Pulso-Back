-- 000-enable-timescaledb.sql
-- This migration ensures the TimescaleDB extension is enabled in the database.
-- It must run before any other migration that uses TimescaleDB functions.
-- depends:
SELECT version();
CREATE EXTENSION IF NOT EXISTS timescaledb;
