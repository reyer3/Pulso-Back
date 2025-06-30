-- 002: Create project-specific schemas
-- depends: 001-create-etl-metadata-tables.sql

CREATE SCHEMA IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e;
COMMENT ON SCHEMA raw_P3fV4dWNeMkN5RJMhV8e IS 'Raw staging data for project P3fV4dWNeMkN5RJMhV8e.';

CREATE SCHEMA IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e;
COMMENT ON SCHEMA aux_P3fV4dWNeMkN5RJMhV8e IS 'Auxiliary/intermediate transformation data for project P3fV4dWNeMkN5RJMhV8e.';

CREATE SCHEMA IF NOT EXISTS mart_P3fV4dWNeMkN5RJMhV8e;
COMMENT ON SCHEMA mart_P3fV4dWNeMkN5RJMhV8e IS 'Final, aggregated data marts for project P3fV4dWNeMkN5RJMhV8e.';
