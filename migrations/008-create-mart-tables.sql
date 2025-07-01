-- 008: Create final mart tables for project P3fV4dWNeMkN5RJMhV8e
-- depends: 007-create-aux-gestiones-unificadas-table
-- (Mart tables depend on the structure of aux and raw tables for their data sources)

-- Re-using the update_timestamp_column function, assuming it's accessible (e.g., created in raw_P3fV4dWNeMkN5RJMhV8e schema or public)
-- If it needs to be schema-specific for mart:
-- CREATE OR REPLACE FUNCTION mart_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column() ...

-- Mart Dashboard Data (from original 003-create-dashboard-data-table)
CREATE TABLE IF NOT EXISTS mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data (
    fecha_foto DATE NOT NULL, -- For TimescaleDB partitioning
    archivo VARCHAR(100) NOT NULL,
    cartera VARCHAR(50) NOT NULL,
    servicio VARCHAR(20) NOT NULL,
    cuentas INTEGER NOT NULL DEFAULT 0,
    clientes INTEGER NOT NULL DEFAULT 0,
    deuda_asig FLOAT NOT NULL DEFAULT 0.0,
    deuda_act FLOAT NOT NULL DEFAULT 0.0,
    cuentas_gestionadas INTEGER NOT NULL DEFAULT 0,
    cuentas_cd INTEGER NOT NULL DEFAULT 0,
    cuentas_ci INTEGER NOT NULL DEFAULT 0,
    cuentas_sc INTEGER NOT NULL DEFAULT 0,
    cuentas_sg INTEGER NOT NULL DEFAULT 0,
    cuentas_pdp INTEGER NOT NULL DEFAULT 0,
    recupero FLOAT NOT NULL DEFAULT 0.0,
    pct_cober FLOAT NOT NULL DEFAULT 0.0,
    pct_contac FLOAT NOT NULL DEFAULT 0.0,
    pct_cd FLOAT NOT NULL DEFAULT 0.0,
    pct_ci FLOAT NOT NULL DEFAULT 0.0,
    pct_conversion FLOAT NOT NULL DEFAULT 0.0,
    pct_efectividad FLOAT NOT NULL DEFAULT 0.0,
    pct_cierre FLOAT NOT NULL DEFAULT 0.0,
    inten FLOAT NOT NULL DEFAULT 0.0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, archivo, cartera, servicio)
);
SELECT create_hypertable('mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data', 'fecha_foto', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data', INTERVAL '2 years', if_not_exists => TRUE);
COMMENT ON TABLE mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data IS 'Main dashboard metrics for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_mart_dd_fecha_foto ON mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data(fecha_foto DESC);
CREATE INDEX IF NOT EXISTS idx_mart_dd_cartera ON mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data(cartera, fecha_foto DESC);
CREATE INDEX IF NOT EXISTS idx_mart_dd_servicio ON mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data(servicio, fecha_foto DESC);
DROP TRIGGER IF EXISTS trigger_mart_dd_updated_at ON mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data;
CREATE TRIGGER trigger_mart_dd_updated_at
    BEFORE UPDATE ON mart_P3fV4dWNeMkN5RJMhV8e.dashboard_data
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Mart Evolution Data (from original 004-create-evolution-data-table)
CREATE TABLE IF NOT EXISTS mart_P3fV4dWNeMkN5RJMhV8e.evolution_data (
    fecha_foto DATE NOT NULL, -- For TimescaleDB partitioning
    archivo VARCHAR(100) NOT NULL, -- Assuming archivo helps define uniqueness with fecha_foto
    cartera VARCHAR(50) NOT NULL,
    servicio VARCHAR(20) NOT NULL,
    pct_cober FLOAT NOT NULL DEFAULT 0.0,
    pct_contac FLOAT NOT NULL DEFAULT 0.0,
    pct_efectividad FLOAT NOT NULL DEFAULT 0.0,
    pct_cierre FLOAT NOT NULL DEFAULT 0.0,
    recupero FLOAT NOT NULL DEFAULT 0.0,
    cuentas INTEGER NOT NULL DEFAULT 0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, archivo, cartera, servicio) -- Adjusted PK to be more specific
);
SELECT create_hypertable('mart_P3fV4dWNeMkN5RJMhV8e.evolution_data', 'fecha_foto', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('mart_P3fV4dWNeMkN5RJMhV8e.evolution_data', INTERVAL '2 years', if_not_exists => TRUE);
COMMENT ON TABLE mart_P3fV4dWNeMkN5RJMhV8e.evolution_data IS 'Time series data for evolution analysis for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_mart_ed_fecha_foto ON mart_P3fV4dWNeMkN5RJMhV8e.evolution_data(fecha_foto DESC);
CREATE INDEX IF NOT EXISTS idx_mart_ed_cartera ON mart_P3fV4dWNeMkN5RJMhV8e.evolution_data(cartera, fecha_foto DESC);
DROP TRIGGER IF EXISTS trigger_mart_ed_updated_at ON mart_P3fV4dWNeMkN5RJMhV8e.evolution_data;
CREATE TRIGGER trigger_mart_ed_updated_at
    BEFORE UPDATE ON mart_P3fV4dWNeMkN5RJMhV8e.evolution_data
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Mart Assignment Data (from original 005-create-assignment-data-table)
-- This table does not seem to be time-series in the same way, uses 'periodo' (YYYY-MM). Not making it a hypertable.

CREATE TABLE IF NOT EXISTS mart_P3fV4dWNeMkN5RJMhV8e.assignment_data (
    periodo VARCHAR(7) NOT NULL,
    archivo VARCHAR(100) NOT NULL,
    cartera VARCHAR(50) NOT NULL,
    servicio varchar(20) NOT NULL CHECK ( servicio in('FIJA', 'MOVIL') ),
    fecha_vencimiento DATE NOT NULL,                         -- Vencimiento de la deuda
    clientes INTEGER NOT NULL DEFAULT 0,
    cuentas INTEGER NOT NULL DEFAULT 0,
    deuda_asig FLOAT NOT NULL DEFAULT 0.0,
    deuda_actual FLOAT NOT NULL DEFAULT 0.0,
    ticket_promedio FLOAT NOT NULL DEFAULT 0.0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (periodo, archivo, cartera,servicio,fecha_vencimiento)
);
COMMENT ON TABLE mart_P3fV4dWNeMkN5RJMhV8e.assignment_data IS 'Assignment composition data by period and portfolio for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_mart_ad_periodo ON mart_P3fV4dWNeMkN5RJMhV8e.assignment_data(periodo);
CREATE INDEX IF NOT EXISTS idx_mart_ad_cartera ON mart_P3fV4dWNeMkN5RJMhV8e.assignment_data(cartera);
DROP TRIGGER IF EXISTS trigger_mart_ad_updated_at ON mart_P3fV4dWNeMkN5RJMhV8e.assignment_data;
CREATE TRIGGER trigger_mart_ad_updated_at
    BEFORE UPDATE ON mart_P3fV4dWNeMkN5RJMhV8e.assignment_data
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Mart Operation Data (from original 006-create-operation-data-table)
CREATE TABLE IF NOT EXISTS mart_P3fV4dWNeMkN5RJMhV8e.operation_data (
    fecha_foto DATE NOT NULL, -- For TimescaleDB partitioning
    hora INTEGER NOT NULL,
    canal VARCHAR(20) NOT NULL,
    archivo VARCHAR(100) NOT NULL DEFAULT 'GENERAL',
    total_gestiones INTEGER NOT NULL DEFAULT 0,
    contactos_efectivos INTEGER NOT NULL DEFAULT 0,
    contactos_no_efectivos INTEGER NOT NULL DEFAULT 0,
    total_pdp INTEGER NOT NULL DEFAULT 0,
    tasa_contacto FLOAT NOT NULL DEFAULT 0.0,
    tasa_conversion FLOAT NOT NULL DEFAULT 0.0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, hora, canal, archivo),
    CONSTRAINT chk_mart_od_hora_valid CHECK (hora >= 0 AND hora <= 23)
);
SELECT create_hypertable('mart_P3fV4dWNeMkN5RJMhV8e.operation_data', 'fecha_foto', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('mart_P3fV4dWNeMkN5RJMhV8e.operation_data', INTERVAL '1 year', if_not_exists => TRUE);
COMMENT ON TABLE mart_P3fV4dWNeMkN5RJMhV8e.operation_data IS 'Hourly operational metrics by channel for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_mart_opd_fecha_foto ON mart_P3fV4dWNeMkN5RJMhV8e.operation_data(fecha_foto DESC);
CREATE INDEX IF NOT EXISTS idx_mart_opd_canal ON mart_P3fV4dWNeMkN5RJMhV8e.operation_data(canal, fecha_foto DESC);
DROP TRIGGER IF EXISTS trigger_mart_opd_updated_at ON mart_P3fV4dWNeMkN5RJMhV8e.operation_data;
CREATE TRIGGER trigger_mart_opd_updated_at
    BEFORE UPDATE ON mart_P3fV4dWNeMkN5RJMhV8e.operation_data
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Mart Productivity Data (from original 007-create-productivity-data-table)
CREATE TABLE IF NOT EXISTS mart_P3fV4dWNeMkN5RJMhV8e.productivity_data (
    fecha_foto DATE NOT NULL, -- For TimescaleDB partitioning
    correo_agente VARCHAR(100) NOT NULL,
    archivo VARCHAR(100) NOT NULL DEFAULT 'GENERAL',
    total_gestiones INTEGER NOT NULL DEFAULT 0,
    contactos_efectivos INTEGER NOT NULL DEFAULT 0,
    total_pdp INTEGER NOT NULL DEFAULT 0,
    peso_total FLOAT NOT NULL DEFAULT 0.0,
    tasa_contacto FLOAT NOT NULL DEFAULT 0.0,
    tasa_conversion FLOAT NOT NULL DEFAULT 0.0,
    score_productividad FLOAT NOT NULL DEFAULT 0.0,
    nombre_agente VARCHAR(100),
    dni_agente VARCHAR(20),
    equipo VARCHAR(50),
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, correo_agente, archivo)
);
SELECT create_hypertable('mart_P3fV4dWNeMkN5RJMhV8e.productivity_data', 'fecha_foto', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('mart_P3fV4dWNeMkN5RJMhV8e.productivity_data', INTERVAL '2 years', if_not_exists => TRUE);
COMMENT ON TABLE mart_P3fV4dWNeMkN5RJMhV8e.productivity_data IS 'Daily productivity metrics by agent for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_mart_prd_fecha_foto ON mart_P3fV4dWNeMkN5RJMhV8e.productivity_data(fecha_foto DESC);
CREATE INDEX IF NOT EXISTS idx_mart_prd_agente ON mart_P3fV4dWNeMkN5RJMhV8e.productivity_data(correo_agente, fecha_foto DESC);
CREATE INDEX IF NOT EXISTS idx_mart_prd_equipo ON mart_P3fV4dWNeMkN5RJMhV8e.productivity_data(equipo, fecha_foto DESC);
DROP TRIGGER IF EXISTS trigger_mart_prd_updated_at ON mart_P3fV4dWNeMkN5RJMhV8e.productivity_data;
CREATE TRIGGER trigger_mart_prd_updated_at
    BEFORE UPDATE ON mart_P3fV4dWNeMkN5RJMhV8e.productivity_data
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();
