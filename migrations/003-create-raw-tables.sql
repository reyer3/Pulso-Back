-- 003: Create common raw staging tables for project P3fV4dWNeMkN5RJMhV8e
-- depends: 002-create-project-schemas.sql

-- Constants for project UID
-- (Note: SQL doesn't have variables like this, schema is hardcoded. This is a comment.)
-- PROJECT_UID = 'P3fV4dWNeMkN5RJMhV8e'

-- Raw Calendario (from original 011-create-raw-calendario-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.calendario (
    ARCHIVO TEXT NOT NULL,
    TIPO_CARTERA TEXT,
    fecha_apertura DATE NOT NULL,
    fecha_trandeuda DATE,
    fecha_cierre DATE,
    FECHA_CIERRE_PLANIFICADA DATE,
    DURACION_CAMPANA_DIAS_HABILES INTEGER,
    ANNO_ASIGNACION INTEGER,
    PERIODO_ASIGNACION TEXT,
    ES_CARTERA_ABIERTA BOOLEAN,
    RANGO_VENCIMIENTO TEXT,
    ESTADO_CARTERA TEXT,
    periodo_mes TEXT,
    periodo_date DATE NOT NULL, -- For TimescaleDB partitioning
    tipo_ciclo_campana TEXT,
    categoria_duracion TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ARCHIVO, periodo_date)
);
SELECT create_hypertable(
    'raw_P3fV4dWNeMkN5RJMhV8e.calendario',
    'periodo_date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_raw_calendario_fecha_apertura ON raw_P3fV4dWNeMkN5RJMhV8e.calendario(fecha_apertura, periodo_date);
CREATE INDEX IF NOT EXISTS idx_raw_calendario_tipo_cartera ON raw_P3fV4dWNeMkN5RJMhV8e.calendario(TIPO_CARTERA, periodo_date);
CREATE INDEX IF NOT EXISTS idx_raw_calendario_estado ON raw_P3fV4dWNeMkN5RJMhV8e.calendario(ESTADO_CARTERA, periodo_date);
CREATE INDEX IF NOT EXISTS idx_raw_calendario_archivo ON raw_P3fV4dWNeMkN5RJMhV8e.calendario(ARCHIVO, periodo_date); -- Adjusted to include partition key
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.calendario IS 'Raw staging table for BigQuery calendario data for project P3fV4dWNeMkN5RJMhV8e.';

-- Raw Asignaciones (from original 012-create-raw-asignaciones-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.asignaciones (
    cod_luna TEXT NOT NULL,
    cuenta TEXT NOT NULL,
    archivo TEXT NOT NULL,
    cliente TEXT,
    telefono TEXT,
    tramo_gestion TEXT,
    negocio TEXT,
    dias_sin_trafico TEXT,
    decil_contacto INTEGER,
    decil_pago INTEGER,
    min_vto DATE,
    zona TEXT,
    rango_renta INTEGER,
    campania_act TEXT,
    fraccionamiento TEXT,
    cuota_fracc_act TEXT,
    fecha_corte DATE,
    priorizado TEXT,
    inscripcion TEXT,
    incrementa_velocidad TEXT,
    detalle_dscto_futuro TEXT,
    cargo_fijo TEXT,
    dni TEXT,
    estado_pc TEXT,
    tipo_linea TEXT,
    cod_sistema INTEGER,
    tipo_alta TEXT,
    creado_el TIMESTAMPTZ,
    fecha_asignacion DATE NOT NULL, -- For TimescaleDB partitioning
    motivo_rechazo TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cod_luna, cuenta, archivo, fecha_asignacion)
);
SELECT create_hypertable(
    'raw_P3fV4dWNeMkN5RJMhV8e.asignaciones',
    'fecha_asignacion',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_cod_luna ON raw_P3fV4dWNeMkN5RJMhV8e.asignaciones(cod_luna, fecha_asignacion);
CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_cuenta ON raw_P3fV4dWNeMkN5RJMhV8e.asignaciones(cuenta, fecha_asignacion);
CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_archivo ON raw_P3fV4dWNeMkN5RJMhV8e.asignaciones(archivo, fecha_asignacion); -- Adjusted
CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_dni ON raw_P3fV4dWNeMkN5RJMhV8e.asignaciones(dni, fecha_asignacion); -- Added index on DNI as it's often used for joins
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.asignaciones IS 'Raw staging table for BigQuery asignaciones data for project P3fV4dWNeMkN5RJMhV8e.';

-- Raw Trandeuda (from original 013-create-raw-trandeuda-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.trandeuda (
    cod_cuenta TEXT NOT NULL,
    nro_documento TEXT NOT NULL,
    archivo TEXT NOT NULL,
    fecha_vencimiento DATE,
    monto_exigible DECIMAL(15,2) NOT NULL,
    creado_el TIMESTAMPTZ,
    fecha_proceso DATE NOT NULL, -- For TimescaleDB partitioning
    motivo_rechazo TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cod_cuenta, nro_documento, archivo, fecha_proceso),
    CONSTRAINT chk_raw_trandeuda_monto_positive CHECK (monto_exigible >= 0)
);
SELECT create_hypertable(
    'raw_P3fV4dWNeMkN5RJMhV8e.trandeuda',
    'fecha_proceso',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_cod_cuenta ON raw_P3fV4dWNeMkN5RJMhV8e.trandeuda(cod_cuenta, fecha_proceso);
CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_nro_documento ON raw_P3fV4dWNeMkN5RJMhV8e.trandeuda(nro_documento, fecha_proceso);
CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_archivo ON raw_P3fV4dWNeMkN5RJMhV8e.trandeuda(archivo, fecha_proceso); -- Adjusted
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.trandeuda IS 'Raw staging table for BigQuery trandeuda data for project P3fV4dWNeMkN5RJMhV8e.';

-- Raw Pagos (from original 014-create-raw-pagos-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.pagos (
    nro_documento TEXT NOT NULL,
    fecha_pago DATE NOT NULL, -- Also for TimescaleDB partitioning
    monto_cancelado DECIMAL(15,2) NOT NULL,
    cod_sistema TEXT,
    archivo TEXT NOT NULL,
    creado_el TIMESTAMPTZ,
    motivo_rechazo TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (nro_documento, fecha_pago, monto_cancelado), -- Natural key, also good for Timescale
    CONSTRAINT chk_raw_pagos_monto_positive CHECK (monto_cancelado > 0)
    --CONSTRAINT chk_raw_pagos_fecha_reasonable CHECK (fecha_pago >= '2020-01-01' AND fecha_pago <= CURRENT_DATE + INTERVAL '30 days') -- This might be too restrictive
);
SELECT create_hypertable(
    'raw_P3fV4dWNeMkN5RJMhV8e.pagos',
    'fecha_pago',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_raw_pagos_nro_documento ON raw_P3fV4dWNeMkN5RJMhV8e.pagos(nro_documento, fecha_pago);
CREATE INDEX IF NOT EXISTS idx_raw_pagos_archivo ON raw_P3fV4dWNeMkN5RJMhV8e.pagos(archivo, fecha_pago); -- Adjusted
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.pagos IS 'Raw staging table for BigQuery pagos data for project P3fV4dWNeMkN5RJMhV8e.';

-- Raw Homologacion MibotAir (from original 016-create-raw-homologacion-mibotair-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.homologacion_mibotair (
    management TEXT,
    n_1 TEXT,
    n_2 TEXT,
    n_3 TEXT,
    peso INTEGER,
    contactabilidad TEXT,
    tipo_gestion TEXT,
    codigo_rpta TEXT,
    pdp TEXT,
    gestor TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (n_1, n_2, n_3) -- Assuming these define uniqueness for homologation rules
);
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.homologacion_mibotair IS 'Raw staging for MibotAir homologation rules for project P3fV4dWNeMkN5RJMhV8e.';

-- Raw Homologacion Voicebot (from original 017-create-raw-homologacion-voicebot-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.homologacion_voicebot (
    bot_management TEXT,
    bot_sub_management TEXT,
    bot_compromiso TEXT,
    n1_homologado TEXT,
    n2_homologado TEXT,
    n3_homologado TEXT,
    contactabilidad_homologada TEXT,
    es_pdp_homologado BOOLEAN,
    peso_homologado INTEGER,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bot_management, bot_sub_management, bot_compromiso) -- Assuming these define uniqueness
);
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.homologacion_voicebot IS 'Raw staging for Voicebot homologation rules for project P3fV4dWNeMkN5RJMhV8e.';

-- Raw Ejecutivos (from original 018-create-raw-ejecutivos-table.sql)
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.ejecutivos (
    correo_name TEXT PRIMARY KEY,
    document TEXT NOT NULL,
    nombre TEXT,
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.ejecutivos IS 'Raw staging for agent/executive data for project P3fV4dWNeMkN5RJMhV8e.';

-- Note: The original 'gestiones_unificadas' (015) is NOT created here as a raw table.
-- It's being replaced by raw_...voicebot_gestiones, raw_...mibotair_gestiones,
-- and the ETL-generated aux_...gestiones_unificadas.

-- Update functions for 'updated_at' columns (example for calendario, repeat for others if needed)
CREATE OR REPLACE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to tables with updated_at; example for calendario
DROP TRIGGER IF EXISTS trigger_calendario_updated_at ON raw_P3fV4dWNeMkN5RJMhV8e.calendario;
CREATE TRIGGER trigger_calendario_updated_at
    BEFORE UPDATE ON raw_P3fV4dWNeMkN5RJMhV8e.calendario
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

DROP TRIGGER IF EXISTS trigger_asignaciones_updated_at ON raw_P3fV4dWNeMkN5RJMhV8e.asignaciones;
CREATE TRIGGER trigger_asignaciones_updated_at
    BEFORE UPDATE ON raw_P3fV4dWNeMkN5RJMhV8e.asignaciones
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

DROP TRIGGER IF EXISTS trigger_trandeuda_updated_at ON raw_P3fV4dWNeMkN5RJMhV8e.trandeuda;
CREATE TRIGGER trigger_trandeuda_updated_at
    BEFORE UPDATE ON raw_P3fV4dWNeMkN5RJMhV8e.trandeuda
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

DROP TRIGGER IF EXISTS trigger_pagos_updated_at ON raw_P3fV4dWNeMkN5RJMhV8e.pagos;
CREATE TRIGGER trigger_pagos_updated_at
    BEFORE UPDATE ON raw_P3fV4dWNeMkN5RJMhV8e.pagos
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();
