-- 006: Create all auxiliary tables for project P3fV4dWNeMkN5RJMhV8e
-- depends: 005-create-raw-mibotair-gestiones-table

-- Se asume que en una migración anterior (ej. 001) se creó esta función genérica en el esquema public.
-- Si no, descomentar para crearla aquí.
/*
CREATE OR REPLACE FUNCTION public.update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION public.update_timestamp_column() IS 'Generic trigger function to update the updated_at column to the current timestamp.';
*/

-- =============================================================================
-- Tabla: cuenta_campana_state
-- Propósito: Almacena el estado de cada cuenta dentro del contexto de una campaña. Es la "foto" inicial.
-- =============================================================================
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state (
    archivo VARCHAR(200) NOT NULL,
    cod_luna VARCHAR(50) NOT NULL,
    cuenta VARCHAR(50) NOT NULL,
    fecha_apertura DATE NOT NULL,
    fecha_cierre DATE,
    monto_inicial DECIMAL(15,2) DEFAULT 0.0,
    monto_actual DECIMAL(15,2) DEFAULT 0.0,
    fecha_ultima_actualizacion DATE,
    tiene_deuda_activa BOOLEAN DEFAULT FALSE,
    es_cuenta_gestionable BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (archivo, cod_luna, cuenta)
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state IS 'Tracks account states within campaign windows. Populated once per campaign.';

CREATE INDEX IF NOT EXISTS idx_aux_ccs_archivo ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state(archivo);
CREATE INDEX IF NOT EXISTS idx_aux_ccs_cod_luna ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state(cod_luna);
CREATE INDEX IF NOT EXISTS idx_aux_ccs_cuenta ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state(cuenta);

DROP TRIGGER IF EXISTS trg_cuenta_campana_state_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state;
CREATE TRIGGER trg_cuenta_campana_state_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp_column();

-- =============================================================================
-- Tabla: gestiones_unificadas (Copia de la migración 007 para completitud)
-- Propósito: Tabla intermedia que unifica las gestiones de Voicebot y MibotAir.
-- =============================================================================
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas (
    gestion_uid TEXT PRIMARY KEY,
    cod_luna TEXT NOT NULL,
    timestamp_gestion TIMESTAMPTZ NOT NULL,
    fecha_gestion DATE NOT NULL,
    canal_origen TEXT NOT NULL,
    contactabilidad TEXT,
    es_contacto_efectivo BOOLEAN DEFAULT FALSE,
    es_compromiso BOOLEAN DEFAULT FALSE,
    peso_gestion INTEGER DEFAULT 1,
    archivo_campana TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas IS 'Unified and cleaned gestiones data from all channels. Populated once per campaign.';

CREATE INDEX IF NOT EXISTS idx_aux_gu_fecha_gestion ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(fecha_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gu_archivo_campana ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(archivo_campana);

-- =============================================================================
-- Tabla: gestion_cuenta_impact (sin cambios, ya era robusta)
-- =============================================================================
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact (
    archivo VARCHAR(200) NOT NULL,
    cod_luna VARCHAR(50) NOT NULL,
    timestamp_gestion TIMESTAMPTZ NOT NULL,
    cuenta VARCHAR(50) NOT NULL,
    canal_origen VARCHAR(20) NOT NULL,
    contactabilidad VARCHAR(50),
    es_contacto_efectivo BOOLEAN DEFAULT FALSE,
    es_compromiso BOOLEAN DEFAULT FALSE,
    peso_gestion INTEGER DEFAULT 0,
    monto_deuda_momento DECIMAL(15,2) DEFAULT 0.0,
    es_cuenta_con_deuda BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (archivo, cod_luna, timestamp_gestion, cuenta)
);
SELECT create_hypertable('aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact', 'timestamp_gestion', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact IS 'Maps individual gestiones to the specific accounts they impacted.';

CREATE INDEX IF NOT EXISTS idx_aux_gci_archivo_fecha ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(archivo, timestamp_gestion DESC);
CREATE INDEX IF NOT EXISTS idx_aux_gci_cuenta ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(cuenta);

DROP TRIGGER IF EXISTS trg_gestion_cuenta_impact_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact;
CREATE TRIGGER trg_gestion_cuenta_impact_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp_column();

-- =============================================================================
-- Tabla: pago_deduplication (sin cambios, ya era robusta)
-- =============================================================================
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication (
    archivo_campana VARCHAR(200) NOT NULL, -- Renombrado para claridad
    cuenta VARCHAR(50) NOT NULL,
    nro_documento VARCHAR(100) NOT NULL,
    fecha_pago DATE NOT NULL,
    monto_cancelado DECIMAL(15,2) NOT NULL,
    es_pago_unico BOOLEAN DEFAULT TRUE,
    fecha_primera_carga DATE NOT NULL,
    fecha_ultima_carga DATE NOT NULL,
    veces_visto INTEGER DEFAULT 1,
    esta_en_ventana BOOLEAN DEFAULT FALSE,
    cod_luna VARCHAR(50),
    es_pago_valido BOOLEAN DEFAULT TRUE,
    motivo_rechazo TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (archivo_campana, nro_documento, fecha_pago, monto_cancelado) -- PK ajustado
);
SELECT create_hypertable('aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication', 'fecha_pago', chunk_time_interval => INTERVAL '30 days', if_not_exists => TRUE);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication IS 'Handles payment deduplication within the context of a campaign.';

CREATE INDEX IF NOT EXISTS idx_aux_pd_archivo_fecha ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication(archivo_campana, fecha_pago DESC);

DROP TRIGGER IF EXISTS trg_pago_deduplication_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication;
CREATE OR REPLACE FUNCTION public.update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    -- Asigna el timestamp actual a la columna 'updated_at' de la fila que se está modificando.
    NEW.updated_at = NOW();

    -- Devuelve la fila modificada para que la operación de UPDATE continúe.
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.update_timestamp_column() IS 'Generic trigger function to update the updated_at column to the current timestamp. To be used in BEFORE UPDATE triggers.';


CREATE TRIGGER trg_pago_deduplication_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp_column();

-- =============================================================================
-- NUEVAS TABLAS AUXILIARES DIARIAS
-- Propósito: Pre-agregar datos diarios para acelerar la construcción de los marts.
-- =============================================================================

-- Tabla: gestiones_diarias
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestiones_diarias (
    fecha_gestion DATE NOT NULL,
    archivo_campana VARCHAR(200) NOT NULL,
    cod_luna VARCHAR(50) NOT NULL,
    total_gestiones INTEGER NOT NULL DEFAULT 0,
    total_contactos_efectivos INTEGER NOT NULL DEFAULT 0,
    total_promesas INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (fecha_gestion, archivo_campana, cod_luna)
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.gestiones_diarias IS 'Daily summary of gestiones per client and campaign.';

-- Tabla: pagos_diarios
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.pagos_diarios (
    fecha_pago DATE NOT NULL,
    archivo_campana VARCHAR(200) NOT NULL,
    cod_luna VARCHAR(50) NOT NULL,
    monto_pagado DECIMAL(15,2) NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (fecha_pago, archivo_campana, cod_luna)
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.pagos_diarios IS 'Daily summary of payments per client and campaign.';