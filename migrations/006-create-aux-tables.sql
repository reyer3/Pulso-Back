-- 006: Create auxiliary tables for project P3fV4dWNeMkN5RJMhV8e
-- depends: 005-create-raw-mibotair-gestiones-table
-- (or 002-create-project-schemas if raw tables are not direct dependencies for aux structure)

-- Function for updated_at triggers within the aux schema (if not already created in raw schema's function)
-- It's better to have schema-qualified function names if they are specific or use public if generic.
-- Re-using the one from raw schema for now: raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column()

-- Aux Cuenta Campana State (from original 008-create-cuenta-campana-state-table)
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (archivo, cod_luna, cuenta)
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state IS 'Tracks account states within campaign windows for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_aux_ccs_archivo ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state(archivo);
CREATE INDEX IF NOT EXISTS idx_aux_ccs_cod_luna ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state(cod_luna);
CREATE INDEX IF NOT EXISTS idx_aux_ccs_cuenta ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state(cuenta);
DROP TRIGGER IF EXISTS trigger_ccs_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state;
CREATE TRIGGER trigger_ccs_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.cuenta_campana_state
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Aux Gestion Cuenta Impact (from original 009-create-gestion-cuenta-impact-table)
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact (
    archivo VARCHAR(200) NOT NULL,
    cod_luna VARCHAR(50) NOT NULL,
    timestamp_gestion TIMESTAMP WITH TIME ZONE NOT NULL, -- For TimescaleDB partitioning
    cuenta VARCHAR(50) NOT NULL,
    canal_origen VARCHAR(20) NOT NULL,
    contactabilidad VARCHAR(50),
    es_contacto_efectivo BOOLEAN DEFAULT FALSE,
    es_compromiso BOOLEAN DEFAULT FALSE,
    peso_gestion INTEGER DEFAULT 0,
    monto_deuda_momento DECIMAL(15,2) DEFAULT 0.0,
    es_cuenta_con_deuda BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (archivo, cod_luna, timestamp_gestion, cuenta)
);
SELECT create_hypertable(
    'aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact',
    'timestamp_gestion',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact IS 'Maps gestiones to impacted accounts for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_aux_gci_archivo ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(archivo, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gci_cod_luna ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(cod_luna, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gci_cuenta ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(cuenta, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gci_contacto_efectivo ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(es_contacto_efectivo, timestamp_gestion) WHERE es_contacto_efectivo = TRUE;
CREATE INDEX IF NOT EXISTS idx_aux_gci_compromiso ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact(es_compromiso, timestamp_gestion) WHERE es_compromiso = TRUE;
DROP TRIGGER IF EXISTS trigger_gci_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact;
CREATE TRIGGER trigger_gci_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.gestion_cuenta_impact
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Aux Pago Deduplication (from original 010-create-pago-deduplication-table)
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication (
    archivo VARCHAR(200) NOT NULL,
    cuenta VARCHAR(50) NOT NULL,
    nro_documento VARCHAR(100) NOT NULL,
    fecha_pago DATE NOT NULL, -- For TimescaleDB partitioning
    monto_cancelado DECIMAL(15,2) NOT NULL,
    es_pago_unico BOOLEAN DEFAULT TRUE,
    fecha_primera_carga DATE NOT NULL,
    fecha_ultima_carga DATE NOT NULL,
    veces_visto INTEGER DEFAULT 1,
    esta_en_ventana BOOLEAN DEFAULT FALSE,
    cod_luna VARCHAR(50),
    es_pago_valido BOOLEAN DEFAULT TRUE,
    motivo_rechazo TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (archivo, nro_documento, fecha_pago, monto_cancelado)
);
SELECT create_hypertable(
    'aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication',
    'fecha_pago',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);
COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication IS 'Deduplicates payments and tracks attribution for project P3fV4dWNeMkN5RJMhV8e.';
CREATE INDEX IF NOT EXISTS idx_aux_pd_archivo ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication(archivo, fecha_pago);
CREATE INDEX IF NOT EXISTS idx_aux_pd_cuenta ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication(cuenta, fecha_pago);
CREATE INDEX IF NOT EXISTS idx_aux_pd_cod_luna ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication(cod_luna, fecha_pago);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aux_pd_business_key ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication(nro_documento, fecha_pago, monto_cancelado) WHERE es_pago_unico = TRUE;
DROP TRIGGER IF EXISTS trigger_pd_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication;
CREATE TRIGGER trigger_pd_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.pago_deduplication
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column();

-- Placeholder for other aux tables if their DDLs become available
-- CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestiones_diarias (...);
-- CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.pagos_diarios (...);
-- CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.deuda_diaria (...);
