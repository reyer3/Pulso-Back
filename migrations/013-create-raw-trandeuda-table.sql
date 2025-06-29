-- ========================================
-- 013: Create raw_trandeuda table for BigQuery extraction staging
-- TimescaleDB optimized version  
-- ========================================

-- Raw staging table to store trandeuda (debt transactions) data from BigQuery before transformation
CREATE TABLE IF NOT EXISTS raw_trandeuda (
    -- Account identification
    cod_cuenta TEXT NOT NULL,
    nro_documento TEXT NOT NULL,
    archivo TEXT NOT NULL,
    
    -- Debt information
    fecha_vencimiento DATE,
    monto_exigible DECIMAL(15,2) NOT NULL,
    
    -- Technical metadata
    creado_el TIMESTAMPTZ,
    fecha_proceso DATE NOT NULL,  -- Derived from creado_el (for hypertable)
    motivo_rechazo TEXT,
    
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key constraint
    PRIMARY KEY (cod_cuenta, nro_documento, archivo),
    
    -- Business constraint
    CONSTRAINT chk_raw_trandeuda_monto_positive CHECK (monto_exigible >= 0)
);

-- ========================================
-- Convert to TimescaleDB hypertable for time-series optimization
-- ========================================
SELECT create_hypertable(
    'raw_trandeuda', 
    'fecha_proceso',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ========================================
-- Performance indexes for common queries (TimescaleDB optimized)
-- ========================================
CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_cod_cuenta 
    ON raw_trandeuda(cod_cuenta, fecha_proceso);

CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_nro_documento 
    ON raw_trandeuda(nro_documento, fecha_proceso);

CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_archivo 
    ON raw_trandeuda(archivo) INCLUDE (fecha_proceso);

CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_fecha_vencimiento 
    ON raw_trandeuda(fecha_vencimiento, fecha_proceso);

CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_monto_exigible 
    ON raw_trandeuda(monto_exigible, fecha_proceso);

CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_creado_el 
    ON raw_trandeuda(creado_el, fecha_proceso);

-- Composite indexes for common business queries
CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_cuenta_fecha 
    ON raw_trandeuda(cod_cuenta, fecha_proceso, monto_exigible);

CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_archivo_fecha 
    ON raw_trandeuda(archivo, fecha_proceso, cod_cuenta);

-- Index for debt aging analysis
CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_debt_aging
    ON raw_trandeuda(cod_cuenta, fecha_vencimiento, fecha_proceso) 
    INCLUDE (monto_exigible);

-- ========================================
-- Update trigger for updated_at
-- ========================================
CREATE OR REPLACE FUNCTION update_raw_trandeuda_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_raw_trandeuda_updated_at ON raw_trandeuda;
CREATE TRIGGER trigger_raw_trandeuda_updated_at
    BEFORE UPDATE ON raw_trandeuda
    FOR EACH ROW
    EXECUTE FUNCTION update_raw_trandeuda_updated_at();

-- ========================================
-- Comments for documentation
-- ========================================
COMMENT ON TABLE raw_trandeuda IS 'Raw staging table for BigQuery trandeuda (debt snapshots) data with TimescaleDB optimization';
COMMENT ON COLUMN raw_trandeuda.fecha_proceso IS 'Time dimension for hypertable partitioning - derived from archivo date';
COMMENT ON COLUMN raw_trandeuda.cod_cuenta IS 'Account identifier - links to asignaciones.cuenta';
COMMENT ON COLUMN raw_trandeuda.monto_exigible IS 'Outstanding debt amount at fecha_proceso';
COMMENT ON COLUMN raw_trandeuda.archivo IS 'Source file identifier with embedded date';
