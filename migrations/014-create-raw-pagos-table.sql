-- ========================================
-- 014: Create raw_pagos table for BigQuery extraction staging
-- TimescaleDB optimized version
-- ========================================

-- Raw staging table to store pagos (payments) data from BigQuery before transformation
CREATE TABLE IF NOT EXISTS raw_pagos (
    -- Payment identification
    nro_documento TEXT NOT NULL,
    fecha_pago DATE NOT NULL,
    monto_cancelado DECIMAL(15,2) NOT NULL,
    
    -- System identification  
    cod_sistema TEXT,
    archivo TEXT NOT NULL,
    
    -- Technical metadata
    creado_el TIMESTAMPTZ,
    motivo_rechazo TEXT,
    
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key constraint (for automatic deduplication)
    PRIMARY KEY (nro_documento, fecha_pago, monto_cancelado),
    
    -- Business constraints
    CONSTRAINT chk_raw_pagos_monto_positive CHECK (monto_cancelado > 0),
    CONSTRAINT chk_raw_pagos_fecha_reasonable CHECK (
        fecha_pago >= '2020-01-01' AND 
        fecha_pago <= CURRENT_DATE + INTERVAL '30 days'
    )
);

-- ========================================
-- Convert to TimescaleDB hypertable for time-series optimization
-- ========================================
SELECT create_hypertable(
    'raw_pagos', 
    'fecha_pago',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ========================================
-- Performance indexes for common queries (TimescaleDB optimized)
-- ========================================
CREATE INDEX IF NOT EXISTS idx_raw_pagos_nro_documento 
    ON raw_pagos(nro_documento, fecha_pago);

CREATE INDEX IF NOT EXISTS idx_raw_pagos_archivo 
    ON raw_pagos(archivo) INCLUDE (fecha_pago);

CREATE INDEX IF NOT EXISTS idx_raw_pagos_cod_sistema 
    ON raw_pagos(cod_sistema, fecha_pago);

CREATE INDEX IF NOT EXISTS idx_raw_pagos_creado_el 
    ON raw_pagos(creado_el, fecha_pago);

-- Composite indexes for business queries
CREATE INDEX IF NOT EXISTS idx_raw_pagos_documento_fecha 
    ON raw_pagos(nro_documento, fecha_pago, monto_cancelado);

CREATE INDEX IF NOT EXISTS idx_raw_pagos_fecha_monto 
    ON raw_pagos(fecha_pago, monto_cancelado) INCLUDE (nro_documento);

CREATE INDEX IF NOT EXISTS idx_raw_pagos_archivo_fecha 
    ON raw_pagos(archivo, fecha_pago, nro_documento);

-- Index for payment analytics and aggregations
CREATE INDEX IF NOT EXISTS idx_raw_pagos_analytics
    ON raw_pagos(fecha_pago, cod_sistema) 
    INCLUDE (monto_cancelado, nro_documento);

-- ========================================
-- Update trigger for updated_at
-- ========================================
CREATE OR REPLACE FUNCTION update_raw_pagos_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_raw_pagos_updated_at ON raw_pagos;
CREATE TRIGGER trigger_raw_pagos_updated_at
    BEFORE UPDATE ON raw_pagos
    FOR EACH ROW
    EXECUTE FUNCTION update_raw_pagos_updated_at();

-- ========================================
-- Comments for documentation
-- ========================================
COMMENT ON TABLE raw_pagos IS 'Raw staging table for BigQuery pagos (payments) data with TimescaleDB optimization';
COMMENT ON COLUMN raw_pagos.fecha_pago IS 'Time dimension for hypertable partitioning - payment date';
COMMENT ON COLUMN raw_pagos.nro_documento IS 'Document/Account identifier - links to trandeuda';
COMMENT ON COLUMN raw_pagos.monto_cancelado IS 'Payment amount - must be positive';
COMMENT ON COLUMN raw_pagos.archivo IS 'Source file identifier - cumulative files need deduplication';
COMMENT ON CONSTRAINT chk_raw_pagos_monto_positive ON raw_pagos IS 'Ensures payment amounts are positive';
COMMENT ON CONSTRAINT chk_raw_pagos_fecha_reasonable ON raw_pagos IS 'Validates payment dates within reasonable range';
