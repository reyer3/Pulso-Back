-- ========================================
-- 012: Create raw_asignaciones table for BigQuery extraction staging  
-- TimescaleDB optimized version - PRIMARY KEY FIXED
-- ========================================

-- Raw staging table to store asignaciones data from BigQuery before transformation
CREATE TABLE IF NOT EXISTS raw_asignaciones (
    -- Primary identification (composite key)
    cod_luna TEXT NOT NULL,
    cuenta TEXT NOT NULL,
    archivo TEXT NOT NULL,
    
    -- Client information  
    cliente TEXT,
    telefono TEXT,
    
    -- Business classification
    tramo_gestion TEXT,
    negocio TEXT,
    dias_sin_trafico TEXT,
    
    -- Risk and behavior scoring
    decil_contacto INTEGER,
    decil_pago INTEGER,
    
    -- Account details
    min_vto DATE,
    zona TEXT,
    rango_renta INTEGER,
    campania_act TEXT,
    
    -- Payment arrangement details
    fraccionamiento TEXT,
    cuota_fracc_act TEXT,
    fecha_corte DATE,
    priorizado TEXT,
    inscripcion TEXT,
    incrementa_velocidad TEXT,
    detalle_dscto_futuro TEXT,
    cargo_fijo TEXT,
    
    -- Client identification
    dni TEXT,
    estado_pc TEXT,
    tipo_linea TEXT,
    cod_sistema INTEGER,
    tipo_alta TEXT,
    
    -- Technical metadata
    creado_el TIMESTAMPTZ,
    fecha_asignacion DATE NOT NULL,  -- Derived from creado_el (for hypertable)
    motivo_rechazo TEXT,
    
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- ✅ FIXED: Primary key includes partitioning column
    PRIMARY KEY (cod_luna, cuenta, archivo, fecha_asignacion)
);

-- ========================================
-- Convert to TimescaleDB hypertable for time-series optimization
-- ========================================
SELECT create_hypertable(
    'raw_asignaciones', 
    'fecha_asignacion',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ========================================
-- Performance indexes for common queries (TimescaleDB optimized)
-- ========================================
CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_cod_luna 
    ON raw_asignaciones(cod_luna, fecha_asignacion);

CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_cuenta 
    ON raw_asignaciones(cuenta, fecha_asignacion);

CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_archivo 
    ON raw_asignaciones(archivo) INCLUDE (fecha_asignacion);

CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_negocio 
    ON raw_asignaciones(negocio, fecha_asignacion);

CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_tramo_gestion 
    ON raw_asignaciones(tramo_gestion, fecha_asignacion);

CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_decil_contacto 
    ON raw_asignaciones(decil_contacto, fecha_asignacion);

CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_creado_el 
    ON raw_asignaciones(creado_el, fecha_asignacion);

-- Composite index for business logic queries
CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_business_lookup
    ON raw_asignaciones(cod_luna, negocio, tramo_gestion, fecha_asignacion);

-- ========================================
-- Update trigger for updated_at
-- ========================================
CREATE OR REPLACE FUNCTION update_raw_asignaciones_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_raw_asignaciones_updated_at ON raw_asignaciones;
CREATE TRIGGER trigger_raw_asignaciones_updated_at
    BEFORE UPDATE ON raw_asignaciones
    FOR EACH ROW
    EXECUTE FUNCTION update_raw_asignaciones_updated_at();

-- ========================================
-- Comments for documentation
-- ========================================
COMMENT ON TABLE raw_asignaciones IS 'Raw staging table for BigQuery asignaciones data with TimescaleDB optimization';
COMMENT ON COLUMN raw_asignaciones.fecha_asignacion IS 'Time dimension for hypertable partitioning';
COMMENT ON COLUMN raw_asignaciones.cod_luna IS 'Client identifier - core business key';
COMMENT ON COLUMN raw_asignaciones.cuenta IS 'Account identifier - core business key';
COMMENT ON COLUMN raw_asignaciones.archivo IS 'Campaign file identifier - links to calendario';
