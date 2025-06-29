-- ========================================
-- 011: Create raw_calendario table for BigQuery extraction staging
-- TimescaleDB optimized version
-- ========================================

-- Raw staging table to store calendario data from BigQuery before transformation
CREATE TABLE IF NOT EXISTS raw_calendario (
    -- Primary identification
    ARCHIVO TEXT PRIMARY KEY,
    
    -- Campaign metadata
    TIPO_CARTERA TEXT,
    
    -- Business dates (core for campaign logic)
    fecha_apertura DATE NOT NULL,
    fecha_trandeuda DATE,
    fecha_cierre DATE,
    FECHA_CIERRE_PLANIFICADA DATE,
    
    -- Campaign characteristics
    DURACION_CAMPANA_DIAS_HABILES INTEGER,
    ANNO_ASIGNACION INTEGER,
    PERIODO_ASIGNACION TEXT,
    ES_CARTERA_ABIERTA BOOLEAN,
    RANGO_VENCIMIENTO TEXT,
    ESTADO_CARTERA TEXT,
    
    -- Time partitioning fields (for TimescaleDB hypertable)
    periodo_mes TEXT,
    periodo_date DATE NOT NULL,
    
    -- Campaign classification
    tipo_ciclo_campana TEXT,
    categoria_duracion TEXT,
    
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Convert to TimescaleDB hypertable for time-series optimization
-- ========================================
SELECT create_hypertable(
    'raw_calendario', 
    'periodo_date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ========================================
-- Indexes for query performance (TimescaleDB optimized)
-- ========================================
CREATE INDEX IF NOT EXISTS idx_raw_calendario_fecha_apertura 
    ON raw_calendario(fecha_apertura, periodo_date);

CREATE INDEX IF NOT EXISTS idx_raw_calendario_tipo_cartera 
    ON raw_calendario(TIPO_CARTERA, periodo_date);

CREATE INDEX IF NOT EXISTS idx_raw_calendario_estado 
    ON raw_calendario(ESTADO_CARTERA, periodo_date);

CREATE INDEX IF NOT EXISTS idx_raw_calendario_archivo 
    ON raw_calendario(ARCHIVO) INCLUDE (periodo_date);

-- ========================================
-- Update trigger for updated_at
-- ========================================
CREATE OR REPLACE FUNCTION update_raw_calendario_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_raw_calendario_updated_at ON raw_calendario;
CREATE TRIGGER trigger_raw_calendario_updated_at
    BEFORE UPDATE ON raw_calendario
    FOR EACH ROW
    EXECUTE FUNCTION update_raw_calendario_updated_at();

-- ========================================
-- Comments for documentation
-- ========================================
COMMENT ON TABLE raw_calendario IS 'Raw staging table for BigQuery calendario data with TimescaleDB optimization';
COMMENT ON COLUMN raw_calendario.periodo_date IS 'Time dimension for hypertable partitioning';
COMMENT ON COLUMN raw_calendario.ARCHIVO IS 'Primary key - campaign file identifier';
