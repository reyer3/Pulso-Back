-- ========================================
-- 015: Create gestiones_unificadas table for BigQuery extraction staging
-- TimescaleDB optimized version
-- ========================================

-- Raw staging table to store unified gestiones data from BigQuery view before transformation
CREATE TABLE IF NOT EXISTS gestiones_unificadas (
    -- Core identification
    cod_luna TEXT NOT NULL,
    timestamp_gestion TIMESTAMPTZ NOT NULL,
    fecha_gestion DATE NOT NULL,
    
    -- Channel information
    canal_origen TEXT NOT NULL, -- 'BOT' or 'HUMANO'
    
    -- Original management data (before homologation)
    management_original TEXT,
    sub_management_original TEXT,
    compromiso_original TEXT,
    
    -- Homologated classification (business ready)
    nivel_1 TEXT,
    nivel_2 TEXT,
    contactabilidad TEXT,
    
    -- Business flags for KPI calculation
    es_contacto_efectivo BOOLEAN DEFAULT FALSE,
    es_contacto_no_efectivo BOOLEAN DEFAULT FALSE,
    es_compromiso BOOLEAN DEFAULT FALSE,
    
    -- Weighting for business logic
    peso_gestion INTEGER DEFAULT 1,
    
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key constraint
    PRIMARY KEY (cod_luna, timestamp_gestion),
    
    -- Business constraints
    CONSTRAINT chk_gestiones_unificadas_canal_origen 
        CHECK (canal_origen IN ('BOT', 'HUMANO')),
    CONSTRAINT chk_gestiones_unificadas_contactabilidad 
        CHECK (contactabilidad IN ('Contacto Efectivo', 'Contacto No Efectivo', 'SIN_CLASIFICAR') OR contactabilidad IS NULL),
    CONSTRAINT chk_gestiones_unificadas_peso_positive 
        CHECK (peso_gestion > 0),
    CONSTRAINT chk_gestiones_unificadas_fecha_consistency 
        CHECK (fecha_gestion = DATE(timestamp_gestion)),
    CONSTRAINT chk_gestiones_unificadas_contact_exclusive 
        CHECK (NOT (es_contacto_efectivo = TRUE AND es_contacto_no_efectivo = TRUE))
);

-- ========================================
-- Convert to TimescaleDB hypertable for time-series optimization
-- ========================================
SELECT create_hypertable(
    'gestiones_unificadas', 
    'timestamp_gestion',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ========================================
-- Performance indexes for common queries (TimescaleDB optimized)
-- ========================================
CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_cod_luna 
    ON gestiones_unificadas(cod_luna, timestamp_gestion);

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_fecha_gestion 
    ON gestiones_unificadas(fecha_gestion, timestamp_gestion);

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_canal_origen 
    ON gestiones_unificadas(canal_origen, timestamp_gestion);

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_contactabilidad 
    ON gestiones_unificadas(contactabilidad, timestamp_gestion);

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_nivel_1 
    ON gestiones_unificadas(nivel_1, timestamp_gestion);

-- Business logic indexes for KPI calculations
CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_contacto_efectivo 
    ON gestiones_unificadas(timestamp_gestion, cod_luna) 
    WHERE es_contacto_efectivo = TRUE;

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_compromiso 
    ON gestiones_unificadas(timestamp_gestion, cod_luna) 
    WHERE es_compromiso = TRUE;

-- Composite indexes for common business queries
CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_luna_fecha 
    ON gestiones_unificadas(cod_luna, fecha_gestion, timestamp_gestion);

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_fecha_canal 
    ON gestiones_unificadas(fecha_gestion, canal_origen) 
    INCLUDE (cod_luna, peso_gestion);

CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_fecha_contactabilidad 
    ON gestiones_unificadas(fecha_gestion, contactabilidad) 
    INCLUDE (cod_luna, es_contacto_efectivo, es_compromiso);

-- Analytics index for KPI calculations
CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_kpi_analytics
    ON gestiones_unificadas(fecha_gestion, cod_luna)
    INCLUDE (es_contacto_efectivo, es_contacto_no_efectivo, es_compromiso, peso_gestion, canal_origen);

-- ========================================
-- Update trigger for updated_at
-- ========================================
CREATE OR REPLACE FUNCTION update_gestiones_unificadas_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_gestiones_unificadas_updated_at ON gestiones_unificadas;
CREATE TRIGGER trigger_gestiones_unificadas_updated_at
    BEFORE UPDATE ON gestiones_unificadas
    FOR EACH ROW
    EXECUTE FUNCTION update_gestiones_unificadas_updated_at();

-- ========================================
-- Comments for documentation
-- ========================================
COMMENT ON TABLE gestiones_unificadas IS 'Raw staging table for unified BigQuery gestiones (bot + human) data with TimescaleDB optimization';
COMMENT ON COLUMN gestiones_unificadas.timestamp_gestion IS 'Time dimension for hypertable partitioning - exact gestion timestamp';
COMMENT ON COLUMN gestiones_unificadas.cod_luna IS 'Client identifier - links to asignaciones';
COMMENT ON COLUMN gestiones_unificadas.canal_origen IS 'Channel: BOT or HUMANO';
COMMENT ON COLUMN gestiones_unificadas.contactabilidad IS 'Homologated contact result';
COMMENT ON COLUMN gestiones_unificadas.es_contacto_efectivo IS 'Flag for PCT_CONTAC KPI calculation';
COMMENT ON COLUMN gestiones_unificadas.es_compromiso IS 'Flag for PCT_EFECTIVIDAD KPI calculation';
COMMENT ON COLUMN gestiones_unificadas.peso_gestion IS 'Weight for business logic calculations';
