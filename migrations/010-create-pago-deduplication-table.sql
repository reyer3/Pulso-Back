-- Create pago_deduplication table for unique payment tracking per campaign window
-- depends: 009-create-gestion-cuenta-impact-table

CREATE TABLE pago_deduplication (
    archivo VARCHAR(200) NOT NULL,              -- Campaign identifier
    cuenta VARCHAR(50) NOT NULL,                -- Account that made the payment
    nro_documento VARCHAR(100) NOT NULL,        -- Payment document number
    fecha_pago DATE NOT NULL,                   -- Payment date
    monto_cancelado DECIMAL(15,2) NOT NULL,     -- Payment amount
    
    -- Deduplication logic fields
    es_pago_unico BOOLEAN DEFAULT TRUE,         -- TRUE for first occurrence, FALSE for duplicates
    fecha_primera_carga DATE NOT NULL,          -- First time this payment was loaded (from filename)
    fecha_ultima_carga DATE NOT NULL,           -- Last time this payment was seen (from filename)
    veces_visto INTEGER DEFAULT 1,              -- How many times this payment has been loaded
    
    -- Business attribution
    esta_en_ventana BOOLEAN DEFAULT FALSE,      -- Payment is within campaign window
    cod_luna VARCHAR(50),                       -- Client associated with this account (from asignaciones)
    
    -- Recovery calculation support
    es_pago_valido BOOLEAN DEFAULT TRUE,        -- Passes business validation rules
    motivo_rechazo TEXT,                        -- Reason if payment is rejected
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite primary key for payment uniqueness
    PRIMARY KEY (archivo, nro_documento, fecha_pago, monto_cancelado)
);

-- Performance indexes
CREATE INDEX idx_pago_dedup_archivo ON pago_deduplication(archivo);
CREATE INDEX idx_pago_dedup_cuenta ON pago_deduplication(cuenta);
CREATE INDEX idx_pago_dedup_fecha_pago ON pago_deduplication(fecha_pago);
CREATE INDEX idx_pago_dedup_cod_luna ON pago_deduplication(cod_luna);
CREATE INDEX idx_pago_dedup_unico ON pago_deduplication(es_pago_unico) WHERE es_pago_unico = TRUE;
CREATE INDEX idx_pago_dedup_valido ON pago_deduplication(es_pago_valido) WHERE es_pago_valido = TRUE;
CREATE INDEX idx_pago_dedup_ventana ON pago_deduplication(esta_en_ventana) WHERE esta_en_ventana = TRUE;

-- Composite indexes for recovery calculation
CREATE INDEX idx_pago_dedup_recovery ON pago_deduplication(archivo, es_pago_unico, es_pago_valido, esta_en_ventana);
CREATE INDEX idx_pago_dedup_timeline ON pago_deduplication(archivo, fecha_pago, es_pago_unico);

-- TimescaleDB hypertable for time-series performance (partitioned by payment date)
SELECT create_hypertable('pago_deduplication', by_range('fecha_pago', INTERVAL '30 days'), if_not_exists => TRUE);

-- Retention policy for payment data (keep 2 years)
SELECT add_retention_policy('pago_deduplication', INTERVAL '2 years', if_not_exists => TRUE);

-- Constraint to ensure positive payment amounts
ALTER TABLE pago_deduplication ADD CONSTRAINT chk_pago_monto_positive CHECK (monto_cancelado > 0);

-- Comments for documentation  
COMMENT ON TABLE pago_deduplication IS 'Deduplicates payments across file reloads and tracks payment attribution to campaigns';
COMMENT ON COLUMN pago_deduplication.archivo IS 'Campaign identifier linking to cuenta_campana_state';
COMMENT ON COLUMN pago_deduplication.es_pago_unico IS 'TRUE only for the first occurrence of this payment (based on fecha_primera_carga)';
COMMENT ON COLUMN pago_deduplication.esta_en_ventana IS 'TRUE if payment date falls within campaign apertura/cierre window';
COMMENT ON COLUMN pago_deduplication.fecha_primera_carga IS 'Date from filename when this payment was first loaded (business date, not creado_el)';
COMMENT ON COLUMN pago_deduplication.fecha_ultima_carga IS 'Date from filename when this payment was last seen (tracks reprocessing)';
COMMENT ON COLUMN pago_deduplication.veces_visto IS 'Counter of how many times this payment has been reprocessed';
COMMENT ON COLUMN pago_deduplication.cod_luna IS 'Client identifier derived from cuenta via asignaciones lookup';

-- Create a partial unique index on the business key for unique payments
CREATE UNIQUE INDEX idx_pago_dedup_business_key ON pago_deduplication(nro_documento, fecha_pago, monto_cancelado) 
    WHERE es_pago_unico = TRUE;
