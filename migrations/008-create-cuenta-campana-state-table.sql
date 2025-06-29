-- Create cuenta_campana_state table for tracking account states per campaign window
-- depends: 003-create-dashboard-data-table

CREATE TABLE cuenta_campana_state (
    archivo VARCHAR(200) NOT NULL,              -- Campaign identifier (from calendario)
    cod_luna VARCHAR(50) NOT NULL,              -- Client identifier  
    cuenta VARCHAR(50) NOT NULL,                -- Account identifier
    fecha_apertura DATE NOT NULL,               -- Campaign start (from calendario)
    fecha_cierre DATE,                          -- Campaign end (from calendario, nullable for open campaigns)
    
    -- Account state within this campaign
    monto_inicial DECIMAL(15,2) DEFAULT 0.0,    -- Initial debt at campaign start
    monto_actual DECIMAL(15,2) DEFAULT 0.0,     -- Current debt in campaign
    fecha_ultima_actualizacion DATE,            -- Last debt update date
    
    -- Activity flags for business logic
    tiene_deuda_activa BOOLEAN DEFAULT FALSE,   -- Has active debt > 0
    es_cuenta_gestionable BOOLEAN DEFAULT FALSE, -- Is in scope for management
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (archivo, cod_luna, cuenta)
);

-- Performance indexes
CREATE INDEX idx_cuenta_campana_archivo ON cuenta_campana_state(archivo);
CREATE INDEX idx_cuenta_campana_fecha_apertura ON cuenta_campana_state(fecha_apertura);
CREATE INDEX idx_cuenta_campana_cod_luna ON cuenta_campana_state(cod_luna);
CREATE INDEX idx_cuenta_campana_gestionable ON cuenta_campana_state(es_cuenta_gestionable) WHERE es_cuenta_gestionable = TRUE;

-- NOTE: cuenta_campana_state is NOT converted to hypertable because:
-- 1. It's account state data per campaign (configuration/lookup table)
-- 2. Primary key doesn't include partitioning column (fecha_apertura)  
-- 3. Not high-volume time-series data - it's business state data

-- Comments for documentation
COMMENT ON TABLE cuenta_campana_state IS 'Tracks account states within campaign windows for accurate metric calculation - NOT a hypertable (state data, not time-series)';
COMMENT ON COLUMN cuenta_campana_state.archivo IS 'Campaign file identifier from calendario table';
COMMENT ON COLUMN cuenta_campana_state.cod_luna IS 'Client identifier (can have multiple accounts)';
COMMENT ON COLUMN cuenta_campana_state.cuenta IS 'Specific account identifier';
COMMENT ON COLUMN cuenta_campana_state.es_cuenta_gestionable IS 'Whether account is in scope for management actions';
COMMENT ON COLUMN cuenta_campana_state.monto_inicial IS 'Account debt at campaign start (for coverage calculation)';
COMMENT ON COLUMN cuenta_campana_state.monto_actual IS 'Current account debt (for closure calculation)';
