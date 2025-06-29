-- Create gestion_cuenta_impact table for mapping gestiones to affected accounts
-- depends: 008-create-cuenta-campana-state-table

CREATE TABLE gestion_cuenta_impact (
    archivo VARCHAR(200) NOT NULL,              -- Campaign identifier
    cod_luna VARCHAR(50) NOT NULL,              -- Client who was contacted
    timestamp_gestion TIMESTAMP WITH TIME ZONE NOT NULL, -- When the gestión occurred
    cuenta VARCHAR(50) NOT NULL,                -- Specific account impacted by this gestión
    
    -- Gestión details (from gestiones_unificadas view)
    canal_origen VARCHAR(20) NOT NULL,          -- 'BOT' | 'HUMANO'
    contactabilidad VARCHAR(50),                -- 'Contacto Efectivo' | 'Contacto No Efectivo'
    es_contacto_efectivo BOOLEAN DEFAULT FALSE, -- Flag for PCT_CONTAC calculation
    es_compromiso BOOLEAN DEFAULT FALSE,        -- Flag for PCT_EFECTIVIDAD calculation
    peso_gestion INTEGER DEFAULT 0,             -- Gestión weight for intensity calculation
    
    -- Business context at time of gestión
    monto_deuda_momento DECIMAL(15,2) DEFAULT 0.0, -- Account debt at time of gestión
    
    -- Derived business flags for easier aggregation
    es_cuenta_con_deuda BOOLEAN DEFAULT FALSE,   -- Had debt > 0 at moment of gestión
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (archivo, cod_luna, timestamp_gestion, cuenta)
);

-- Performance indexes for KPI calculation queries
CREATE INDEX idx_gestion_impact_archivo ON gestion_cuenta_impact(archivo);
CREATE INDEX idx_gestion_impact_timestamp ON gestion_cuenta_impact(timestamp_gestion);
CREATE INDEX idx_gestion_impact_cod_luna ON gestion_cuenta_impact(cod_luna);
CREATE INDEX idx_gestion_impact_cuenta ON gestion_cuenta_impact(cuenta);
CREATE INDEX idx_gestion_impact_contacto_efectivo ON gestion_cuenta_impact(es_contacto_efectivo) WHERE es_contacto_efectivo = TRUE;
CREATE INDEX idx_gestion_impact_compromiso ON gestion_cuenta_impact(es_compromiso) WHERE es_compromiso = TRUE;

-- Composite indexes for common KPI queries
CREATE INDEX idx_gestion_impact_kpi_coverage ON gestion_cuenta_impact(archivo, es_contacto_efectivo, cuenta);
CREATE INDEX idx_gestion_impact_kpi_contact ON gestion_cuenta_impact(archivo, contactabilidad);
CREATE INDEX idx_gestion_impact_kpi_effectiveness ON gestion_cuenta_impact(archivo, es_contacto_efectivo, es_compromiso);

-- TimescaleDB hypertable for time-series performance
SELECT create_hypertable('gestion_cuenta_impact', by_range('timestamp_gestion', INTERVAL '7 days'), if_not_exists => TRUE);

-- Retention policy for operational data (keep 1 year)
SELECT add_retention_policy('gestion_cuenta_impact', INTERVAL '1 year', if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE gestion_cuenta_impact IS 'Maps each gestión to the specific accounts that were impacted, enabling accurate KPI calculation';
COMMENT ON COLUMN gestion_cuenta_impact.archivo IS 'Campaign identifier linking to cuenta_campana_state';
COMMENT ON COLUMN gestion_cuenta_impact.cod_luna IS 'Client identifier - the person who was contacted';
COMMENT ON COLUMN gestion_cuenta_impact.cuenta IS 'Specific account impacted by the gestión (one client can have multiple accounts)';
COMMENT ON COLUMN gestion_cuenta_impact.timestamp_gestion IS 'Exact timestamp when gestión occurred';
COMMENT ON COLUMN gestion_cuenta_impact.es_contacto_efectivo IS 'Used for PCT_CONTAC calculation (effective contacts / total contacts)';
COMMENT ON COLUMN gestion_cuenta_impact.es_compromiso IS 'Used for PCT_EFECTIVIDAD calculation (commitments / effective contacts)';
COMMENT ON COLUMN gestion_cuenta_impact.monto_deuda_momento IS 'Account debt at time of gestión for context';
