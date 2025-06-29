"""
014: Create gestiones_unificadas table for BigQuery extraction staging

Raw staging table to store unified gestiones data from BigQuery view before transformation
"""

from yoyo import step

__depends__ = {'013-create-raw-pagos-table'}

steps = [
    step("""
        CREATE TABLE IF NOT EXISTS gestiones_unificadas (
            -- Core identification
            cod_luna VARCHAR(50) NOT NULL,
            timestamp_gestion TIMESTAMPTZ NOT NULL,
            fecha_gestion DATE NOT NULL,
            
            -- Channel information
            canal_origen VARCHAR(20) NOT NULL, -- 'BOT' or 'HUMANO'
            
            -- Original management data (before homologation)
            management_original VARCHAR(200),
            sub_management_original VARCHAR(200),
            compromiso_original VARCHAR(200),
            
            -- Homologated classification (business ready)
            nivel_1 VARCHAR(100),
            nivel_2 VARCHAR(100),
            contactabilidad VARCHAR(50),
            
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
            PRIMARY KEY (cod_luna, timestamp_gestion)
        );
    """),
    
    step("""
        -- Performance indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_cod_luna 
            ON gestiones_unificadas(cod_luna);
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_fecha_gestion 
            ON gestiones_unificadas(fecha_gestion);
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_canal_origen 
            ON gestiones_unificadas(canal_origen);
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_contactabilidad 
            ON gestiones_unificadas(contactabilidad);
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_nivel_1 
            ON gestiones_unificadas(nivel_1);
        
        -- Business logic indexes for KPI calculations
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_contacto_efectivo 
            ON gestiones_unificadas(es_contacto_efectivo) WHERE es_contacto_efectivo = TRUE;
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_compromiso 
            ON gestiones_unificadas(es_compromiso) WHERE es_compromiso = TRUE;
        
        -- Composite indexes for common business queries
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_luna_fecha 
            ON gestiones_unificadas(cod_luna, fecha_gestion);
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_fecha_canal 
            ON gestiones_unificadas(fecha_gestion, canal_origen);
        CREATE INDEX IF NOT EXISTS idx_gestiones_unificadas_fecha_contactabilidad 
            ON gestiones_unificadas(fecha_gestion, contactabilidad);
    """),
    
    step("""
        -- Update trigger for updated_at
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
    """),
    
    step("""
        -- Business constraints
        ALTER TABLE gestiones_unificadas 
        ADD CONSTRAINT chk_gestiones_unificadas_canal_origen 
        CHECK (canal_origen IN ('BOT', 'HUMANO'));
        
        ALTER TABLE gestiones_unificadas 
        ADD CONSTRAINT chk_gestiones_unificadas_contactabilidad 
        CHECK (contactabilidad IN ('Contacto Efectivo', 'Contacto No Efectivo', 'SIN_CLASIFICAR') OR contactabilidad IS NULL);
        
        ALTER TABLE gestiones_unificadas 
        ADD CONSTRAINT chk_gestiones_unificadas_peso_positive 
        CHECK (peso_gestion > 0);
        
        -- Derived date consistency
        ALTER TABLE gestiones_unificadas 
        ADD CONSTRAINT chk_gestiones_unificadas_fecha_consistency 
        CHECK (fecha_gestion = DATE(timestamp_gestion));
        
        -- Business logic: can't be both effective and non-effective contact
        ALTER TABLE gestiones_unificadas 
        ADD CONSTRAINT chk_gestiones_unificadas_contact_exclusive 
        CHECK (NOT (es_contacto_efectivo = TRUE AND es_contacto_no_efectivo = TRUE));
    """)
]
