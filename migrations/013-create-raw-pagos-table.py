"""
013: Create raw_pagos table for BigQuery extraction staging

Raw staging table to store pagos data from BigQuery before transformation
"""

from yoyo import step

__depends__ = {'012-create-raw-trandeuda-table'}

steps = [
    step("""
        CREATE TABLE IF NOT EXISTS raw_pagos (
            -- Payment identification
            nro_documento VARCHAR(100) NOT NULL,
            fecha_pago DATE NOT NULL,
            monto_cancelado DECIMAL(15,2) NOT NULL,
            
            -- System identification
            cod_sistema VARCHAR(50),
            archivo VARCHAR(200) NOT NULL,
            
            -- Technical metadata
            creado_el TIMESTAMPTZ,
            motivo_rechazo VARCHAR(200),
            
            -- ETL metadata
            extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            
            -- Primary key constraint (for deduplication)
            PRIMARY KEY (nro_documento, fecha_pago, monto_cancelado)
        );
    """),
    
    step("""
        -- Performance indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_nro_documento 
            ON raw_pagos(nro_documento);
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_fecha_pago 
            ON raw_pagos(fecha_pago);
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_archivo 
            ON raw_pagos(archivo);
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_cod_sistema 
            ON raw_pagos(cod_sistema);
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_creado_el 
            ON raw_pagos(creado_el);
        
        -- Composite indexes for business queries
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_documento_fecha 
            ON raw_pagos(nro_documento, fecha_pago);
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_fecha_monto 
            ON raw_pagos(fecha_pago, monto_cancelado);
        CREATE INDEX IF NOT EXISTS idx_raw_pagos_archivo_fecha 
            ON raw_pagos(archivo, fecha_pago);
    """),
    
    step("""
        -- Update trigger for updated_at
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
    """),
    
    step("""
        -- Add check constraint for valid payment amounts
        ALTER TABLE raw_pagos 
        ADD CONSTRAINT chk_raw_pagos_monto_positive 
        CHECK (monto_cancelado > 0);
        
        -- Add check constraint for reasonable dates
        ALTER TABLE raw_pagos 
        ADD CONSTRAINT chk_raw_pagos_fecha_reasonable 
        CHECK (fecha_pago >= '2020-01-01' AND fecha_pago <= CURRENT_DATE + INTERVAL '30 days');
    """)
]
