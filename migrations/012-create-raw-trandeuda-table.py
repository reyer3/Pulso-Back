"""
012: Create raw_trandeuda table for BigQuery extraction staging

Raw staging table to store trandeuda data from BigQuery before transformation
"""

from yoyo import step

__depends__ = {'011-create-raw-asignaciones-table'}

steps = [
    step("""
        CREATE TABLE IF NOT EXISTS raw_trandeuda (
            -- Account identification
            cod_cuenta VARCHAR(50) NOT NULL,
            nro_documento VARCHAR(100) NOT NULL,
            archivo VARCHAR(200) NOT NULL,
            
            -- Debt information
            fecha_vencimiento DATE,
            monto_exigible DECIMAL(15,2) NOT NULL,
            
            -- Technical metadata
            creado_el TIMESTAMPTZ,
            fecha_proceso DATE,  -- Derived from creado_el
            motivo_rechazo VARCHAR(200),
            
            -- ETL metadata
            extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            
            -- Primary key constraint
            PRIMARY KEY (cod_cuenta, nro_documento, archivo)
        );
    """),
    
    step("""
        -- Performance indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_cod_cuenta 
            ON raw_trandeuda(cod_cuenta);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_nro_documento 
            ON raw_trandeuda(nro_documento);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_archivo 
            ON raw_trandeuda(archivo);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_fecha_proceso 
            ON raw_trandeuda(fecha_proceso);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_fecha_vencimiento 
            ON raw_trandeuda(fecha_vencimiento);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_monto_exigible 
            ON raw_trandeuda(monto_exigible);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_creado_el 
            ON raw_trandeuda(creado_el);
        
        -- Composite index for common business queries
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_cuenta_fecha 
            ON raw_trandeuda(cod_cuenta, fecha_proceso);
        CREATE INDEX IF NOT EXISTS idx_raw_trandeuda_archivo_fecha 
            ON raw_trandeuda(archivo, fecha_proceso);
    """),
    
    step("""
        -- Update trigger for updated_at
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
    """),
    
    step("""
        -- Add check constraint for valid debt amounts
        ALTER TABLE raw_trandeuda 
        ADD CONSTRAINT chk_raw_trandeuda_monto_positive 
        CHECK (monto_exigible >= 0);
    """)
]
