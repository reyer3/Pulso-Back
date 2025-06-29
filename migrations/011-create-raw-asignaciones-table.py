"""
011: Create raw_asignaciones table for BigQuery extraction staging

Raw staging table to store asignaciones data from BigQuery before transformation
"""

from yoyo import step

__depends__ = {'010-create-raw-calendario-table'}

steps = [
    step("""
        CREATE TABLE IF NOT EXISTS raw_asignaciones (
            -- Primary identification (composite key)
            cod_luna VARCHAR(50) NOT NULL,
            cuenta VARCHAR(50) NOT NULL,
            archivo VARCHAR(200) NOT NULL,
            
            -- Client information  
            cliente VARCHAR(50),
            telefono VARCHAR(50),
            
            -- Business classification
            tramo_gestion VARCHAR(100),
            negocio VARCHAR(100),
            dias_sin_trafico VARCHAR(50),
            
            -- Risk and behavior scoring
            decil_contacto INTEGER,
            decil_pago INTEGER,
            
            -- Account details
            min_vto DATE,
            zona VARCHAR(100),
            rango_renta INTEGER,
            campania_act VARCHAR(200),
            
            -- Payment arrangement details
            fraccionamiento VARCHAR(100),
            cuota_fracc_act VARCHAR(100),
            fecha_corte DATE,
            priorizado VARCHAR(50),
            inscripcion VARCHAR(100),
            incrementa_velocidad VARCHAR(50),
            detalle_dscto_futuro VARCHAR(200),
            cargo_fijo VARCHAR(100),
            
            -- Client identification
            dni VARCHAR(20),
            estado_pc VARCHAR(50),
            tipo_linea VARCHAR(100),
            cod_sistema INTEGER,
            tipo_alta VARCHAR(100),
            
            -- Technical metadata
            creado_el TIMESTAMPTZ,
            fecha_asignacion DATE,  -- Derived from creado_el
            motivo_rechazo VARCHAR(200),
            
            -- ETL metadata
            extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            
            -- Primary key constraint
            PRIMARY KEY (cod_luna, cuenta, archivo)
        );
    """),
    
    step("""
        -- Performance indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_cod_luna 
            ON raw_asignaciones(cod_luna);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_cuenta 
            ON raw_asignaciones(cuenta);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_archivo 
            ON raw_asignaciones(archivo);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_fecha_asignacion 
            ON raw_asignaciones(fecha_asignacion);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_negocio 
            ON raw_asignaciones(negocio);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_tramo_gestion 
            ON raw_asignaciones(tramo_gestion);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_decil_contacto 
            ON raw_asignaciones(decil_contacto);
        CREATE INDEX IF NOT EXISTS idx_raw_asignaciones_creado_el 
            ON raw_asignaciones(creado_el);
    """),
    
    step("""
        -- Update trigger for updated_at
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
    """)
]
