"""
010: Create raw_calendario table for BigQuery extraction staging

Raw staging table to store calendario data from BigQuery before transformation
"""

from yoyo import step

__depends__ = {'009-create-gestion-cuenta-impact-table'}

steps = [
    step("""
        CREATE TABLE IF NOT EXISTS raw_calendario (
            -- Primary identification
            ARCHIVO VARCHAR(200) PRIMARY KEY,
            
            -- Campaign metadata
            TIPO_CARTERA VARCHAR(100),
            
            -- Business dates (core for campaign logic)
            fecha_apertura DATE NOT NULL,
            fecha_trandeuda DATE,
            fecha_cierre DATE,
            FECHA_CIERRE_PLANIFICADA DATE,
            
            -- Campaign characteristics
            DURACION_CAMPANA_DIAS_HABILES INTEGER,
            ANNO_ASIGNACION INTEGER,
            PERIODO_ASIGNACION VARCHAR(20),
            ES_CARTERA_ABIERTA BOOLEAN,
            RANGO_VENCIMIENTO VARCHAR(100),
            ESTADO_CARTERA VARCHAR(50),
            
            -- Time partitioning fields
            periodo_mes VARCHAR(10),
            periodo_date DATE,
            
            -- Campaign classification
            tipo_ciclo_campana VARCHAR(100),
            categoria_duracion VARCHAR(50),
            
            -- ETL metadata
            extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
    """),
    
    step("""
        -- Indexes for query performance
        CREATE INDEX IF NOT EXISTS idx_raw_calendario_fecha_apertura 
            ON raw_calendario(fecha_apertura);
        CREATE INDEX IF NOT EXISTS idx_raw_calendario_periodo_date 
            ON raw_calendario(periodo_date);
        CREATE INDEX IF NOT EXISTS idx_raw_calendario_tipo_cartera 
            ON raw_calendario(TIPO_CARTERA);
        CREATE INDEX IF NOT EXISTS idx_raw_calendario_estado 
            ON raw_calendario(ESTADO_CARTERA);
    """),
    
    step("""
        -- Update trigger for updated_at
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
    """)
]
