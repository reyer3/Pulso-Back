-- 007: Create unified gestiones table in the auxiliary schema for project P3fV4dWNeMkN5RJMhV8e
-- depends: 006-create-aux-tables
CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas (
    -- Core identification
    gestion_uid TEXT NOT NULL, -- UID from the source table
    timestamp_gestion TIMESTAMPTZ NOT NULL, -- Time partitioning column
    cod_luna TEXT NOT NULL,
    fecha_gestion DATE NOT NULL,

    -- Channel information
    canal_origen TEXT NOT NULL CHECK (canal_origen IN ('BOT', 'HUMANO')),

    -- Homologated classification
    nivel_1 TEXT,
    nivel_2 TEXT,
    nivel_3 TEXT,
    contactabilidad TEXT,

    -- Business flags for KPI calculation
    es_contacto_efectivo BOOLEAN DEFAULT FALSE,
    es_compromiso BOOLEAN DEFAULT FALSE,
    monto_compromiso NUMERIC(15,2),
    fecha_compromiso DATE,

    -- Metadata
    archivo_campana TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- ✅ CORRECCIÓN: La clave primaria ahora es compuesta e incluye la columna de particionamiento.
    PRIMARY KEY (gestion_uid, timestamp_gestion),

    -- Constraint para asegurar consistencia
    CONSTRAINT chk_aux_gu_fecha_consistency CHECK (fecha_gestion = DATE(timestamp_gestion))
);

COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas IS 'Unified and cleaned gestiones data from all channels. Populated by the ETL.';
COMMENT ON COLUMN aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas.gestion_uid IS 'Unique identifier of the original gestion (from raw sources).';
COMMENT ON COLUMN aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas.timestamp_gestion IS 'Time partitioning column for TimescaleDB.';
COMMENT ON COLUMN aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas.archivo_campana IS 'Identifier of the campaign this gestion belongs to.';

-- TimescaleDB hypertable
-- Esta llamada ahora funcionará porque la PK incluye 'timestamp_gestion'.
SELECT create_hypertable(
    'aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas',
    'timestamp_gestion',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Indexes for performance
-- Los índices deben incluir la columna de tiempo para un rendimiento óptimo en TimescaleDB.
CREATE INDEX IF NOT EXISTS idx_aux_gu_fecha_gestion ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(fecha_gestion, timestamp_gestion DESC);
CREATE INDEX IF NOT EXISTS idx_aux_gu_cod_luna ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(cod_luna, timestamp_gestion DESC);
CREATE INDEX IF NOT EXISTS idx_aux_gu_archivo_campana ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(archivo_campana, timestamp_gestion DESC);
CREATE INDEX IF NOT EXISTS idx_aux_gu_canal_origen ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(canal_origen, timestamp_gestion DESC);

-- Trigger for updated_at
DROP TRIGGER IF EXISTS trg_gestiones_unificadas_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas;
CREATE TRIGGER trg_gestiones_unificadas_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas
    FOR EACH ROW
    -- Asumimos que la función pública ya fue creada en una migración anterior
    EXECUTE FUNCTION public.update_timestamp_column();