-- 007: Create unified gestiones table in the auxiliary schema for project P3fV4dWNeMkN5RJMhV8e
-- depends: 006-create-aux-tables.sql
-- (Also depends on raw tables like raw_P3fV4dWNeMkN5RJMhV8e.asignaciones,
-- raw_P3fV4dWNeMkN5RJMhV8e.homologacion_mibotair etc. for its data population,
-- but structurally depends on the aux schema being present and potentially other aux tables if any FKs were planned)

CREATE TABLE IF NOT EXISTS aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas (
    -- Core identification
    gestion_uid TEXT PRIMARY KEY, -- UID from the source table (voicebot_gestiones.uid or mibotair_gestiones.uid)
    cod_luna TEXT NOT NULL,
    timestamp_gestion TIMESTAMPTZ NOT NULL, -- For TimescaleDB partitioning
    fecha_gestion DATE NOT NULL,

    -- Channel information
    canal_origen TEXT NOT NULL CHECK (canal_origen IN ('BOT', 'HUMANO')),

    -- Homologated classification (campos unificados)
    nivel_1 TEXT,
    nivel_2 TEXT,
    nivel_3 TEXT,
    contactabilidad TEXT, -- e.g., 'Contacto Efectivo', 'Contacto No Efectivo', 'SIN_CLASIFICAR'

    -- Business flags for KPI calculation
    es_contacto_efectivo BOOLEAN DEFAULT FALSE,
    es_compromiso BOOLEAN DEFAULT FALSE,
    monto_compromiso NUMERIC(15,2),
    fecha_compromiso DATE,

    -- Metadata
    archivo_campana TEXT NOT NULL, -- Reference to the campaign file (e.g., raw_P3fV4dWNeMkN5RJMhV8e.calendario.ARCHIVO)
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Constraint to ensure consistency
    CONSTRAINT chk_aux_gu_fecha_consistency CHECK (fecha_gestion = DATE(timestamp_gestion))
);

-- TimescaleDB hypertable
SELECT create_hypertable(
    'aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas',
    'timestamp_gestion',
    chunk_time_interval => INTERVAL '7 days', -- Or '1 month' depending on volume and query patterns
    if_not_exists => TRUE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_aux_gu_fecha_gestion ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(fecha_gestion, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gu_cod_luna ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(cod_luna, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gu_archivo_campana ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(archivo_campana, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gu_canal_origen ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(canal_origen, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gu_contactabilidad ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(contactabilidad, timestamp_gestion);
CREATE INDEX IF NOT EXISTS idx_aux_gu_es_contacto_efectivo ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(es_contacto_efectivo, timestamp_gestion) WHERE es_contacto_efectivo = TRUE;
CREATE INDEX IF NOT EXISTS idx_aux_gu_es_compromiso ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas(es_compromiso, timestamp_gestion) WHERE es_compromiso = TRUE;


COMMENT ON TABLE aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas IS 'Unified and cleaned gestiones data from Voicebot and MibotAir sources for project P3fV4dWNeMkN5RJMhV8e, populated by the ETL.';
COMMENT ON COLUMN aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas.gestion_uid IS 'Unique identifier of the original gestion (from raw_...voicebot_gestiones or raw_...mibotair_gestiones).';
COMMENT ON COLUMN aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas.archivo_campana IS 'Identifier of the campaign file this gestion belongs to.';

-- Trigger for updated_at
DROP TRIGGER IF EXISTS trigger_aux_gestiones_unificadas_updated_at ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas;
CREATE TRIGGER trigger_aux_gestiones_unificadas_updated_at
    BEFORE UPDATE ON aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column(); -- Reuse function from 003
