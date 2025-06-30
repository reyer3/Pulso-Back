-- 005: Create raw table for MibotAir gestiones for project P3fV4dWNeMkN5RJMhV8e
-- depends: 004-create-raw-voicebot-gestiones-table

-- Raw staging table for MibotAir (human agent) interaction data from BigQuery
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones (
    uid TEXT PRIMARY KEY,
    campaign_id TEXT,
    campaign_name TEXT,
    document TEXT,
    phone NUMERIC,
    "date" TIMESTAMPTZ, -- "date" es una palabra reservada, usar comillas
    management TEXT,
    sub_management TEXT,
    weight INTEGER,
    origin TEXT,
    n1 TEXT,
    n2 TEXT,
    n3 TEXT,
    observacion TEXT,
    extra TEXT,
    project TEXT,
    client TEXT,
    nombre_agente TEXT,
    correo_agente TEXT,
    duracion INTEGER,
    monto_compromiso NUMERIC,
    fecha_compromiso DATE,
    url TEXT,
    -- ETL Metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(), -- Added created_at
    updated_at TIMESTAMPTZ DEFAULT NOW()  -- Added updated_at
);

COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones IS 'Raw staging data for human agent (MibotAir) interactions for project P3fV4dWNeMkN5RJMhV8e.';

-- Trigger for updated_at
DROP TRIGGER IF EXISTS trigger_mibotair_gestiones_updated_at ON raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones;
CREATE TRIGGER trigger_mibotair_gestiones_updated_at
    BEFORE UPDATE ON raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column(); -- Reuse function from 003

-- Similar to voicebot_gestiones, only make a hypertable if high volume and time-based queries on "date" are common.
-- If it needs to be a hypertable on "date":
-- SELECT create_hypertable('raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 month');
-- CREATE INDEX IF NOT EXISTS idx_raw_mibotair_gestiones_date ON raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones("date" DESC);
CREATE INDEX IF NOT EXISTS idx_raw_mibotair_gestiones_document ON raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones(document);
CREATE INDEX IF NOT EXISTS idx_raw_mibotair_gestiones_campaign_id ON raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones(campaign_id);
CREATE INDEX IF NOT EXISTS idx_raw_mibotair_gestiones_correo_agente ON raw_P3fV4dWNeMkN5RJMhV8e.mibotair_gestiones(correo_agente);
