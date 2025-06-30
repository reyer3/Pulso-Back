-- 004: Create raw table for Voicebot gestiones for project P3fV4dWNeMkN5RJMhV8e
-- depends: 003-create-raw-tables

-- Raw staging table for Voicebot interaction data from BigQuery
CREATE TABLE IF NOT EXISTS raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones (
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
    fecha_compromiso TIMESTAMPTZ,
    compromiso TEXT,
    observacion TEXT,
    project TEXT,
    client TEXT,
    duracion INTEGER,
    id_telephony TEXT,
    url_record_bot TEXT,
    -- ETL Metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(), -- Added created_at
    updated_at TIMESTAMPTZ DEFAULT NOW()  -- Added updated_at
);

COMMENT ON TABLE raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones IS 'Raw staging data for Voicebot interactions for project P3fV4dWNeMkN5RJMhV8e.';

-- Trigger for updated_at
DROP TRIGGER IF EXISTS trigger_voicebot_gestiones_updated_at ON raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones;
CREATE TRIGGER trigger_voicebot_gestiones_updated_at
    BEFORE UPDATE ON raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones
    FOR EACH ROW
    EXECUTE FUNCTION raw_P3fV4dWNeMkN5RJMhV8e.update_timestamp_column(); -- Reuse function from 003

-- It's not typically a hypertable unless 'date' is used for partitioning and high volume is expected.
-- If it needs to be a hypertable on "date":
-- SELECT create_hypertable('raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones', 'date', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 month');
-- CREATE INDEX IF NOT EXISTS idx_raw_voicebot_gestiones_date ON raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones("date" DESC);
CREATE INDEX IF NOT EXISTS idx_raw_voicebot_gestiones_document ON raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones(document);
CREATE INDEX IF NOT EXISTS idx_raw_voicebot_gestiones_campaign_id ON raw_P3fV4dWNeMkN5RJMhV8e.voicebot_gestiones(campaign_id);
