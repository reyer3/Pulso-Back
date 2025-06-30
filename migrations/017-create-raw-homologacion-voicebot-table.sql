-- 017: Create raw_homologacion_voicebot table
-- Stores the homologation rules for Voicebot interactions.
CREATE TABLE IF NOT EXISTS raw_homologacion_voicebot (
    bot_management TEXT,
    bot_sub_management TEXT,
    bot_compromiso TEXT,
    n1_homologado TEXT,
    n2_homologado TEXT,
    n3_homologado TEXT,
    contactabilidad_homologada TEXT,
    es_pdp_homologado BOOLEAN, -- Usaremos BOOLEAN para mayor claridad
    peso_homologado INTEGER,
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Clave compuesta de las tipificaciones originales del bot
    PRIMARY KEY (bot_management, bot_sub_management, bot_compromiso)
);

COMMENT ON TABLE raw_homologacion_voicebot IS 'Staging table for homologation rules from Voicebot interactions.';
