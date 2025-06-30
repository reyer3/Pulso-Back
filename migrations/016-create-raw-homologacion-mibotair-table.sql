-- 016: Create raw_homologacion_mibotair table
-- Stores the homologation rules for human agent (MibotAir) interactions.
CREATE TABLE IF NOT EXISTS raw_homologacion_mibotair (
    management TEXT,
    n_1 TEXT,
    n_2 TEXT,
    n_3 TEXT,
    peso INTEGER, -- Asumiendo que se convertirá a numérico en el ETL
    contactabilidad TEXT,
    tipo_gestion TEXT,
    codigo_rpta TEXT,
    pdp TEXT,
    gestor TEXT,
    -- ETL metadata
    extraction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Usamos una clave compuesta de los niveles de tipificación
    PRIMARY KEY (n_1, n_2, n_3)
);

COMMENT ON TABLE raw_homologacion_mibotair IS 'Staging table for homologation rules from MibotAir (human agents).';
