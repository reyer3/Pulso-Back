-- Daily productivity metrics by agent, optimized with TimescaleDB
-- depends: 001-create-watermarks-table

CREATE TABLE productivity_data (
    fecha_foto DATE NOT NULL,
    correo_agente VARCHAR(100) NOT NULL,
    archivo VARCHAR(100) NOT NULL DEFAULT 'GENERAL',
    total_gestiones INTEGER NOT NULL DEFAULT 0,
    contactos_efectivos INTEGER NOT NULL DEFAULT 0,
    total_pdp INTEGER NOT NULL DEFAULT 0,
    peso_total FLOAT NOT NULL DEFAULT 0.0,
    tasa_contacto FLOAT NOT NULL DEFAULT 0.0,
    tasa_conversion FLOAT NOT NULL DEFAULT 0.0,
    score_productividad FLOAT NOT NULL DEFAULT 0.0,
    nombre_agente VARCHAR(100),
    dni_agente VARCHAR(20),
    equipo VARCHAR(50),
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, correo_agente, archivo)
);

-- Indexes for performance
CREATE INDEX idx_productivity_data_fecha_foto ON productivity_data(fecha_foto DESC);
CREATE INDEX idx_productivity_data_agente ON productivity_data(correo_agente);
CREATE INDEX idx_productivity_data_equipo ON productivity_data(equipo);
CREATE INDEX idx_productivity_data_composite ON productivity_data(fecha_foto, equipo);

-- TimescaleDB hypertable for time-series data
SELECT create_hypertable('productivity_data', by_range('fecha_foto', INTERVAL '7 days'), if_not_exists => TRUE);

-- Retention policy to manage data storage
SELECT add_retention_policy('productivity_data', INTERVAL '2 years', if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE productivity_data IS 'Daily productivity metrics by agent';
COMMENT ON COLUMN productivity_data.fecha_foto IS 'Performance date';
COMMENT ON COLUMN productivity_data.correo_agente IS 'Agent email address';
COMMENT ON COLUMN productivity_data.archivo IS 'Campaign identifier, defaults to GENERAL';
COMMENT ON COLUMN productivity_data.total_gestiones IS 'Total management actions by the agent';
COMMENT ON COLUMN productivity_data.contactos_efectivos IS 'Effective contacts made by the agent';
COMMENT ON COLUMN productivity_data.total_pdp IS 'Total Promises to Pay (PDP) secured by the agent';
COMMENT ON COLUMN productivity_data.peso_total IS 'Total weight or score for actions';
COMMENT ON COLUMN productivity_data.tasa_contacto IS 'Agent-specific contact rate';
COMMENT ON COLUMN productivity_data.tasa_conversion IS 'Agent-specific PDP conversion rate';
COMMENT ON COLUMN productivity_data.score_productividad IS 'Overall productivity score for the agent';
COMMENT ON COLUMN productivity_data.nombre_agente IS 'Agent full name (denormalized)';
COMMENT ON COLUMN productivity_data.dni_agente IS 'Agent DNI (denormalized)';
COMMENT ON COLUMN productivity_data.equipo IS 'Agent team name (denormalized)';
COMMENT ON COLUMN productivity_data.fecha_procesamiento IS 'Timestamp of when the data was processed';
