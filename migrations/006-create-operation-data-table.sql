-- Hourly operational metrics by channel, optimized with TimescaleDB
-- depends: 001-create-watermarks-table

CREATE TABLE operation_data (
    fecha_foto DATE NOT NULL,
    hora INTEGER NOT NULL,
    canal VARCHAR(20) NOT NULL,
    archivo VARCHAR(100) NOT NULL DEFAULT 'GENERAL',
    total_gestiones INTEGER NOT NULL DEFAULT 0,
    contactos_efectivos INTEGER NOT NULL DEFAULT 0,
    contactos_no_efectivos INTEGER NOT NULL DEFAULT 0,
    total_pdp INTEGER NOT NULL DEFAULT 0,
    tasa_contacto FLOAT NOT NULL DEFAULT 0.0,
    tasa_conversion FLOAT NOT NULL DEFAULT 0.0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, hora, canal, archivo),
    CONSTRAINT chk_operation_hora_valid CHECK (hora >= 0 AND hora <= 23)
);

-- Indexes for performance
CREATE INDEX idx_operation_data_fecha_foto ON operation_data(fecha_foto DESC);
CREATE INDEX idx_operation_data_hora ON operation_data(hora);
CREATE INDEX idx_operation_data_canal ON operation_data(canal);
CREATE INDEX idx_operation_data_composite ON operation_data(fecha_foto, canal);

-- TimescaleDB hypertable for time-series data
SELECT create_hypertable('operation_data', by_range('fecha_foto', INTERVAL '1 day'), if_not_exists => TRUE);

-- Retention policy to manage data storage
SELECT add_retention_policy('operation_data', INTERVAL '1 year', if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE operation_data IS 'Hourly operational metrics by channel';
COMMENT ON COLUMN operation_data.fecha_foto IS 'Operation date';
COMMENT ON COLUMN operation_data.hora IS 'Hour of the day (0-23)';
COMMENT ON COLUMN operation_data.canal IS 'Channel (e.g., BOT, HUMANO)';
COMMENT ON COLUMN operation_data.archivo IS 'Campaign identifier, defaults to GENERAL';
COMMENT ON COLUMN operation_data.total_gestiones IS 'Total management actions';
COMMENT ON COLUMN operation_data.contactos_efectivos IS 'Number of effective contacts';
COMMENT ON COLUMN operation_data.contactos_no_efectivos IS 'Number of non-effective contacts';
COMMENT ON COLUMN operation_data.total_pdp IS 'Total Promises to Pay (PDP) made';
COMMENT ON COLUMN operation_data.tasa_contacto IS 'Contact rate percentage';
COMMENT ON COLUMN operation_data.tasa_conversion IS 'PDP conversion rate percentage';
COMMENT ON COLUMN operation_data.fecha_procesamiento IS 'Timestamp of when the data was processed';
