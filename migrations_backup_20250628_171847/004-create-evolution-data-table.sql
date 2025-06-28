-- Time series data for evolution analysis, optimized with TimescaleDB
-- depends: 001-create-watermarks-table
-- depends: 000-enable-timescaledb

CREATE TABLE evolution_data (
    fecha_foto DATE NOT NULL,
    archivo VARCHAR(100) NOT NULL,
    cartera VARCHAR(50) NOT NULL,
    servicio VARCHAR(20) NOT NULL,
    pct_cober FLOAT NOT NULL DEFAULT 0.0,
    pct_contac FLOAT NOT NULL DEFAULT 0.0,
    pct_efectividad FLOAT NOT NULL DEFAULT 0.0,
    pct_cierre FLOAT NOT NULL DEFAULT 0.0,
    recupero FLOAT NOT NULL DEFAULT 0.0,
    cuentas INTEGER NOT NULL DEFAULT 0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, archivo)
);

-- Indexes for performance
CREATE INDEX idx_evolution_data_fecha_foto ON evolution_data(fecha_foto DESC);
CREATE INDEX idx_evolution_data_cartera ON evolution_data(cartera);
CREATE INDEX idx_evolution_data_composite ON evolution_data(fecha_foto, cartera);

-- TimescaleDB hypertable for time-series data
SELECT create_hypertable('evolution_data', by_range('fecha_foto', INTERVAL '7 days'), if_not_exists => TRUE);

-- Retention policy to manage data storage
SELECT add_retention_policy('evolution_data', INTERVAL '2 years', if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE evolution_data IS 'Time series data for evolution analysis';
COMMENT ON COLUMN evolution_data.fecha_foto IS 'Snapshot date for the metrics';
COMMENT ON COLUMN evolution_data.archivo IS 'Campaign file identifier';
COMMENT ON COLUMN evolution_data.cartera IS 'Portfolio type';
COMMENT ON COLUMN evolution_data.servicio IS 'Service type';
COMMENT ON COLUMN evolution_data.pct_cober IS 'Coverage percentage over time';
COMMENT ON COLUMN evolution_data.pct_contac IS 'Contact percentage over time';
COMMENT ON COLUMN evolution_data.pct_efectividad IS 'Effectiveness percentage over time';
COMMENT ON COLUMN evolution_data.pct_cierre IS 'Closure percentage over time';
COMMENT ON COLUMN evolution_data.recupero IS 'Recovered amount over time';
COMMENT ON COLUMN evolution_data.cuentas IS 'Total accounts over time';
COMMENT ON COLUMN evolution_data.fecha_procesamiento IS 'Timestamp of when the data was processed';
