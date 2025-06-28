-- Main dashboard metrics table, optimized for time-series analysis with TimescaleDB
-- depends: 001-create-watermarks-table

CREATE TABLE dashboard_data (
    fecha_foto DATE NOT NULL,
    archivo VARCHAR(100) NOT NULL,
    cartera VARCHAR(50) NOT NULL,
    servicio VARCHAR(20) NOT NULL,
    cuentas INTEGER NOT NULL DEFAULT 0,
    clientes INTEGER NOT NULL DEFAULT 0,
    deuda_asig FLOAT NOT NULL DEFAULT 0.0,
    deuda_act FLOAT NOT NULL DEFAULT 0.0,
    cuentas_gestionadas INTEGER NOT NULL DEFAULT 0,
    cuentas_cd INTEGER NOT NULL DEFAULT 0,
    cuentas_ci INTEGER NOT NULL DEFAULT 0,
    cuentas_sc INTEGER NOT NULL DEFAULT 0,
    cuentas_sg INTEGER NOT NULL DEFAULT 0,
    cuentas_pdp INTEGER NOT NULL DEFAULT 0,
    recupero FLOAT NOT NULL DEFAULT 0.0,
    pct_cober FLOAT NOT NULL DEFAULT 0.0,
    pct_contac FLOAT NOT NULL DEFAULT 0.0,
    pct_cd FLOAT NOT NULL DEFAULT 0.0,
    pct_ci FLOAT NOT NULL DEFAULT 0.0,
    pct_conversion FLOAT NOT NULL DEFAULT 0.0,
    pct_efectividad FLOAT NOT NULL DEFAULT 0.0,
    pct_cierre FLOAT NOT NULL DEFAULT 0.0,
    inten FLOAT NOT NULL DEFAULT 0.0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_foto, archivo, cartera, servicio),
    CONSTRAINT chk_dashboard_cuentas_positive CHECK (cuentas >= 0),
    CONSTRAINT chk_dashboard_deuda_positive CHECK (deuda_asig >= 0)
);

-- Indexes for performance
CREATE INDEX idx_dashboard_data_fecha_foto ON dashboard_data(fecha_foto DESC);
CREATE INDEX idx_dashboard_data_cartera ON dashboard_data(cartera);
CREATE INDEX idx_dashboard_data_servicio ON dashboard_data(servicio);
CREATE INDEX idx_dashboard_data_procesamiento ON dashboard_data(fecha_procesamiento);

-- TimescaleDB hypertable for time-series data
SELECT create_hypertable('dashboard_data', by_range('fecha_foto', INTERVAL '7 days'), if_not_exists => TRUE);

-- Retention policy to manage data storage
SELECT add_retention_policy('dashboard_data', INTERVAL '2 years', if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE dashboard_data IS 'Main dashboard metrics by date, campaign, portfolio and service';
COMMENT ON COLUMN dashboard_data.fecha_foto IS 'Snapshot date for the metrics';
COMMENT ON COLUMN dashboard_data.archivo IS 'Campaign file identifier';
COMMENT ON COLUMN dashboard_data.cartera IS 'Portfolio type';
COMMENT ON COLUMN dashboard_data.servicio IS 'Service type (e.g., MOVIL, FIJA)';
COMMENT ON COLUMN dashboard_data.cuentas IS 'Total accounts';
COMMENT ON COLUMN dashboard_data.clientes IS 'Total clients';
COMMENT ON COLUMN dashboard_data.deuda_asig IS 'Assigned debt amount';
COMMENT ON COLUMN dashboard_data.deuda_act IS 'Current debt amount';
COMMENT ON COLUMN dashboard_data.cuentas_gestionadas IS 'Number of managed accounts';
COMMENT ON COLUMN dashboard_data.cuentas_cd IS 'Accounts with direct contact';
COMMENT ON COLUMN dashboard_data.cuentas_ci IS 'Accounts with indirect contact';
COMMENT ON COLUMN dashboard_data.cuentas_sc IS 'Accounts with no contact';
COMMENT ON COLUMN dashboard_data.cuentas_sg IS 'Accounts with no management actions';
COMMENT ON COLUMN dashboard_data.cuentas_pdp IS 'Accounts with a Promise to Pay (PDP)';
COMMENT ON COLUMN dashboard_data.recupero IS 'Total amount recovered';
COMMENT ON COLUMN dashboard_data.pct_cober IS 'Coverage percentage';
COMMENT ON COLUMN dashboard_data.pct_contac IS 'Contact percentage';
COMMENT ON COLUMN dashboard_data.pct_cd IS 'Direct contact percentage';
COMMENT ON COLUMN dashboard_data.pct_ci IS 'Indirect contact percentage';
COMMENT ON COLUMN dashboard_data.pct_conversion IS 'PDP conversion percentage';
COMMENT ON COLUMN dashboard_data.pct_efectividad IS 'Effectiveness percentage';
COMMENT ON COLUMN dashboard_data.pct_cierre IS 'Closure percentage';
COMMENT ON COLUMN dashboard_data.inten IS 'Management intensity';
COMMENT ON COLUMN dashboard_data.fecha_procesamiento IS 'Timestamp of when the data was processed';
