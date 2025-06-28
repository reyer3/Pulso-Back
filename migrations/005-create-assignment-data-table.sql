-- Assignment composition data by period and portfolio
-- depends: 001-create-watermarks-table

CREATE TABLE assignment_data (
    periodo VARCHAR(7) NOT NULL,
    archivo VARCHAR(100) NOT NULL,
    cartera VARCHAR(50) NOT NULL,
    clientes INTEGER NOT NULL DEFAULT 0,
    cuentas INTEGER NOT NULL DEFAULT 0,
    deuda_asig FLOAT NOT NULL DEFAULT 0.0,
    deuda_actual FLOAT NOT NULL DEFAULT 0.0,
    ticket_promedio FLOAT NOT NULL DEFAULT 0.0,
    fecha_procesamiento TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (periodo, archivo, cartera)
);

-- Indexes for performance
CREATE INDEX idx_assignment_data_periodo ON assignment_data(periodo);
CREATE INDEX idx_assignment_data_cartera ON assignment_data(cartera);

-- Comments for documentation
COMMENT ON TABLE assignment_data IS 'Assignment composition data by period and portfolio';
COMMENT ON COLUMN assignment_data.periodo IS 'Period in YYYY-MM format';
COMMENT ON COLUMN assignment_data.archivo IS 'Campaign file identifier';
COMMENT ON COLUMN assignment_data.cartera IS 'Portfolio type';
COMMENT ON COLUMN assignment_data.clientes IS 'Total clients in the assignment';
COMMENT ON COLUMN assignment_data.cuentas IS 'Total accounts in the assignment';
COMMENT ON COLUMN assignment_data.deuda_asig IS 'Total assigned debt';
COMMENT ON COLUMN assignment_data.deuda_actual IS 'Current debt amount';
COMMENT ON COLUMN assignment_data.ticket_promedio IS 'Average debt per account';
COMMENT ON COLUMN assignment_data.fecha_procesamiento IS 'Timestamp of when the data was processed';
