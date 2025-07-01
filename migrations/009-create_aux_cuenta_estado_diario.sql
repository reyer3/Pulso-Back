-- =====================================================
-- HYPERTABLE SIMPLIFICADA: Estado diario de cuentas
-- Regido por trandeuda - Solo lo esencial
-- =====================================================

CREATE TABLE IF NOT EXISTS aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario (
    -- === DIMENSIONES DE PARTICIONADO ===
    fecha_proceso DATE NOT NULL,                    -- Partitioning key para TimescaleDB
    archivo_campana TEXT NOT NULL,                  -- Campaña a la que pertenece
    cod_luna TEXT NOT NULL,                         -- Identificador único del cliente
    cuenta TEXT NOT NULL,                           -- Número de cuenta

    -- === CAMPOS BÁSICOS REQUERIDOS ===
    fecha_asignacion DATE,                          -- Cuándo se asignó la cuenta
    monto_exigible NUMERIC(15,2) DEFAULT 0,        -- Monto de deuda del día
    servicio varchar(20) NOT NULL CHECK ( servicio in('FIJA', 'MOVIL') ),
    fecha_vencimiento DATE,                         -- Vencimiento de la deuda
    motivo_no_gestionable TEXT,                     -- Por qué no es gestionable
    monto_saldo_actual NUMERIC(15,2) DEFAULT 0,    -- Saldo actual después de pagos

    -- === ESTADO DE DEUDA (solo los que indiques) ===
    estado_deuda VARCHAR(30),                       -- A definir por ti

    -- === TIMESTAMPS ===
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- === CONSTRAINTS ===
    PRIMARY KEY (fecha_proceso, archivo_campana, cod_luna, cuenta, servicio),

    CONSTRAINT chk_monto_exigible_positive
        CHECK (monto_exigible >= 0),
    CONSTRAINT chk_monto_saldo_positive
        CHECK (monto_saldo_actual >= 0)
);

-- =====================================================
-- CONFIGURACIÓN DE HYPERTABLE
-- =====================================================

-- Convertir a hypertable con particionado por fecha
SELECT create_hypertable(
    'aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario',
    'fecha_proceso',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- =====================================================
-- ÍNDICES ESENCIALES
-- =====================================================

-- Índice por cuenta y fecha
CREATE INDEX IF NOT EXISTS idx_ced_cuenta_fecha
ON aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario (cuenta, fecha_proceso DESC);

-- Índice por campaña y fecha
CREATE INDEX IF NOT EXISTS idx_ced_campana_fecha
ON aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario (archivo_campana, fecha_proceso DESC);

-- Índice por cod_luna
CREATE INDEX IF NOT EXISTS idx_ced_cod_luna
ON aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario (cod_luna, fecha_proceso DESC);

-- =====================================================
-- TRIGGER PARA UPDATED_AT
-- =====================================================

CREATE OR REPLACE FUNCTION aux_p3fv4dwnemkn5rjmhv8e.update_cuenta_estado_diario_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cuenta_estado_diario_updated_at
    BEFORE UPDATE ON aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario
    FOR EACH ROW
    EXECUTE FUNCTION aux_p3fv4dwnemkn5rjmhv8e.update_cuenta_estado_diario_updated_at();

-- =====================================================
-- POLÍTICA DE RETENCIÓN
-- =====================================================

SELECT add_retention_policy(
    'aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario',
    INTERVAL '2 years',
    if_not_exists => TRUE
);

-- =====================================================
-- COMENTARIOS
-- =====================================================

COMMENT ON TABLE aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario IS
'Hypertable simplificada que registra el estado diario de cuentas regido por trandeuda.
Fuente de datos para los marts del dashboard.';

COMMENT ON COLUMN aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario.fecha_proceso IS
'Fecha de proceso (partitioning key). Regida por la fecha en trandeuda.';

COMMENT ON COLUMN aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario.estado_deuda IS
'Estado específico de la deuda a definir según criterios del negocio.';

-- Asignar propietario
ALTER TABLE aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario OWNER TO pulso_sa;