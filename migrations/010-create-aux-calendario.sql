-- =====================================================
-- TABLA AUXILIAR: Calendario con días hábiles y gestión
-- Lunes a Viernes como días hábiles
-- =====================================================

CREATE TABLE IF NOT EXISTS aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles (
    fecha DATE PRIMARY KEY,

    -- === INFORMACIÓN BÁSICA DE LA FECHA ===
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,                    -- 1=Lunes, 7=Domingo
    nombre_dia_semana VARCHAR(10) NOT NULL,         -- 'Lunes', 'Martes', etc.

    -- === INDICADORES ===
    es_dia_habil BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE si es Lunes-Viernes
    es_fin_de_semana BOOLEAN NOT NULL DEFAULT FALSE, -- TRUE si es Sábado-Domingo

    -- === CONTADORES DE DÍAS HÁBILES ===
    dia_habil_del_mes INTEGER,                      -- Número de día hábil desde inicio del mes
    dia_habil_del_anio INTEGER,                     -- Número de día hábil desde inicio del año

    -- === INFORMACIÓN DEL MES ===
    es_primer_dia_habil_mes BOOLEAN DEFAULT FALSE,
    es_ultimo_dia_habil_mes BOOLEAN DEFAULT FALSE,
    total_dias_habiles_mes INTEGER,

    -- === TIMESTAMPS ===
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =====================================================
-- ÍNDICES PARA OPTIMIZAR CONSULTAS
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_calendario_habiles_anio_mes
ON aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles (anio, mes);

CREATE INDEX IF NOT EXISTS idx_calendario_habiles_es_habil
ON aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles (es_dia_habil, fecha);

CREATE INDEX IF NOT EXISTS idx_calendario_habiles_dia_semana
ON aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles (dia_semana, fecha);

-- =====================================================
-- TABLA AUXILIAR: Días de gestión por campaña
-- =====================================================

CREATE TABLE IF NOT EXISTS aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana (
    fecha DATE NOT NULL,
    archivo_campana TEXT NOT NULL,

    -- === INFORMACIÓN DE LA CAMPAÑA ===
    fecha_apertura DATE NOT NULL,
    fecha_cierre DATE,

    -- === CONTADORES DE GESTIÓN ===
    dia_campana INTEGER NOT NULL,                   -- Días desde apertura (incluye fines de semana)
    dia_gestion_campana INTEGER,                    -- Solo días hábiles desde apertura

    -- === INDICADORES ===
    es_dia_gestion BOOLEAN NOT NULL DEFAULT FALSE, -- TRUE si es día hábil y campaña activa
    es_primer_dia_gestion BOOLEAN DEFAULT FALSE,
    es_ultimo_dia_gestion BOOLEAN DEFAULT FALSE,

    -- === TIMESTAMPS ===
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (fecha, archivo_campana),

    -- === FOREIGN KEY ===
    CONSTRAINT fk_calendario_gestion_fecha
        FOREIGN KEY (fecha) REFERENCES aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles(fecha)
);

-- =====================================================
-- ÍNDICES PARA CALENDARIO GESTIÓN
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_cal_gestion_campana
ON aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana (archivo_campana, fecha);

CREATE INDEX IF NOT EXISTS idx_cal_gestion_es_dia_gestion
ON aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana (es_dia_gestion, fecha);

-- =====================================================
-- FUNCIÓN: Poblar calendario de días hábiles
-- =====================================================

CREATE OR REPLACE FUNCTION aux_p3fv4dwnemkn5rjmhv8e.poblar_calendario_habiles(
    fecha_inicio DATE DEFAULT '2020-01-01',
    fecha_fin DATE DEFAULT CURRENT_DATE + INTERVAL '2 years'
)
RETURNS VOID AS $FUNC$
DECLARE
    fecha_actual DATE;
    contador_habil_mes INTEGER;
    contador_habil_anio INTEGER;
    mes_anterior INTEGER;
    anio_anterior INTEGER;
BEGIN
    -- Inicializar contadores
    mes_anterior := 0;
    anio_anterior := 0;
    contador_habil_mes := 0;
    contador_habil_anio := 0;

    -- Limpiar datos existentes en el rango
    DELETE FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
    WHERE fecha BETWEEN fecha_inicio AND fecha_fin;

    -- Generar fechas día por día
    fecha_actual := fecha_inicio;

    WHILE fecha_actual <= fecha_fin LOOP
        -- Reiniciar contador mensual si cambió el mes
        IF EXTRACT(MONTH FROM fecha_actual) != mes_anterior THEN
            contador_habil_mes := 0;
            mes_anterior := EXTRACT(MONTH FROM fecha_actual);
        END IF;

        -- Reiniciar contador anual si cambió el año
        IF EXTRACT(YEAR FROM fecha_actual) != anio_anterior THEN
            contador_habil_anio := 0;
            anio_anterior := EXTRACT(YEAR FROM fecha_actual);
        END IF;

        -- Incrementar contadores si es día hábil (Lunes=1 a Viernes=5)
        IF EXTRACT(DOW FROM fecha_actual) BETWEEN 1 AND 5 THEN
            contador_habil_mes := contador_habil_mes + 1;
            contador_habil_anio := contador_habil_anio + 1;
        END IF;

        -- Insertar registro
        INSERT INTO aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles (
            fecha, anio, mes, dia, dia_semana, nombre_dia_semana,
            es_dia_habil, es_fin_de_semana,
            dia_habil_del_mes, dia_habil_del_anio
        ) VALUES (
            fecha_actual,
            EXTRACT(YEAR FROM fecha_actual),
            EXTRACT(MONTH FROM fecha_actual),
            EXTRACT(DAY FROM fecha_actual),
            EXTRACT(DOW FROM fecha_actual),
            CASE EXTRACT(DOW FROM fecha_actual)
                WHEN 1 THEN 'Lunes'
                WHEN 2 THEN 'Martes'
                WHEN 3 THEN 'Miércoles'
                WHEN 4 THEN 'Jueves'
                WHEN 5 THEN 'Viernes'
                WHEN 6 THEN 'Sábado'
                WHEN 0 THEN 'Domingo'
            END,
            EXTRACT(DOW FROM fecha_actual) BETWEEN 1 AND 5, -- es_dia_habil
            EXTRACT(DOW FROM fecha_actual) IN (0, 6),       -- es_fin_de_semana
            CASE WHEN EXTRACT(DOW FROM fecha_actual) BETWEEN 1 AND 5
                 THEN contador_habil_mes ELSE NULL END,     -- dia_habil_del_mes
            CASE WHEN EXTRACT(DOW FROM fecha_actual) BETWEEN 1 AND 5
                 THEN contador_habil_anio ELSE NULL END     -- dia_habil_del_anio
        );

        fecha_actual := fecha_actual + 1;
    END LOOP;

    -- Actualizar indicadores de primer y último día hábil del mes
    UPDATE aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
    SET es_primer_dia_habil_mes = TRUE
    WHERE (anio, mes, dia_habil_del_mes) IN (
        SELECT anio, mes, MIN(dia_habil_del_mes)
        FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
        WHERE es_dia_habil = TRUE
        GROUP BY anio, mes
    );

    UPDATE aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
    SET es_ultimo_dia_habil_mes = TRUE
    WHERE (anio, mes, dia_habil_del_mes) IN (
        SELECT anio, mes, MAX(dia_habil_del_mes)
        FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
        WHERE es_dia_habil = TRUE
        GROUP BY anio, mes
    );

    -- Actualizar total de días hábiles por mes
    UPDATE aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles c1
    SET total_dias_habiles_mes = (
        SELECT COUNT(*)
        FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles c2
        WHERE c2.anio = c1.anio
        AND c2.mes = c1.mes
        AND c2.es_dia_habil = TRUE
    );

    RAISE NOTICE 'Calendario poblado desde % hasta %. Total registros: %',
                 fecha_inicio, fecha_fin,
                 (SELECT COUNT(*) FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
                  WHERE fecha BETWEEN fecha_inicio AND fecha_fin);
END;
$FUNC$ LANGUAGE plpgsql;

-- =====================================================
-- FUNCIÓN: Poblar días de gestión por campaña
-- =====================================================

CREATE OR REPLACE FUNCTION aux_p3fv4dwnemkn5rjmhv8e.poblar_calendario_gestion()
RETURNS VOID AS $
BEGIN
    -- Limpiar datos existentes
    TRUNCATE aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana;

    -- Poblar con datos de campañas
    WITH campanas_fechas AS (
        SELECT
            c.archivo,
            c.fecha_apertura,
            c.fecha_cierre,
            ch.fecha,
            ch.es_dia_habil,
            -- Contador de días desde apertura
            (ch.fecha - c.fecha_apertura + 1) AS dia_campana
        FROM raw_p3fv4dwnemkn5rjmhv8e.calendario c
        INNER JOIN aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles ch
            ON ch.fecha BETWEEN c.fecha_apertura
            AND COALESCE(c.fecha_cierre, CURRENT_DATE)
    ),

    dias_gestion_numerados AS (
        SELECT
            archivo,
            fecha_apertura,
            fecha_cierre,
            fecha,
            es_dia_habil,
            dia_campana,
            -- Contador de días hábiles desde apertura usando ROW_NUMBER solo para días hábiles
            CASE
                WHEN es_dia_habil = TRUE THEN
                    ROW_NUMBER() OVER (
                        PARTITION BY archivo
                        ORDER BY fecha
                    )
                ELSE NULL
            END AS dia_gestion_campana
        FROM campanas_fechas
        WHERE es_dia_habil = TRUE

        UNION ALL

        -- Incluir también los fines de semana pero sin número de gestión
        SELECT
            archivo,
            fecha_apertura,
            fecha_cierre,
            fecha,
            es_dia_habil,
            dia_campana,
            NULL AS dia_gestion_campana
        FROM campanas_fechas
        WHERE es_dia_habil = FALSE
    )

    INSERT INTO aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana (
        fecha, archivo_campana, fecha_apertura, fecha_cierre,
        dia_campana, dia_gestion_campana, es_dia_gestion,
        es_primer_dia_gestion, es_ultimo_dia_gestion
    )
    SELECT
        fecha,
        archivo,
        fecha_apertura,
        fecha_cierre,
        dia_campana,
        dia_gestion_campana,
        es_dia_habil AS es_dia_gestion,
        (dia_gestion_campana = 1) AS es_primer_dia_gestion,
        FALSE AS es_ultimo_dia_gestion  -- Se actualiza después
    FROM dias_gestion_numerados
    ORDER BY archivo, fecha;

    -- Marcar último día de gestión por campaña
    UPDATE aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana c1
    SET es_ultimo_dia_gestion = TRUE
    WHERE c1.es_dia_gestion = TRUE
    AND c1.dia_gestion_campana = (
        SELECT MAX(c2.dia_gestion_campana)
        FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana c2
        WHERE c2.archivo_campana = c1.archivo_campana
        AND c2.es_dia_gestion = TRUE
    );

    RAISE NOTICE 'Calendario de gestión poblado. Total registros: %',
                 (SELECT COUNT(*) FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana);
END;
$ LANGUAGE plpgsql;

-- =====================================================
-- POBLAR CALENDARIOS (EJECUTAR INICIAL)
-- =====================================================

-- Poblar calendario base (ajustar fechas según necesidad)
SELECT aux_p3fv4dwnemkn5rjmhv8e.poblar_calendario_habiles('2024-01-01', '2026-12-31');

-- Poblar calendario de gestión por campaña
SELECT aux_p3fv4dwnemkn5rjmhv8e.poblar_calendario_gestion();

-- =====================================================
-- VISTA AUXILIAR: Resumen por mes
-- =====================================================

CREATE OR REPLACE VIEW aux_p3fv4dwnemkn5rjmhv8e.v_resumen_dias_habiles_mes AS
SELECT
    anio,
    mes,
    MIN(fecha) AS primer_dia_mes,
    MAX(fecha) AS ultimo_dia_mes,
    COUNT(*) AS total_dias_mes,
    COUNT(CASE WHEN es_dia_habil THEN 1 END) AS total_dias_habiles_mes,
    COUNT(CASE WHEN es_fin_de_semana THEN 1 END) AS total_dias_fin_semana,
    MIN(CASE WHEN es_dia_habil THEN fecha END) AS primer_dia_habil_mes,
    MAX(CASE WHEN es_dia_habil THEN fecha END) AS ultimo_dia_habil_mes
FROM aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles
GROUP BY anio, mes
ORDER BY anio, mes;

-- =====================================================
-- COMENTARIOS
-- =====================================================

COMMENT ON TABLE aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles IS
'Calendario auxiliar con información de días hábiles (Lunes-Viernes).
Incluye contadores por mes y año para análisis temporal.';

COMMENT ON TABLE aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana IS
'Calendario de días de gestión por campaña.
Relaciona cada fecha con el día de gestión correspondiente desde apertura.';

-- Asignar propietarios
ALTER TABLE aux_p3fv4dwnemkn5rjmhv8e.calendario_habiles OWNER TO pulso_sa;
ALTER TABLE aux_p3fv4dwnemkn5rjmhv8e.calendario_gestion_campana OWNER TO pulso_sa;