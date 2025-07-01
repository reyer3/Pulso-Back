-- UPSERT en tabla de deduplicación de pagos
-- Corregido y mejorado para PostgreSQL con ventana basada en calendario
WITH pagos_preparados AS (
    SELECT
        p.archivo AS archivo_campana,
        t.cod_cuenta AS cuenta,
        p.nro_documento,
        a.cod_luna,
        p.fecha_pago::DATE AS fecha_pago,
        p.monto_cancelado::NUMERIC(15,2) AS monto_cancelado,

        -- Fechas del calendario para determinar ventana
        c.fecha_apertura,
        c.fecha_cierre,

        -- Cálculo de si está en ventana basado en calendario
        (p.fecha_pago::DATE BETWEEN c.fecha_apertura
         AND COALESCE(c.fecha_cierre, CURRENT_DATE)) AS esta_en_ventana_calendario,

        -- Cálculo mejorado de pago único por cuenta/documento/fecha
        (COUNT(*) OVER (
            PARTITION BY t.cod_cuenta, p.nro_documento, p.fecha_pago::DATE
        ) = 1) AS es_pago_unico_calculado,

        -- Información adicional útil para validaciones
        COUNT(*) OVER (
            PARTITION BY t.cod_cuenta, p.nro_documento, p.fecha_pago::DATE
        ) AS total_pagos_mismo_dia,

        -- Ranking para identificar el primer pago del día (en caso de múltiples)
        ROW_NUMBER() OVER (
            PARTITION BY t.cod_cuenta, p.nro_documento, p.fecha_pago::DATE
            ORDER BY p.monto_cancelado DESC, p.created_at
        ) AS ranking_pago_dia

    FROM raw_p3fv4dwnemkn5rjmhv8e.pagos p
    INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.trandeuda t
        ON p.nro_documento = t.nro_documento
    INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.asignaciones a
        ON t.cod_cuenta = a.cuenta
        AND p.fecha_pago::DATE = t.fecha_proceso::DATE
    INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.calendario c
        ON a.archivo = CONCAT(c.archivo, '.txt')

    WHERE
        p.monto_cancelado IS NOT NULL
        AND p.fecha_pago IS NOT NULL
        AND p.nro_documento IS NOT NULL
)

-- INSERT con UPSERT - Eliminando duplicados
INSERT INTO aux_p3fv4dwnemkn5rjmhv8e.pago_deduplication (
    archivo_campana,
    cuenta,
    nro_documento,
    fecha_pago,
    monto_cancelado,
    es_pago_unico,
    fecha_primera_carga,
    fecha_ultima_carga,
    veces_visto,
    esta_en_ventana,
    cod_luna,
    es_pago_valido,
    motivo_rechazo,
    created_at,
    updated_at
)
SELECT DISTINCT ON (p.nro_documento, p.fecha_pago, p.monto_cancelado)
    p.archivo_campana,
    p.cuenta,
    p.nro_documento,
    p.fecha_pago,
    p.monto_cancelado,
    p.es_pago_unico_calculado,

    -- Campos de control
    CURRENT_DATE AS fecha_primera_carga,
    CURRENT_DATE AS fecha_ultima_carga,
    1 AS veces_visto,

    -- Lógica de negocio mejorada basada en calendario
    p.esta_en_ventana_calendario AS esta_en_ventana,
    p.cod_luna,

    -- Validación robusta del pago
    CASE
        WHEN p.monto_cancelado <= 0 THEN FALSE
        WHEN p.fecha_pago > CURRENT_DATE THEN FALSE
        WHEN p.total_pagos_mismo_dia > 3 THEN FALSE  -- Sospechoso si hay más de 3 pagos el mismo día
        ELSE TRUE
    END AS es_pago_valido,

    -- Motivo de rechazo detallado
    CASE
        WHEN p.monto_cancelado <= 0 THEN 'Monto de pago no positivo'
        WHEN p.fecha_pago > CURRENT_DATE THEN 'Fecha de pago futura'
        WHEN p.total_pagos_mismo_dia > 3 THEN 'Demasiados pagos en un día'
    END AS motivo_rechazo,

    -- Timestamps
    NOW() AS created_at,
    NOW() AS updated_at

FROM pagos_preparados p
ORDER BY p.nro_documento, p.fecha_pago, p.monto_cancelado,
         p.ranking_pago_dia  -- Prioriza el ranking más alto (primer pago del día)

-- UPSERT: Si existe, actualizar
ON CONFLICT ( nro_documento, fecha_pago, monto_cancelado)
DO UPDATE SET
    -- Actualizar campos que pueden cambiar
    archivo_campana = EXCLUDED.archivo_campana,
    monto_cancelado = EXCLUDED.monto_cancelado,
    es_pago_unico = EXCLUDED.es_pago_unico,

    -- Actualizar campos de control
    fecha_ultima_carga = CURRENT_DATE,
    veces_visto = pago_deduplication.veces_visto + 1,

    -- Re-evaluar lógica de negocio basada en calendario
    esta_en_ventana = EXCLUDED.esta_en_ventana,
    es_pago_valido = EXCLUDED.es_pago_valido,
    motivo_rechazo = EXCLUDED.motivo_rechazo,

    -- Timestamp de actualización
    updated_at = NOW()

-- Solo procesar registros válidos
WHERE EXCLUDED.es_pago_valido = TRUE OR pago_deduplication.es_pago_valido = FALSE;