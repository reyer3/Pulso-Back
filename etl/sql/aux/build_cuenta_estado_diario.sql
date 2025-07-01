-- =====================================================
-- UPSERT DIARIO: cuenta_estado_diario
-- Genera registros para TODAS las fechas entre apertura y cierre
-- =====================================================

WITH fechas_campana AS (
    -- Generar todas las fechas entre apertura y cierre de cada campaña
    SELECT c.archivo,
           c.fecha_apertura,
           c.fecha_cierre,
           fecha_serie AS fecha_proceso
    FROM raw_p3fv4dwnemkn5rjmhv8e.calendario c
             CROSS JOIN LATERAL generate_series(
            c.fecha_apertura,
            COALESCE(c.fecha_cierre, CURRENT_DATE),
            INTERVAL '1 day'
                                ) AS fecha_serie),

     cuentas_por_fecha AS (
         -- Combinar cada cuenta con cada fecha de su campaña
         SELECT fc.fecha_proceso,
                a.archivo AS                                                    archivo_campana,
                a.cod_luna,
                a.cuenta,
                a.fecha_asignacion,
                a.min_vto,
                CASE UPPER(a.negocio) WHEN 'MOVIL' THEN 'MOVIL' ELSE 'FIJA' END servicio
         FROM raw_p3fv4dwnemkn5rjmhv8e.asignaciones a
                  INNER JOIN fechas_campana fc
                             ON a.archivo = CONCAT(fc.archivo, '.txt')
                                 AND fc.fecha_proceso >= a.fecha_asignacion -- Solo desde que se asignó
     ),

     estado_cuentas_dia AS (SELECT cpf.fecha_proceso,
                                   cpf.archivo_campana,
                                   cpf.cod_luna,
                                   cpf.cuenta,
                                   cpf.fecha_asignacion,
                                   cpf.servicio,


                                   -- Datos de trandeuda del día (puede ser NULL)
                                   COALESCE(t.monto_exigible, 0)       AS monto_exigible,
                                   cpf.min_vto,

                                   -- Lógica de gestionabilidad
                                   CASE
                                       WHEN t.cod_cuenta IS NULL THEN 'No está en trandeuda'
                                       WHEN t.monto_exigible < 1 THEN 'Deuda menor a 1 sol'
                                       ELSE 'Gestionable' -- Es gestionable
                                       END                             AS motivo_no_gestionable,

                                   -- Cálculo de saldo actual (deuda - pagos hasta la fecha)
                                   COALESCE(t.monto_exigible, 0) -
                                   COALESCE(SUM(p.monto_cancelado), 0) AS monto_saldo_actual,

                                   -- Estado de deuda
                                   CASE
                                       WHEN t.cod_cuenta IS NULL THEN 'SIN_TRANDEUDA'
                                       WHEN t.monto_exigible < 1 THEN 'DEUDA_MINIMA'
                                       WHEN t.monto_exigible >= 1 THEN 'CON_DEUDA'
                                       END                             AS estado_deuda

                            FROM cuentas_por_fecha cpf
                                     LEFT JOIN raw_p3fv4dwnemkn5rjmhv8e.trandeuda t
                                               ON cpf.cuenta = t.cod_cuenta
                                                   AND t.fecha_proceso = cpf.fecha_proceso
                                     LEFT JOIN aux_p3fv4dwnemkn5rjmhv8e.pago_deduplication p
                                               ON cpf.cuenta = p.cuenta
                                                   AND cpf.archivo_campana = p.archivo_campana
                                                   AND p.fecha_pago <= cpf.fecha_proceso
                                                   AND p.es_pago_valido = TRUE

                            GROUP BY cpf.fecha_proceso, cpf.archivo_campana, cpf.cod_luna, cpf.cuenta,
                                     cpf.fecha_asignacion, t.monto_exigible, min_vto, cpf.servicio, cpf.archivo_campana, cpf.cod_luna, cpf.cuenta, cpf.fecha_asignacion, cpf.fecha_proceso, COALESCE(t.monto_exigible, 0), min_vto, CASE
                                       WHEN t.cod_cuenta IS NULL THEN 'No está en trandeuda'
                                       WHEN t.monto_exigible < 1 THEN 'Deuda menor a 1 sol'
                                       ELSE 'Gestionable' -- Es gestionable
                                       END, CASE
                                       WHEN t.cod_cuenta IS NULL THEN 'SIN_TRANDEUDA'
                                       WHEN t.monto_exigible < 1 THEN 'DEUDA_MINIMA'
                                       WHEN t.monto_exigible >= 1 THEN 'CON_DEUDA'
                                       END, t.cod_cuenta)

-- UPSERT simplificado - Eliminando duplicados
INSERT
INTO aux_p3fv4dwnemkn5rjmhv8e.cuenta_estado_diario (fecha_proceso,
                                                    archivo_campana,
                                                    cod_luna,
                                                    cuenta,
                                                    fecha_asignacion,
                                                    monto_exigible,
                                                    servicio,
                                                    fecha_vencimiento,
                                                    motivo_no_gestionable,
                                                    monto_saldo_actual,
                                                    estado_deuda,
                                                    created_at,
                                                    updated_at)
SELECT DISTINCT ON (fecha_proceso, archivo_campana, cod_luna, cuenta) fecha_proceso,
                                                                      archivo_campana,
                                                                      cod_luna,
                                                                      cuenta,
                                                                      fecha_asignacion,
                                                                      monto_exigible,
                                                                      servicio,
                                                                      min_vto,
                                                                      motivo_no_gestionable,
                                                                      monto_saldo_actual,
                                                                      estado_deuda,
                                                                      NOW(),
                                                                      NOW()
FROM estado_cuentas_dia
ORDER BY fecha_proceso, archivo_campana, cod_luna, cuenta, monto_exigible
    DESC -- Prioriza el mayor monto si hay duplicados

-- Si existe, actualizar
ON CONFLICT (fecha_proceso, archivo_campana, cod_luna, cuenta, servicio)
    DO UPDATE SET monto_exigible        = EXCLUDED.monto_exigible,
                  fecha_vencimiento     = EXCLUDED.fecha_vencimiento,
                  motivo_no_gestionable = EXCLUDED.motivo_no_gestionable,
                  monto_saldo_actual    = EXCLUDED.monto_saldo_actual,
                  estado_deuda          = EXCLUDED.estado_deuda,
                  updated_at            = NOW();