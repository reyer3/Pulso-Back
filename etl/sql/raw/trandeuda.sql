-- Extracts daily debt snapshots.
-- Parameters:
-- {incremental_filter}: WHERE clause for incremental loading.

SELECT
    cod_cuenta,
    nro_documento,
    fecha_vencimiento,
    monto_exigible,
    archivo,
    creado_el,
    DATE(creado_el) as fecha_proceso,
    motivo_rechazo,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
WHERE 1=1--creado_el {incremental_filter}
AND creado_el >'2025-06-27 07:04:00.000000'
  AND monto_exigible > 0
  AND (motivo_rechazo IS NULL OR motivo_rechazo = '');