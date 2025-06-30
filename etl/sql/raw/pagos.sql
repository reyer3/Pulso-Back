-- Extracts payment transaction data.
-- Parameters:
-- {incremental_filter}: WHERE clause for incremental loading.

SELECT
    cod_sistema,
    nro_documento,
    monto_cancelado,
    fecha_pago,
    archivo,
    creado_el,
    motivo_rechazo,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
WHERE creado_el {incremental_filter}
  AND monto_cancelado > 0
  AND (motivo_rechazo IS NULL OR motivo_rechazo = '');