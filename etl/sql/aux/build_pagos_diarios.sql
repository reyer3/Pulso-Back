-- Builds a daily summary of validated, unique payments.
-- Parameters:
-- {aux_schema}: e.g., aux_P3fV4dWNeMkN5RJMhV8e
-- $1: campaign_archivo (TEXT)
-- $2: fecha_proceso (DATE)

-- Idempotency: Clean previous data for this day and campaign
DELETE FROM {aux_schema}.pagos_diarios
WHERE archivo_campana = $1 AND fecha_pago = $2;

INSERT INTO {aux_schema}.pagos_diarios (
    fecha_pago,
    archivo_campana,
    cod_luna,
    monto_pagado
)
SELECT
    p.fecha_pago,
    p.archivo_campana,
    p.cod_luna,
    SUM(p.monto_cancelado) as monto_pagado
FROM {aux_schema}.pago_deduplication p
WHERE
    p.archivo_campana = $1
    AND p.fecha_pago = $2
    AND p.es_pago_unico = TRUE
    AND p.es_pago_valido = TRUE
GROUP BY 1, 2, 3;
-- etl/sql/aux/build_pagos_diarios.sql
-- Construye agregaciones diarias de pagos por campaña

INSERT INTO {aux}.pagos_diarios (
    archivo,
    fecha,
    total_pagos,
    monto_total,
    promedio_pago,
    pagos_unicos_deudores,
    created_at
)
SELECT 
    archivo,
    DATE(creado_el) as fecha,
    COUNT(*) as total_pagos,
    SUM(monto_cancelado) as monto_total,
    AVG(monto_cancelado) as promedio_pago,
    COUNT(DISTINCT nro_documento) as pagos_unicos_deudores,
    CURRENT_TIMESTAMP as created_at
FROM {raw}.pagos
WHERE archivo = '{campaign_archivo}'
    AND DATE(creado_el) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
GROUP BY 
    archivo,
    DATE(creado_el)
ORDER BY 
    archivo,
    fecha;