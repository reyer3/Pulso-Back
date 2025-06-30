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