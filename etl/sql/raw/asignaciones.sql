-- Extracts client assignment data.
-- Parameters:
-- {incremental_filter}: WHERE clause for incremental loading.

SELECT
    CAST(cliente AS STRING) as cliente,
    CAST(cuenta AS STRING) as cuenta,
    CAST(cod_luna AS STRING) as cod_luna,
    CAST(telefono AS STRING) as telefono,
    tramo_gestion,
    min_vto,
    negocio,
    dias_sin_trafico,
    decil_contacto,
    decil_pago,
    zona,
    rango_renta,
    campania_act,
    archivo,
    creado_el,
    DATE(creado_el) as fecha_asignacion,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
WHERE creado_el {incremental_filter};