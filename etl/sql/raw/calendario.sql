-- Extracts campaign calendar data.
-- Parameters:
-- {incremental_filter}: WHERE clause for incremental loading.

SELECT
    ARCHIVO,
    TIPO_CARTERA,
    fecha_apertura,
    fecha_trandeuda,
    fecha_cierre,
    FECHA_CIERRE_PLANIFICADA,
    DURACION_CAMPANA_DIAS_HABILES,
    ANNO_ASIGNACION,
    PERIODO_ASIGNACION,
    ES_CARTERA_ABIERTA,
    RANGO_VENCIMIENTO,
    ESTADO_CARTERA,
    periodo_mes,
    periodo_date,
    tipo_ciclo_campana,
    categoria_duracion,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
WHERE fecha_apertura {incremental_filter};