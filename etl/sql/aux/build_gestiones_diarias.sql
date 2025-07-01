-- Aggregates daily gestiones metrics.
-- Parameters:
-- {aux_schema}: e.g., aux_P3fV4dWNeMkN5RJMhV8e
-- $1: campaign_archivo (TEXT)
-- $2: fecha_proceso (DATE)

INSERT INTO aux_p3fv4dwnemkn5rjmhv8e.gestiones_diarias (fecha_gestion, archivo_campana, cod_luna, total_gestiones,
                                                        total_contactos_efectivos, total_promesas)
SELECT gu.fecha_gestion,
       gu.archivo_campana,
       gu.cod_luna,
       COUNT(distinct cod_luna),
       SUM(CASE WHEN gu.es_contacto_efectivo THEN 1 ELSE 0 END),
       SUM(CASE WHEN gu.es_compromiso THEN 1 ELSE 0 END)
FROM aux_p3fv4dwnemkn5rjmhv8e.gestiones_unificadas gu
GROUP BY 1, 2, 3;