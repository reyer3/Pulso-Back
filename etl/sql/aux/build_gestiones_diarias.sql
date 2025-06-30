-- Aggregates daily gestiones metrics.
-- Parameters:
-- {aux_schema}: e.g., aux_P3fV4dWNeMkN5RJMhV8e
-- $1: campaign_archivo (TEXT)
-- $2: fecha_proceso (DATE)

DELETE FROM {aux_schema}.gestiones_diarias WHERE archivo_campana = $1 AND fecha_gestion = $2;

INSERT INTO {aux_schema}.gestiones_diarias (fecha_gestion, archivo_campana, cod_luna, total_gestiones, total_contactos_efectivos, total_promesas)
SELECT fecha_gestion, archivo_campana, cod_luna, COUNT(*), SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END), SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END)
FROM {aux_schema}.gestiones_unificadas
WHERE archivo_campana = $1 AND fecha_gestion = $2
GROUP BY 1, 2, 3;