-- Extracts raw MibotAir interactions from the flat source table.
-- Parameters:
-- {project_id}, {dataset_id}: For dynamic table name construction.
-- {incremental_filter}: WHERE clause for incremental loading based on 'date' column.

SELECT
    uid, campaign_id, campaign_name, document, phone, date, management, sub_management,
    weight, origin, n1, n2, n3, observacion, extra, project, client, nombre_agente,
    correo_agente, duracion, monto_compromiso, fecha_compromiso, url,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `{project_id}.{dataset_id}.mibotair_P3fV4dWNeMkN5RJMhV8e`
WHERE DATE(date) {incremental_filter};