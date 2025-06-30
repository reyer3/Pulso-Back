-- Extracts raw Voicebot interactions from the flat source table.
-- Parameters:
-- {project_id}, {dataset_id}: For dynamic table name construction.
-- {incremental_filter}: WHERE clause for incremental loading based on 'date' column.

SELECT
    uid, campaign_id, campaign_name, document, phone, date, management, sub_management,
    weight, origin, fecha_compromiso, compromiso, observacion, project, client,
    duracion, id_telephony, url_record_bot,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `{project_id}.{dataset_id}.sync_voicebot_batch`
WHERE DATE(date) >= {incremental_filter}; -- Ajuste para que el filtro sea compatible