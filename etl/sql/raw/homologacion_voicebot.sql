-- Extracts Voicebot homologation rules. Typically a full refresh.
-- Parameters:
-- {incremental_filter}: Will be '1=1' for full refresh.

SELECT
    bot_management, bot_sub_management, bot_compromiso,
    n1_homologado, n2_homologado, n3_homologado,
    contactabilidad_homologada, es_pdp_homologado, peso_homologado,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot`
WHERE {incremental_filter};