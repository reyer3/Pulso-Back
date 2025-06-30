-- Extracts MibotAir homologation rules. Typically a full refresh.
-- Parameters:
-- {incremental_filter}: Will be '1=1' for full refresh.

SELECT
    management, n_1, n_2, n_3, peso, contactabilidad,
    tipo_gestion, codigo_rpta, pdp, gestor,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2`
WHERE {incremental_filter};