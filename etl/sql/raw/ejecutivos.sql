-- Extracts agent/executive information. Typically a full refresh.
-- Parameters:
-- {incremental_filter}: Will be '1=1' for full refresh.

SELECT DISTINCT
    correo_name,
    TRIM(nombre) as nombre,
    document,
    CURRENT_TIMESTAMP() as extraction_timestamp
FROM `mibot-222814.BI_USA.sync_mibotair_batch_SYS_user`
WHERE id_cliente = 145 AND {incremental_filter};