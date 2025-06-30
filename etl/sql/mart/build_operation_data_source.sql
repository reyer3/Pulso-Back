-- Operation Data Source Query
-- Extracts hourly operational metrics by channel from gestiones
-- Schemas: {aux_schema}
-- Parameters: {fecha_proceso}, {archivo} (optional)

WITH hourly_gestiones AS (
    SELECT 
        EXTRACT(HOUR FROM gu.timestamp_gestion) as hora,
        gu.canal_origen,
        gu.archivo_campana as archivo,
        COUNT(*) as total_gestiones,
        
        -- Contact effectiveness breakdown
        SUM(CASE WHEN gu.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
        SUM(CASE WHEN NOT gu.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_no_efectivos,
        
        -- Contact type distribution
        SUM(CASE WHEN gu.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END) as contactos_directos,
        SUM(CASE WHEN gu.contactabilidad = 'Contacto No Efectivo' THEN 1 ELSE 0 END) as contactos_indirectos,
        SUM(CASE WHEN gu.contactabilidad = 'SIN_CLASIFICAR' THEN 1 ELSE 0 END) as sin_clasificar,
        
        -- Commitments (PDPs)
        SUM(CASE WHEN gu.es_compromiso THEN 1 ELSE 0 END) as total_pdp,
        
        -- Unique accounts managed
        COUNT(DISTINCT gu.cod_luna) as cuentas_gestionadas,
        
        -- Weight for intensity calculations
        SUM(gu.peso_gestion) as peso_total,
        
        -- First and last gestion of the hour for duration analysis
        MIN(gu.timestamp_gestion) as primera_gestion_hora,
        MAX(gu.timestamp_gestion) as ultima_gestion_hora
        
    FROM {aux_schema}.gestiones_unificadas gu
    WHERE DATE(gu.timestamp_gestion) = '{fecha_proceso}'
        AND ({archivo} IS NULL OR gu.archivo_campana = '{archivo}')
    GROUP BY 
        EXTRACT(HOUR FROM gu.timestamp_gestion), 
        gu.canal_origen, 
        gu.archivo_campana
),

channel_daily_totals AS (
    -- Get daily totals by channel for percentage calculations
    SELECT 
        gu.canal_origen,
        gu.archivo_campana as archivo,
        COUNT(*) as total_gestiones_dia,
        SUM(CASE WHEN gu.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos_dia,
        SUM(CASE WHEN gu.es_compromiso THEN 1 ELSE 0 END) as total_pdp_dia
    FROM {aux_schema}.gestiones_unificadas gu
    WHERE DATE(gu.timestamp_gestion) = '{fecha_proceso}'
        AND ({archivo} IS NULL OR gu.archivo_campana = '{archivo}')
    GROUP BY gu.canal_origen, gu.archivo_campana
)

SELECT 
    hg.hora,
    
    -- Standardize channel names
    CASE 
        WHEN hg.canal_origen = 'BOT' THEN 'VOICEBOT'
        WHEN hg.canal_origen = 'HUMANO' THEN 'CALL_CENTER'
        ELSE 'OTROS'
    END as canal,
    
    COALESCE(hg.archivo, 'GENERAL') as archivo,
    
    -- Volume metrics
    hg.total_gestiones,
    hg.contactos_efectivos,
    hg.contactos_no_efectivos,
    hg.contactos_directos,
    hg.contactos_indirectos,
    hg.sin_clasificar,
    hg.total_pdp,
    hg.cuentas_gestionadas,
    hg.peso_total,
    
    -- Time metrics
    hg.primera_gestion_hora,
    hg.ultima_gestion_hora,
    EXTRACT(EPOCH FROM (hg.ultima_gestion_hora - hg.primera_gestion_hora)) / 60.0 as minutos_activos,
    
    -- Basic rates (will be refined in Python)
    CASE 
        WHEN hg.total_gestiones > 0 
        THEN ROUND((hg.contactos_efectivos::float / hg.total_gestiones * 100), 2)
        ELSE 0 
    END as tasa_contacto_raw,
    
    CASE 
        WHEN hg.contactos_efectivos > 0 
        THEN ROUND((hg.total_pdp::float / hg.contactos_efectivos * 100), 2)
        ELSE 0 
    END as tasa_conversion_raw,
    
    -- Daily context for percentage calculations
    cdt.total_gestiones_dia,
    cdt.contactos_efectivos_dia,
    cdt.total_pdp_dia,
    
    -- Hourly percentage of daily activity
    CASE 
        WHEN cdt.total_gestiones_dia > 0 
        THEN ROUND((hg.total_gestiones::float / cdt.total_gestiones_dia * 100), 2)
        ELSE 0 
    END as pct_gestiones_del_dia,
    
    -- Intensity metrics
    CASE 
        WHEN hg.cuentas_gestionadas > 0 
        THEN ROUND((hg.total_gestiones::float / hg.cuentas_gestionadas), 2)
        ELSE 0 
    END as intensidad_raw

FROM hourly_gestiones hg
LEFT JOIN channel_daily_totals cdt 
    ON hg.canal_origen = cdt.canal_origen 
    AND hg.archivo = cdt.archivo

WHERE hg.hora BETWEEN 0 AND 23  -- Valid hour range
    AND hg.total_gestiones > 0    -- Only hours with activity

ORDER BY hg.archivo, hg.canal_origen, hg.hora