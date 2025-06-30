-- Productivity Data Source Query
-- Extracts agent performance data from gestiones and agent info
-- Schemas: {raw_schema}, {aux_schema}
-- Parameters: {fecha_proceso}, {archivo} (optional)

WITH agent_gestiones AS (
    -- Get gestiones by agent from MibotAir (human agents)
    SELECT 
        mg.correo_agente,
        mg.nombre_agente,
        gu.archivo_campana as archivo,
        COUNT(*) as total_gestiones,
        SUM(CASE WHEN gu.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
        SUM(CASE WHEN gu.es_compromiso THEN 1 ELSE 0 END) as total_pdp,
        SUM(gu.peso_gestion) as peso_total,
        AVG(mg.duracion) as duracion_promedio,
        COUNT(DISTINCT gu.cod_luna) as cuentas_unicas,
        
        -- Additional metrics for scoring
        MIN(gu.timestamp_gestion) as primera_gestion,
        MAX(gu.timestamp_gestion) as ultima_gestion,
        
        -- Channel distribution
        COUNT(CASE WHEN gu.canal_origen = 'HUMANO' THEN 1 END) as gestiones_humanas,
        
        -- Contact distribution
        SUM(CASE WHEN gu.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END) as contactos_directos,
        SUM(CASE WHEN gu.contactabilidad = 'Contacto No Efectivo' THEN 1 ELSE 0 END) as contactos_indirectos,
        
        -- Financial impact (if available)
        SUM(COALESCE(mg.monto_compromiso, 0)) as monto_comprometido_total
        
    FROM {raw_schema}.mibotair_gestiones mg
    INNER JOIN {aux_schema}.gestiones_unificadas gu 
        ON mg.uid = gu.gestion_uid
    WHERE DATE(mg.date) = '{fecha_proceso}'
        AND mg.correo_agente IS NOT NULL
        AND mg.correo_agente != ''
        AND gu.canal_origen = 'HUMANO'  -- Only human agents
        AND ({archivo} IS NULL OR gu.archivo_campana = '{archivo}')
    GROUP BY mg.correo_agente, mg.nombre_agente, gu.archivo_campana
),

agent_info AS (
    -- Get agent details from ejecutivos table
    SELECT DISTINCT
        correo_name,
        nombre,
        document as dni_agente
    FROM {raw_schema}.ejecutivos
    WHERE correo_name IS NOT NULL
),

agent_assignments AS (
    -- Get assigned accounts per agent (if available through gestiones)
    SELECT 
        mg.correo_agente,
        gu.archivo_campana as archivo,
        COUNT(DISTINCT gu.cod_luna) as cuentas_asignadas
    FROM {raw_schema}.mibotair_gestiones mg
    INNER JOIN {aux_schema}.gestiones_unificadas gu 
        ON mg.uid = gu.gestion_uid
    INNER JOIN {raw_schema}.asignaciones asig
        ON gu.cod_luna = asig.cod_luna 
        AND gu.archivo_campana = asig.archivo
    WHERE DATE(mg.date) <= '{fecha_proceso}'
        AND mg.correo_agente IS NOT NULL
        AND ({archivo} IS NULL OR gu.archivo_campana = '{archivo}')
    GROUP BY mg.correo_agente, gu.archivo_campana
),

team_assignments AS (
    -- Determine team based on performance patterns (simplified logic)
    SELECT 
        correo_agente,
        archivo,
        CASE 
            WHEN AVG(total_gestiones) > 50 THEN 'SENIOR_TEAM'
            WHEN AVG(total_gestiones) > 25 THEN 'REGULAR_TEAM'
            ELSE 'JUNIOR_TEAM'
        END as equipo_estimado
    FROM agent_gestiones
    GROUP BY correo_agente, archivo
)

-- Main query combining all agent metrics
SELECT 
    ag.correo_agente,
    ag.archivo,
    ag.total_gestiones,
    ag.contactos_efectivos,
    ag.total_pdp,
    ag.peso_total,
    ag.duracion_promedio,
    ag.cuentas_unicas,
    ag.primera_gestion,
    ag.ultima_gestion,
    ag.gestiones_humanas,
    ag.contactos_directos,
    ag.contactos_indirectos,
    ag.monto_comprometido_total,
    
    -- Agent information
    COALESCE(ai.nombre, ag.nombre_agente, 'UNKNOWN') as nombre_agente,
    COALESCE(ai.dni_agente, 'SIN_DNI') as dni_agente,
    
    -- Performance context
    COALESCE(aa.cuentas_asignadas, ag.cuentas_unicas) as cuentas_asignadas,
    COALESCE(ta.equipo_estimado, 'GENERAL') as equipo,
    
    -- Time metrics for consistency analysis
    EXTRACT(EPOCH FROM (ag.ultima_gestion - ag.primera_gestion)) / 3600.0 as horas_activas,
    
    -- Calculate basic rates (will be refined in Python)
    CASE 
        WHEN ag.total_gestiones > 0 
        THEN ROUND((ag.contactos_efectivos::float / ag.total_gestiones * 100), 2)
        ELSE 0 
    END as tasa_contacto_raw,
    
    CASE 
        WHEN ag.contactos_efectivos > 0 
        THEN ROUND((ag.total_pdp::float / ag.contactos_efectivos * 100), 2)
        ELSE 0 
    END as tasa_conversion_raw

FROM agent_gestiones ag
LEFT JOIN agent_info ai ON ag.correo_agente = ai.correo_name
LEFT JOIN agent_assignments aa ON ag.correo_agente = aa.correo_agente AND ag.archivo = aa.archivo
LEFT JOIN team_assignments ta ON ag.correo_agente = ta.correo_agente AND ag.archivo = ta.archivo

WHERE ag.total_gestiones > 0  -- Only agents with actual activity
ORDER BY ag.correo_agente, ag.archivo