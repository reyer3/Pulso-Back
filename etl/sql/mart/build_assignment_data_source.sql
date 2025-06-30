-- Assignment Data Source Query
-- Extracts portfolio composition metrics by period from asignaciones and related tables
-- Schemas: {raw_schema}, {aux_schema}
-- Parameters: {fecha_proceso}, {archivo} (optional)

WITH period_info AS (
    SELECT 
        TO_CHAR('{fecha_proceso}'::date, 'YYYY-MM') as periodo_actual,
        DATE_TRUNC('month', '{fecha_proceso}'::date) as inicio_mes,
        (DATE_TRUNC('month', '{fecha_proceso}'::date) + INTERVAL '1 month - 1 day')::date as fin_mes
),

active_campaigns AS (
    -- Get campaigns active during the period
    SELECT DISTINCT
        cal.archivo,
        cal.tipo_cartera,
        cal.fecha_apertura,
        cal.fecha_cierre,
        cal.estado_cartera,
        pi.periodo_actual
    FROM {raw_schema}.calendario cal
    CROSS JOIN period_info pi
    WHERE cal.fecha_apertura <= pi.fin_mes
        AND (cal.fecha_cierre IS NULL OR cal.fecha_cierre >= pi.inicio_mes)
        AND ({archivo} IS NULL OR cal.archivo = '{archivo}')
),

assignment_metrics AS (
    -- Calculate assignment composition metrics
    SELECT 
        ac.periodo_actual as periodo,
        ac.archivo,
        ac.tipo_cartera as cartera,
        
        -- Volume metrics
        COUNT(DISTINCT asig.dni) as clientes,
        COUNT(DISTINCT asig.cuenta) as cuentas,
        
        -- Assignment debt (from assignment time)
        SUM(COALESCE(td_asig.monto_exigible, 0)) as deuda_asig,
        
        -- Current debt (as of fecha_proceso)
        SUM(COALESCE(td_current.monto_exigible, 0)) as deuda_actual,
        
        -- Business classification metrics
        COUNT(DISTINCT CASE WHEN asig.negocio IS NOT NULL THEN asig.negocio END) as tipos_negocio,
        COUNT(DISTINCT CASE WHEN asig.tramo_gestion IS NOT NULL THEN asig.tramo_gestion END) as tramos_gestion,
        COUNT(DISTINCT CASE WHEN asig.zona IS NOT NULL THEN asig.zona END) as zonas,
        
        -- Risk distribution
        AVG(CASE WHEN asig.decil_contacto BETWEEN 1 AND 10 THEN asig.decil_contacto ELSE NULL END) as decil_contacto_promedio,
        AVG(CASE WHEN asig.decil_pago BETWEEN 1 AND 10 THEN asig.decil_pago ELSE NULL END) as decil_pago_promedio,
        
        -- Portfolio characteristics
        COUNT(CASE WHEN asig.fraccionamiento IS NOT NULL AND asig.fraccionamiento != '' THEN 1 END) as cuentas_con_fraccionamiento,
        COUNT(CASE WHEN asig.priorizado = 'SI' OR asig.priorizado = 'S' THEN 1 END) as cuentas_priorizadas,
        
        -- Age distribution (days since assignment)
        AVG(EXTRACT(EPOCH FROM ('{fecha_proceso}'::date - asig.fecha_asignacion)) / 86400.0) as dias_promedio_asignacion,
        MIN(asig.fecha_asignacion) as fecha_asignacion_mas_antigua,
        MAX(asig.fecha_asignacion) as fecha_asignacion_mas_reciente
        
    FROM active_campaigns ac
    INNER JOIN {raw_schema}.asignaciones asig 
        ON ac.archivo = asig.archivo
    
    -- Debt at assignment time
    LEFT JOIN {raw_schema}.trandeuda td_asig 
        ON asig.cuenta = td_asig.cod_cuenta 
        AND asig.archivo = td_asig.archivo
        AND td_asig.fecha_proceso = (
            SELECT MAX(fecha_proceso) 
            FROM {raw_schema}.trandeuda td2 
            WHERE td2.cod_cuenta = asig.cuenta 
            AND td2.archivo = asig.archivo
            AND td2.fecha_proceso <= asig.fecha_asignacion
        )
    
    -- Current debt
    LEFT JOIN {raw_schema}.trandeuda td_current
        ON asig.cuenta = td_current.cod_cuenta 
        AND asig.archivo = td_current.archivo
        AND td_current.fecha_proceso = (
            SELECT MAX(fecha_proceso) 
            FROM {raw_schema}.trandeuda td3 
            WHERE td3.cod_cuenta = asig.cuenta 
            AND td3.archivo = asig.archivo
            AND td3.fecha_proceso <= '{fecha_proceso}'
        )
    
    WHERE asig.fecha_asignacion <= '{fecha_proceso}'
    
    GROUP BY ac.periodo_actual, ac.archivo, ac.tipo_cartera
),

payment_impact AS (
    -- Calculate payment impact for the period
    SELECT 
        pi.periodo_actual as periodo,
        pd.archivo,
        SUM(pd.monto_cancelado) as recupero_periodo,
        COUNT(DISTINCT pd.nro_documento) as documentos_con_pago,
        COUNT(*) as total_pagos
    FROM period_info pi
    CROSS JOIN {aux_schema}.pago_deduplication pd
    WHERE pd.fecha_pago BETWEEN pi.inicio_mes AND pi.fin_mes
        AND pd.es_pago_valido = true
        AND ({archivo} IS NULL OR pd.archivo = '{archivo}')
    GROUP BY pi.periodo_actual, pd.archivo
),

gestiones_impact AS (
    -- Calculate gestiones impact for the period  
    SELECT 
        pi.periodo_actual as periodo,
        gu.archivo_campana as archivo,
        COUNT(DISTINCT gu.cod_luna) as cuentas_gestionadas_periodo,
        COUNT(*) as total_gestiones_periodo,
        SUM(CASE WHEN gu.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos_periodo
    FROM period_info pi
    CROSS JOIN {aux_schema}.gestiones_unificadas gu
    WHERE DATE(gu.timestamp_gestion) BETWEEN pi.inicio_mes AND pi.fin_mes
        AND ({archivo} IS NULL OR gu.archivo_campana = '{archivo}')
    GROUP BY pi.periodo_actual, gu.archivo_campana
)

-- Main query combining all assignment composition metrics
SELECT 
    am.periodo,
    am.archivo,
    am.cartera,
    
    -- Core volume metrics
    am.clientes,
    am.cuentas,
    ROUND(am.deuda_asig, 2) as deuda_asig,
    ROUND(am.deuda_actual, 2) as deuda_actual,
    
    -- Calculated metrics
    CASE 
        WHEN am.cuentas > 0 
        THEN ROUND((am.deuda_asig / am.cuentas), 2)
        ELSE 0 
    END as ticket_promedio,
    
    -- Portfolio composition
    am.tipos_negocio,
    am.tramos_gestion,
    am.zonas,
    ROUND(am.decil_contacto_promedio, 1) as decil_contacto_promedio,
    ROUND(am.decil_pago_promedio, 1) as decil_pago_promedio,
    am.cuentas_con_fraccionamiento,
    am.cuentas_priorizadas,
    
    -- Temporal characteristics
    ROUND(am.dias_promedio_asignacion, 1) as dias_promedio_asignacion,
    am.fecha_asignacion_mas_antigua,
    am.fecha_asignacion_mas_reciente,
    
    -- Performance context
    COALESCE(pi.recupero_periodo, 0) as recupero_periodo,
    COALESCE(pi.documentos_con_pago, 0) as documentos_con_pago,
    COALESCE(gi.cuentas_gestionadas_periodo, 0) as cuentas_gestionadas_periodo,
    COALESCE(gi.total_gestiones_periodo, 0) as total_gestiones_periodo,
    COALESCE(gi.contactos_efectivos_periodo, 0) as contactos_efectivos_periodo

FROM assignment_metrics am
LEFT JOIN payment_impact pi 
    ON am.periodo = pi.periodo AND am.archivo = pi.archivo
LEFT JOIN gestiones_impact gi 
    ON am.periodo = gi.periodo AND am.archivo = gi.archivo

WHERE am.cuentas > 0  -- Only portfolios with actual assignments

ORDER BY am.periodo DESC, am.archivo, am.cartera