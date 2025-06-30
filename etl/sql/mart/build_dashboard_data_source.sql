-- Dashboard Data Source Query
-- Extracts base data from RAW and AUX layers for Python transformation
-- Schemas: {raw_schema}, {aux_schema}
-- Parameters: {fecha_proceso}, {archivo} (optional filter)

WITH base_campaigns AS (
    -- Campaign metadata with active campaigns for the date
    SELECT DISTINCT
        cal.archivo,
        cal.tipo_cartera,
        cal.fecha_apertura,
        cal.fecha_cierre,
        cal.estado_cartera,
        CASE 
            WHEN cal.tipo_cartera ILIKE '%telefonia%' OR cal.tipo_cartera ILIKE '%movil%' THEN 'TELEFONIA'
            WHEN cal.tipo_cartera ILIKE '%internet%' OR cal.tipo_cartera ILIKE '%broadband%' THEN 'INTERNET'  
            WHEN cal.tipo_cartera ILIKE '%tv%' OR cal.tipo_cartera ILIKE '%television%' THEN 'TELEVISION'
            WHEN cal.tipo_cartera ILIKE '%energia%' OR cal.tipo_cartera ILIKE '%luz%' THEN 'ENERGIA'
            ELSE 'OTROS'
        END as servicio
    FROM {raw_schema}.calendario cal
    WHERE cal.fecha_apertura <= '{fecha_proceso}'
        AND (cal.fecha_cierre IS NULL OR cal.fecha_cierre >= '{fecha_proceso}')
        AND ({archivo} IS NULL OR cal.archivo = '{archivo}')
),

assignment_summary AS (
    -- Account assignments with latest debt information
    SELECT 
        asig.archivo,
        COUNT(DISTINCT asig.cuenta) as total_cuentas,
        COUNT(DISTINCT asig.dni) as total_clientes,
        -- Get latest debt per account
        SUM(COALESCE(td_latest.monto_exigible, 0)) as deuda_asignada,
        SUM(COALESCE(td_current.monto_exigible, 0)) as deuda_actual
    FROM {raw_schema}.asignaciones asig
    -- Latest historical debt (at assignment time)
    LEFT JOIN {raw_schema}.trandeuda td_latest 
        ON asig.cuenta = td_latest.cod_cuenta 
        AND asig.archivo = td_latest.archivo
        AND td_latest.fecha_proceso = (
            SELECT MAX(fecha_proceso) 
            FROM {raw_schema}.trandeuda td2 
            WHERE td2.cod_cuenta = asig.cuenta 
            AND td2.archivo = asig.archivo
            AND td2.fecha_proceso <= asig.fecha_asignacion
        )
    -- Current debt (for fecha_proceso)
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
        AND ({archivo} IS NULL OR asig.archivo = '{archivo}')
    GROUP BY asig.archivo
),

gestiones_summary AS (
    -- Unified gestiones metrics from AUX layer
    SELECT 
        gu.archivo_campana as archivo,
        COUNT(*) as total_gestiones,
        COUNT(DISTINCT gu.cod_luna) as cuentas_gestionadas,
        
        -- Contact effectiveness breakdown
        SUM(CASE WHEN gu.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
        SUM(CASE WHEN gu.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END) as cuentas_cd,
        SUM(CASE WHEN gu.contactabilidad = 'Contacto No Efectivo' THEN 1 ELSE 0 END) as cuentas_ci,
        SUM(CASE WHEN gu.contactabilidad = 'SIN_CLASIFICAR' THEN 1 ELSE 0 END) as cuentas_sc,
        
        -- Commitments (PDPs)
        SUM(CASE WHEN gu.es_compromiso THEN 1 ELSE 0 END) as total_pdp,
        
        -- Weight for intensity calculations
        SUM(gu.peso_gestion) as peso_total,
        
        -- Channel breakdown for additional insights
        SUM(CASE WHEN gu.canal_origen = 'BOT' THEN 1 ELSE 0 END) as gestiones_bot,
        SUM(CASE WHEN gu.canal_origen = 'HUMANO' THEN 1 ELSE 0 END) as gestiones_humano
    FROM {aux_schema}.gestiones_unificadas gu
    WHERE DATE(gu.timestamp_gestion) <= '{fecha_proceso}'
        AND ({archivo} IS NULL OR gu.archivo_campana = '{archivo}')
    GROUP BY gu.archivo_campana
),

payment_summary AS (
    -- Recovery from deduplicated payments
    SELECT 
        pd.archivo,
        SUM(pd.monto_cancelado) as recupero_total,
        COUNT(DISTINCT pd.nro_documento) as documentos_con_pago,
        COUNT(*) as total_pagos
    FROM {aux_schema}.pago_deduplication pd
    WHERE pd.fecha_pago <= '{fecha_proceso}'
        AND pd.es_pago_valido = true
        AND pd.esta_en_ventana = true  -- Only payments within campaign window
        AND ({archivo} IS NULL OR pd.archivo = '{archivo}')
    GROUP BY pd.archivo
)

-- Main query combining all metrics
SELECT 
    bc.archivo,
    bc.tipo_cartera,
    bc.servicio,
    bc.fecha_apertura,
    bc.fecha_cierre,
    bc.estado_cartera,
    
    -- Volume metrics
    COALESCE(asig.total_cuentas, 0) as cuentas,
    COALESCE(asig.total_clientes, 0) as clientes,
    COALESCE(asig.deuda_asignada, 0) as deuda_asig,
    COALESCE(asig.deuda_actual, 0) as deuda_act,
    
    -- Management metrics (raw counts for Python processing)
    COALESCE(gest.cuentas_gestionadas, 0) as cuentas_gestionadas,
    COALESCE(gest.total_gestiones, 0) as total_gestiones,
    COALESCE(gest.contactos_efectivos, 0) as contactos_efectivos,
    COALESCE(gest.cuentas_cd, 0) as cuentas_cd,
    COALESCE(gest.cuentas_ci, 0) as cuentas_ci,
    COALESCE(gest.cuentas_sc, 0) as cuentas_sc,
    COALESCE(gest.total_pdp, 0) as cuentas_pdp,
    COALESCE(gest.peso_total, 0) as peso_total,
    
    -- Channel insights
    COALESCE(gest.gestiones_bot, 0) as gestiones_bot,
    COALESCE(gest.gestiones_humano, 0) as gestiones_humano,
    
    -- Recovery metrics
    COALESCE(pag.recupero_total, 0) as recupero,
    COALESCE(pag.documentos_con_pago, 0) as documentos_con_pago,
    COALESCE(pag.total_pagos, 0) as total_pagos

FROM base_campaigns bc
LEFT JOIN assignment_summary asig ON bc.archivo = asig.archivo
LEFT JOIN gestiones_summary gest ON bc.archivo = gest.archivo
LEFT JOIN payment_summary pag ON bc.archivo = pag.archivo

WHERE bc.estado_cartera IN ('ACTIVA', 'CERRADA')  -- Only process active/closed campaigns

ORDER BY bc.archivo