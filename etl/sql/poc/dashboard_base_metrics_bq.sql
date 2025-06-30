-- Dashboard Base Metrics BigQuery Source
-- Based on existing table: BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_tbldashboard_metricas_base
-- Provides granular metrics for dashboard with cross-filters and dynamic aggregations

-- Parameters:
-- @fecha_corte: Date filter for fecha_foto_proceso (optional)
-- @archivo_filter: Campaign file filter (optional)
-- @periodo_filter: Period filter (optional)
-- @cartera_filter: Cartera filter (optional)
-- @servicio_filter: Service filter (optional)

SELECT
  -- Temporal dimensions
  fecha_foto_proceso,
  dia_gestion,
  dia_habil,
  PERIODO,
  PERIODO_DATE,
  
  -- Campaign dimensions for cross-filters
  ARCHIVO,
  TIPO_CARTERA,
  SERVICIO,
  
  -- Derived dimensions for frontend filters
  CASE 
    WHEN UPPER(TIPO_CARTERA) LIKE '%TEMPRANA%' THEN 'TEMPRANA'
    WHEN UPPER(TIPO_CARTERA) LIKE '%INTERMEDIA%' THEN 'INTERMEDIA' 
    WHEN UPPER(TIPO_CARTERA) LIKE '%TARDIA%' THEN 'TARDIA'
    ELSE 'OTROS'
  END as tipo_segmento,
  
  CASE
    WHEN dia_gestion <= 10 THEN '1-10'
    WHEN dia_gestion <= 20 THEN '11-20'
    WHEN dia_gestion <= 30 THEN '21-30'
    ELSE '30+'
  END as rango_dias_gestion,
  
  -- Volume base metrics (for KPI calculations)
  cuentas_asignadas,
  clientes_asignados,
  cuentas_gestionables,
  cuentas_validas,
  deuda_inicial_total,
  deuda_promedio_por_cuenta,
  total_documentos,
  
  -- Management base metrics (for KPI calculations)
  cuentas_gestionadas,
  clientes_gestionados,
  total_gestiones_validas,
  contactos_efectivos_total,
  contactos_no_efectivos_total,
  total_compromisos,
  
  -- Contact distribution metrics
  cuentas_con_contacto_directo,
  cuentas_con_contacto_indirecto,
  cuentas_con_contacto_total,
  cuentas_con_compromiso,
  
  -- Financial base metrics
  cuentas_pagadoras,
  recupero_total,
  pagos_totales_validos,
  
  -- Channel breakdown metrics
  gestiones_bot_total,
  gestiones_humano_total,
  
  -- Calculated fields for frontend convenience
  CASE 
    WHEN cuentas_asignadas > 0 THEN cuentas_gestionadas / cuentas_asignadas * 100 
    ELSE 0 
  END as pct_cobertura_calculado,
  
  CASE 
    WHEN total_gestiones_validas > 0 THEN contactos_efectivos_total / total_gestiones_validas * 100 
    ELSE 0 
  END as pct_contacto_calculado,
  
  CASE 
    WHEN contactos_efectivos_total > 0 THEN total_compromisos / contactos_efectivos_total * 100 
    ELSE 0 
  END as pct_conversion_calculado,
  
  CASE 
    WHEN cuentas_asignadas > 0 THEN cuentas_pagadoras / cuentas_asignadas * 100 
    ELSE 0 
  END as pct_cierre_calculado,
  
  CASE 
    WHEN deuda_inicial_total > 0 THEN recupero_total / deuda_inicial_total * 100 
    ELSE 0 
  END as pct_efectividad_calculado,
  
  CASE 
    WHEN cuentas_gestionadas > 0 THEN total_gestiones_validas / cuentas_gestionadas 
    ELSE 0 
  END as intensidad_calculada

FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_tbldashboard_metricas_base`

WHERE 1=1
  -- Dynamic filters (to be replaced by service layer)
  AND (@fecha_corte IS NULL OR DATE(fecha_foto_proceso) = @fecha_corte)
  AND (@archivo_filter IS NULL OR ARCHIVO = @archivo_filter)
  AND (@periodo_filter IS NULL OR PERIODO = @periodo_filter)
  AND (@cartera_filter IS NULL OR TIPO_CARTERA = @cartera_filter)
  AND (@servicio_filter IS NULL OR SERVICIO = @servicio_filter)
  
  -- Quality filters
  AND cuentas_asignadas > 0
  AND fecha_foto_proceso IS NOT NULL

ORDER BY 
  PERIODO_DATE DESC,
  PERIODO DESC,
  SERVICIO,
  TIPO_CARTERA,
  dia_gestion DESC
