-- Dashboard Base Metrics from BigQuery Raw Sources (POC)
-- Generates granular metrics for dynamic KPI calculation
-- Direct connection to BigQuery for 2-hour POC
-- Based on existing successful query pattern

WITH 
-- 📅 CALENDARIO CON PERIODOS PARA FILTROS
calendario_periodos AS (
  SELECT 
    c.ARCHIVO,
    c.TIPO_CARTERA,
    c.fecha_apertura,
    c.fecha_trandeuda as fecha_trandeuda_campana,
    c.fecha_cierre,
    c.FECHA_CIERRE_PLANIFICADA,
    c.DURACION_CAMPANA_DIAS_HABILES,
    c.ANNO_ASIGNACION,
    c.PERIODO_ASIGNACION,
    
    -- Periodo según reglas de negocio
    CASE 
      WHEN '{fecha_proceso}' >= DATE_TRUNC(c.fecha_apertura, MONTH) 
           AND '{fecha_proceso}' < DATE_TRUNC(COALESCE(c.fecha_cierre, DATE_ADD('{fecha_proceso}', INTERVAL 1 MONTH)), MONTH)
      THEN FORMAT_DATE('%Y-%m', c.fecha_apertura)
      ELSE FORMAT_DATE('%Y-%m', COALESCE(c.fecha_cierre, c.fecha_apertura))
    END AS periodo,
    
    -- Tipo segmento por duración
    CASE 
      WHEN c.DURACION_CAMPANA_DIAS_HABILES <= 30 THEN 'TEMPRANA'
      WHEN c.DURACION_CAMPANA_DIAS_HABILES <= 90 THEN 'INTERMEDIA'
      ELSE 'TARDIA'
    END AS tipo_segmento,
    
    -- Servicio classification
    CASE
      WHEN CONTAINS_SUBSTR(UPPER(c.TIPO_CARTERA), 'TELEFONIA') OR CONTAINS_SUBSTR(UPPER(c.TIPO_CARTERA), 'MOVIL') THEN 'TELEFONIA'
      WHEN CONTAINS_SUBSTR(UPPER(c.TIPO_CARTERA), 'INTERNET') THEN 'INTERNET'
      WHEN CONTAINS_SUBSTR(UPPER(c.TIPO_CARTERA), 'TV') THEN 'TELEVISION'
      WHEN CONTAINS_SUBSTR(UPPER(c.TIPO_CARTERA), 'ENERGIA') THEN 'ENERGIA'
      ELSE 'OTROS'
    END AS servicio,
    
    -- Cartera derivada del archivo
    CASE
      WHEN CONTAINS_SUBSTR(UPPER(c.ARCHIVO), 'TEMPRANA') THEN 'TEMPRANA'
      WHEN CONTAINS_SUBSTR(UPPER(c.ARCHIVO), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO'
      WHEN CONTAINS_SUBSTR(UPPER(c.ARCHIVO), 'AN') THEN 'ALTAS_NUEVAS'
      ELSE 'OTRAS'
    END AS cartera
    
  FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` c
  WHERE c.fecha_apertura <= '{fecha_proceso}'
    AND ({archivo} IS NULL OR c.ARCHIVO = '{archivo}')
),

-- 🏗️ ASIGNACIONES CON GESTIONABILIDAD
asignaciones_base AS (
  SELECT 
    a.archivo,
    a.cod_luna,
    a.cuenta,
    a.cliente,
    a.min_vto,
    IF(a.negocio = "MOVIL", a.negocio, "FIJA") as negocio,
    a.telefono,
    a.tramo_gestion,
    a.zona,
    a.rango_renta,
    DATE(a.creado_el) as fecha_asignacion,
    a.creado_el as timestamp_asignacion,
    
    -- Rango vencimiento para filtros
    CASE 
      WHEN DATE_DIFF('{fecha_proceso}', a.min_vto, DAY) <= 30 THEN '0-30'
      WHEN DATE_DIFF('{fecha_proceso}', a.min_vto, DAY) <= 60 THEN '31-60'
      WHEN DATE_DIFF('{fecha_proceso}', a.min_vto, DAY) <= 90 THEN '61-90'
      WHEN DATE_DIFF('{fecha_proceso}', a.min_vto, DAY) <= 180 THEN '91-180'
      ELSE '180+'
    END AS rango_vencimiento
    
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` a
  WHERE a.creado_el >= '2025-06-11'
    AND DATE(a.creado_el) <= '{fecha_proceso}'
    AND ({archivo} IS NULL OR a.archivo = CONCAT('{archivo}', '.txt'))
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.archivo, a.cuenta ORDER BY a.creado_el DESC
  ) = 1
),

-- 💰 DEUDA CON GESTIONABILIDAD
deuda_consolidada AS (
  SELECT 
    d.cod_cuenta,
    DATE(d.creado_el) as fecha_trandeuda,
    d.nro_documento,
    d.fecha_vencimiento,
    SUM(d.monto_exigible) as monto_exigible_total,
    
    -- Gestionabilidad rules
    CASE 
      WHEN SUM(d.monto_exigible) IS NULL THEN 0
      WHEN SUM(d.monto_exigible) < 1 THEN 0
      ELSE 1
    END as es_gestionable
    
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` d
  WHERE DATE(d.creado_el) <= '{fecha_proceso}'
  GROUP BY 1,2,3,4
),

-- 🎯 GESTIONES UNIFICADAS SIMPLIFICADAS
gestiones_unificadas AS (
  -- Voicebot gestiones
  SELECT
    SAFE_CAST(bot.document AS INT64) AS cod_luna,
    DATE(bot.date) AS fecha_gestion,
    'BOT' AS canal_origen,
    1 as gestiones_count,
    CASE 
      WHEN hom_bot.contactabilidad_homologada = 'Contacto Efectivo' THEN 1 ELSE 0
    END AS es_contacto_efectivo,
    CASE 
      WHEN hom_bot.contactabilidad_homologada = 'Contacto No Efectivo' THEN 1 ELSE 0
    END AS es_contacto_no_efectivo,
    CASE 
      WHEN hom_bot.contactabilidad_homologada = 'Contacto Efectivo' THEN 1
      WHEN hom_bot.contactabilidad_homologada = 'Contacto No Efectivo' THEN 1
      ELSE 0
    END AS tiene_contacto,
    CASE WHEN hom_bot.es_pdp_homologado = 1 THEN 1 ELSE 0 END AS es_compromiso,
    COALESCE(hom_bot.peso_homologado, 1) as peso_gestion

  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e` AS bot
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` AS hom_bot
    ON bot.management = hom_bot.bot_management 
    AND COALESCE(bot.sub_management, '') = COALESCE(hom_bot.bot_sub_management, '')
    AND COALESCE(bot.compromiso, '') = COALESCE(hom_bot.bot_compromiso, '')
  WHERE bot.date >= '2025-05-14'
    AND DATE(bot.date) <= '{fecha_proceso}'

  UNION ALL

  -- MibotAir gestiones
  SELECT
    SAFE_CAST(humano.document AS INT64) AS cod_luna,
    DATE(humano.date) AS fecha_gestion,
    'HUMANO' AS canal_origen,
    1 as gestiones_count,
    CASE WHEN humano.n1 = 'Contacto_Efectivo' THEN 1 ELSE 0 END AS es_contacto_efectivo,
    CASE WHEN humano.n1 = 'Contacto_No_Efectivo' THEN 1 ELSE 0 END AS es_contacto_no_efectivo,
    CASE 
      WHEN humano.n1 = 'Contacto_Efectivo' THEN 1
      WHEN humano.n1 = 'Contacto_No_Efectivo' THEN 1
      ELSE 0
    END AS tiene_contacto,
    CASE WHEN hom_humano.pdp = '1' OR hom_humano.pdp = 'SI' THEN 1 ELSE 0 END AS es_compromiso,
    COALESCE(hom_humano.peso, 1) as peso_gestion

  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e` AS humano
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` AS hom_humano
    ON humano.n1 = hom_humano.n_1 
    AND humano.n2 = hom_humano.n_2 
    AND humano.n3 = hom_humano.n_3
  WHERE humano.date >= '2025-05-14'
    AND DATE(humano.date) <= '{fecha_proceso}'
),

-- 💰 PAGOS ÚNICOS
pagos_unicos AS (
  SELECT 
    nro_documento, 
    monto_cancelado, 
    fecha_pago
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos` 
  WHERE fecha_pago <= '{fecha_proceso}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY nro_documento, fecha_pago, CAST(monto_cancelado AS STRING) 
    ORDER BY creado_el DESC
  ) = 1
),

-- 📊 MÉTRICAS BASE AGREGADAS POR DIMENSIONES
base_metrics AS (
  SELECT
    -- Dimensiones para filtros cruzados
    cp.archivo,
    cp.cartera,
    cp.servicio, 
    cp.periodo,
    cp.tipo_segmento,
    ab.negocio,
    ab.rango_vencimiento,
    ab.zona,
    '{fecha_proceso}' as fecha_foto,
    
    -- Métricas base de volumen
    COUNT(DISTINCT ab.cuenta) as cuentas_asignadas,
    COUNT(DISTINCT ab.cod_luna) as clientes_asignados,
    COUNT(DISTINCT CASE WHEN dc.es_gestionable = 1 THEN ab.cuenta END) as cuentas_gestionables,
    SUM(COALESCE(dc.monto_exigible_total, 0)) as deuda_asignada,
    
    -- Current debt calculation (simplified for POC)
    SUM(COALESCE(dc.monto_exigible_total, 0)) as deuda_actual,
    
    -- Métricas de gestión base
    COUNT(DISTINCT CASE WHEN gu.cod_luna IS NOT NULL THEN ab.cuenta END) as cuentas_gestionadas,
    SUM(COALESCE(gu.gestiones_count, 0)) as total_gestiones,
    SUM(COALESCE(gu.es_contacto_efectivo, 0)) as contactos_efectivos_total,
    SUM(COALESCE(gu.es_contacto_no_efectivo, 0)) as contactos_no_efectivos_total,
    
    -- Contact type counts (account level)
    COUNT(DISTINCT CASE WHEN gu.es_contacto_efectivo > 0 THEN ab.cuenta END) as cuentas_con_contacto_directo,
    COUNT(DISTINCT CASE WHEN gu.es_contacto_no_efectivo > 0 THEN ab.cuenta END) as cuentas_con_contacto_indirecto,
    COUNT(DISTINCT CASE WHEN gu.tiene_contacto > 0 THEN ab.cuenta END) as cuentas_con_contacto_total,
    
    -- Commitment metrics
    COUNT(DISTINCT CASE WHEN gu.es_compromiso > 0 THEN ab.cuenta END) as cuentas_con_compromiso,
    SUM(COALESCE(gu.es_compromiso, 0)) as total_compromisos,
    
    -- Channel breakdown
    SUM(CASE WHEN gu.canal_origen = 'BOT' THEN gu.gestiones_count ELSE 0 END) as gestiones_bot_total,
    SUM(CASE WHEN gu.canal_origen = 'HUMANO' THEN gu.gestiones_count ELSE 0 END) as gestiones_humano_total,
    SUM(CASE WHEN gu.canal_origen = 'BOT' THEN gu.peso_gestion ELSE 0 END) as peso_bot_total,
    SUM(CASE WHEN gu.canal_origen = 'HUMANO' THEN gu.peso_gestion ELSE 0 END) as peso_humano_total,
    
    -- Recovery metrics 
    COUNT(DISTINCT CASE WHEN pu.nro_documento IS NOT NULL THEN ab.cuenta END) as cuentas_pagadoras,
    SUM(COALESCE(pu.monto_cancelado, 0)) as recupero_total,
    COUNT(pu.nro_documento) as pagos_totales
    
  FROM calendario_periodos cp
  INNER JOIN asignaciones_base ab 
    ON ab.archivo = CONCAT(cp.ARCHIVO, '.txt')
  LEFT JOIN deuda_consolidada dc
    ON CAST(ab.cuenta AS STRING) = dc.cod_cuenta
    AND ab.fecha_asignacion = dc.fecha_trandeuda
  LEFT JOIN gestiones_unificadas gu
    ON ab.cod_luna = gu.cod_luna
    AND gu.fecha_gestion >= cp.fecha_apertura
    AND gu.fecha_gestion <= '{fecha_proceso}'
  LEFT JOIN pagos_unicos pu
    ON dc.nro_documento = pu.nro_documento
    AND pu.fecha_pago >= cp.fecha_apertura
    AND pu.fecha_pago <= '{fecha_proceso}'
    
  GROUP BY 1,2,3,4,5,6,7,8,9
)

-- Final result with all dimensions and base metrics
SELECT 
  -- Temporal dimensions
  fecha_foto,
  
  -- Filter dimensions 
  archivo,
  cartera,
  servicio,
  periodo,
  tipo_segmento,
  negocio,
  rango_vencimiento,
  zona,
  
  -- Base volume metrics (for dynamic KPI calculation)
  cuentas_asignadas,
  clientes_asignados, 
  cuentas_gestionables,
  deuda_asignada,
  deuda_actual,
  
  -- Base management metrics  
  cuentas_gestionadas,
  total_gestiones,
  contactos_efectivos_total,
  contactos_no_efectivos_total,
  cuentas_con_contacto_directo,
  cuentas_con_contacto_indirecto,
  cuentas_con_contacto_total,
  
  -- Base commitment metrics
  cuentas_con_compromiso,
  total_compromisos,
  
  -- Channel breakdown
  gestiones_bot_total,
  gestiones_humano_total,
  peso_bot_total,
  peso_humano_total,
  
  -- Recovery base metrics
  cuentas_pagadoras,
  recupero_total,
  pagos_totales

FROM base_metrics
WHERE cuentas_asignadas > 0
ORDER BY periodo DESC, cartera, servicio, negocio