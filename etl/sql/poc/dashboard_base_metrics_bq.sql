-- Dashboard Base Metrics (POC BigQuery Direct)
-- Basado en la query de referencia compartida
-- Genera métricas base granulares para cálculo dinámico de KPIs

WITH 
-- 📅 CALENDARIO CON PERIODOS (basado en referencia)
calendario_expandido as (
  select 
    c.ARCHIVO,
    c.TIPO_CARTERA,
    c.FECHA_ASIGNACION as fecha_asignacion_campana,
    c.FECHA_TRANDEUDA as fecha_trandeuda_campana,
    c.FECHA_CIERRE,
    c.FECHA_CIERRE_PLANIFICADA,
    c.DURACION_CAMPANA_DIAS_HABILES,
    c.ANNO_ASIGNACION,
    c.PERIODO_ASIGNACION,
    
    -- 🎯 PERÍODOS: Campaña que comienza O termina en este mes
    periodo_individual as PERIODO,
    
    -- 📅 PERIODO_DATE: Para comparativas
    DATE(EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', periodo_individual)), 
         EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', periodo_individual)), 
         1) as PERIODO_DATE,
    
    -- 🔄 CLASIFICACIÓN POR CRUCE DE MES
    CASE 
      WHEN EXTRACT(MONTH FROM c.FECHA_ASIGNACION) = EXTRACT(MONTH FROM COALESCE(c.FECHA_CIERRE, c.FECHA_CIERRE_PLANIFICADA)) 
      THEN 'INTRA_MES'
      ELSE 'INTER_MES'
    END AS TIPO_CICLO_CAMPANA,
    
    -- 🏷️ CLASIFICACIÓN POR DURACIÓN (tipo_segmento)
    CASE 
      WHEN c.DURACION_CAMPANA_DIAS_HABILES <= 10 THEN 'TEMPRANA'
      WHEN c.DURACION_CAMPANA_DIAS_HABILES <= 20 THEN 'INTERMEDIA'
      ELSE 'TARDIA'
    END AS TIPO_SEGMENTO
    
  from `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` c
  cross join unnest([
    FORMAT_DATE('%Y-%m', c.FECHA_ASIGNACION),
    FORMAT_DATE('%Y-%m', COALESCE(c.FECHA_CIERRE, c.FECHA_CIERRE_PLANIFICADA))
  ]) as periodo_individual
  where periodo_individual IS NOT NULL
    AND ('{archivo}' IS NULL OR c.ARCHIVO = '{archivo}')
  qualify ROW_NUMBER() OVER (
    PARTITION BY c.ARCHIVO, periodo_individual ORDER BY c.FECHA_ASIGNACION
  ) = 1
),

-- 🏗️ ASIGNACIÓN LIMPIA (basado en referencia)
asignacion_limpia as (
  select 
    a.archivo,
    a.cod_luna,
    a.cuenta,
    a.min_vto,
    IF(a.negocio="MOVIL", a.negocio, "FIJA") as negocio,
    a.telefono,
    a.tramo_gestion,
    a.zona,
    a.rango_renta,
    DATE(a.creado_el) as fecha_asignacion_real,
    a.creado_el as timestamp_asignacion
    
  from `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` a
  where a.archivo IN (
    select concat(ce.ARCHIVO, ".txt") from calendario_expandido ce
  ) 
  AND a.creado_el >= '2025-06-11'
  AND DATE(a.creado_el) <= '{fecha_proceso}'
  qualify ROW_NUMBER() OVER (
    PARTITION BY a.archivo, a.cuenta ORDER BY a.creado_el DESC
  ) = 1
),

-- 💰 DEUDA CONSOLIDADA (basado en referencia)
deuda_limpia as (
  select 
    d.cod_cuenta,
    DATE(d.creado_el) as fecha_trandeuda_real,
    d.nro_documento,
    d.fecha_vencimiento,
    SUM(d.monto_exigible) as monto_exigible_total
    
  from `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` d
  where d.cod_cuenta IN (
      select DISTINCT CAST(al.cuenta AS STRING) from asignacion_limpia al
    )
  AND DATE(d.creado_el) <= '{fecha_proceso}'
  group by 1,2,3,4
),

-- 🎯 GESTIONES UNIFICADAS (basado en referencia)
gestiones_unificadas as (
  SELECT
    SAFE_CAST(bot.document AS INT64) AS cod_luna,
    DATE(bot.date) AS fecha_gestion,
    'BOT' AS canal_origen,
    CASE 
      WHEN hom_bot.contactabilidad_homologada = 'Contacto Efectivo' THEN TRUE 
      ELSE FALSE 
    END AS es_contacto_efectivo,
    CASE 
      WHEN hom_bot.contactabilidad_homologada = 'Contacto No Efectivo' THEN TRUE 
      ELSE FALSE 
    END AS es_contacto_no_efectivo,
    CASE WHEN hom_bot.es_pdp_homologado = 1 THEN TRUE ELSE FALSE END AS es_compromiso,
    COALESCE(hom_bot.peso_homologado, 1) as peso_gestion

  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e` AS bot
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` AS hom_bot
    ON bot.management = hom_bot.bot_management 
    AND COALESCE(bot.sub_management, '') = COALESCE(hom_bot.bot_sub_management, '')
    AND COALESCE(bot.compromiso, '') = COALESCE(hom_bot.bot_compromiso, '')
  WHERE bot.date >= '2025-05-14'
    AND DATE(bot.date) <= '{fecha_proceso}'

  UNION ALL

  SELECT
    SAFE_CAST(humano.document AS INT64) AS cod_luna,
    DATE(humano.date) AS fecha_gestion,
    'HUMANO' AS canal_origen,
    CASE 
      WHEN humano.n1 = 'Contacto_Efectivo' THEN TRUE 
      ELSE FALSE 
    END AS es_contacto_efectivo,
    CASE 
      WHEN humano.n1 = 'Contacto_No_Efectivo' THEN TRUE 
      ELSE FALSE 
    END AS es_contacto_no_efectivo,
    CASE WHEN hom_humano.pdp = '1' OR hom_humano.pdp = 'SI' THEN TRUE ELSE FALSE END AS es_compromiso,
    COALESCE(hom_humano.peso, 1) as peso_gestion

  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e` AS humano
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` AS hom_humano
    ON humano.n1 = hom_humano.n_1 and humano.n2 = hom_humano.n_2 and humano.n3 = hom_humano.n_3
  WHERE humano.date >= '2025-05-14'
    AND DATE(humano.date) <= '{fecha_proceso}'
),

-- 💰 PAGOS ÚNICOS (basado en referencia)
pagos_unicos as (
  SELECT 
    nro_documento, 
    monto_cancelado, 
    fecha_pago
  FROM `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos` 
  WHERE fecha_pago <= '{fecha_proceso}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY nro_documento, fecha_pago, CAST(monto_cancelado AS STRING) 
    ORDER BY creado_el DESC
  ) = 1
),

-- 🎯 BASE CONSOLIDADA POR CUENTA (granularidad para dashboard)
base_cuenta_dashboard as (
  SELECT 
    -- 📅 DIMENSIONES TEMPORALES
    ce.PERIODO,
    ce.PERIODO_DATE,
    ce.TIPO_SEGMENTO,
    
    -- 🏷️ DIMENSIONES DE CAMPAÑA 
    ce.ARCHIVO,
    ce.TIPO_CARTERA,
    
    -- 🎯 CARTERA DERIVADA (basada en referencia)
    CASE
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(al.archivo, '')), 'TEMPRANA') THEN 'TEMPRANA'
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(al.archivo, '')), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO'
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(al.archivo, '')), 'AN') THEN 'ALTAS_NUEVAS'
      ELSE 'OTRAS'
    END AS CARTERA,
    
    -- 🎯 DIMENSIONES DE NEGOCIO (servicio)
    al.negocio as SERVICIO,
    al.zona,
    al.tramo_gestion,
    
    -- 🎯 RANGO VENCIMIENTO (para filtros)
    CASE 
      WHEN DATE_DIFF('{fecha_proceso}', al.min_vto, DAY) <= 30 THEN '0-30'
      WHEN DATE_DIFF('{fecha_proceso}', al.min_vto, DAY) <= 60 THEN '31-60'
      WHEN DATE_DIFF('{fecha_proceso}', al.min_vto, DAY) <= 90 THEN '61-90'
      WHEN DATE_DIFF('{fecha_proceso}', al.min_vto, DAY) <= 180 THEN '91-180'
      ELSE '180+'
    END AS RANGO_VENCIMIENTO,
    
    -- 🎯 GRANULARIDAD: CUENTA
    al.cuenta,
    al.cod_luna,
    
    -- 💰 DEUDA (inicial = actual por simplicidad POC)
    COALESCE(dl.monto_exigible_total, 0) as deuda_inicial,
    COALESCE(dl.monto_exigible_total, 0) as deuda_actual,
    
    -- 📊 GESTIONABILIDAD (reglas de negocio)
    CASE 
      WHEN dl.monto_exigible_total IS NULL THEN 0
      WHEN dl.monto_exigible_total < 1 THEN 0
      ELSE 1
    END AS es_gestionable
    
  FROM calendario_expandido ce
  INNER JOIN asignacion_limpia al
    ON al.archivo = CONCAT(ce.ARCHIVO, ".txt")
  LEFT JOIN deuda_limpia dl
    ON CAST(al.cuenta AS STRING) = dl.cod_cuenta
    AND al.fecha_asignacion_real = dl.fecha_trandeuda_real
)

-- 📊 RESULTADO: MÉTRICAS BASE AGREGADAS PARA DASHBOARD
SELECT 
  -- 📅 DIMENSIONES PARA FILTROS CRUZADOS
  '{fecha_proceso}' as fecha_foto,
  PERIODO,
  PERIODO_DATE,
  ARCHIVO,
  TIPO_CARTERA,
  CARTERA,
  SERVICIO,
  TIPO_SEGMENTO,
  RANGO_VENCIMIENTO,
  zona,
  tramo_gestion,
  
  -- 📊 MÉTRICAS BASE DE VOLUMEN 
  COUNT(DISTINCT cuenta) as cuentas_asignadas,
  COUNT(DISTINCT cod_luna) as clientes_asignados,
  COUNT(DISTINCT CASE WHEN es_gestionable = 1 THEN cuenta END) as cuentas_gestionables,
  
  -- 💰 MÉTRICAS BASE FINANCIERAS
  SUM(deuda_inicial) as deuda_inicial_total,
  SUM(deuda_actual) as deuda_actual_total,
  
  -- 🎯 MÉTRICAS BASE DE GESTIÓN (heredadas del cliente a cuentas)
  COUNT(DISTINCT CASE WHEN g.cod_luna IS NOT NULL THEN bcd.cuenta END) as cuentas_gestionadas,
  COUNT(DISTINCT CASE WHEN g.cod_luna IS NOT NULL THEN g.cod_luna END) as clientes_gestionados,
  
  -- 📊 CONTADORES DE GESTIONES 
  COUNT(g.cod_luna) as total_gestiones,
  SUM(CASE WHEN g.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos_total,
  SUM(CASE WHEN g.es_contacto_no_efectivo THEN 1 ELSE 0 END) as contactos_no_efectivos_total,
  SUM(CASE WHEN g.es_compromiso THEN 1 ELSE 0 END) as total_compromisos,
  
  -- 🎯 CUENTAS CON TIPOS DE CONTACTO (heredado del cliente)
  COUNT(DISTINCT CASE WHEN g.es_contacto_efectivo THEN bcd.cuenta END) as cuentas_con_contacto_directo,
  COUNT(DISTINCT CASE WHEN g.es_contacto_no_efectivo THEN bcd.cuenta END) as cuentas_con_contacto_indirecto,
  COUNT(DISTINCT CASE WHEN (g.es_contacto_efectivo OR g.es_contacto_no_efectivo) THEN bcd.cuenta END) as cuentas_con_contacto_total,
  COUNT(DISTINCT CASE WHEN g.es_compromiso THEN bcd.cuenta END) as cuentas_con_compromiso,
  
  -- 🎯 RECUPERO (por documento de cuenta)
  COUNT(DISTINCT CASE WHEN p.nro_documento IS NOT NULL THEN bcd.cuenta END) as cuentas_pagadoras,
  COALESCE(SUM(p.monto_cancelado), 0) as recupero_total,
  COUNT(p.nro_documento) as pagos_totales,
  
  -- 📊 CHANNEL BREAKDOWN
  SUM(CASE WHEN g.canal_origen = 'BOT' THEN 1 ELSE 0 END) as gestiones_bot_total,
  SUM(CASE WHEN g.canal_origen = 'HUMANO' THEN 1 ELSE 0 END) as gestiones_humano_total,
  SUM(CASE WHEN g.canal_origen = 'BOT' THEN g.peso_gestion ELSE 0 END) as peso_bot_total,
  SUM(CASE WHEN g.canal_origen = 'HUMANO' THEN g.peso_gestion ELSE 0 END) as peso_humano_total,
  SUM(g.peso_gestion) as peso_total

FROM base_cuenta_dashboard bcd
-- 🎯 Gestiones heredadas del cliente a todas sus cuentas
LEFT JOIN gestiones_unificadas g 
  ON bcd.cod_luna = g.cod_luna
-- 💰 Pagos específicos por documento/cuenta  
LEFT JOIN pagos_unicos p
  ON bcd.cuenta = p.nro_documento

GROUP BY 
  fecha_foto, PERIODO, PERIODO_DATE, ARCHIVO, TIPO_CARTERA, CARTERA, 
  SERVICIO, TIPO_SEGMENTO, RANGO_VENCIMIENTO, zona, tramo_gestion

HAVING cuentas_asignadas > 0

ORDER BY 
  PERIODO_DATE DESC,
  CARTERA,
  SERVICIO,
  ARCHIVO