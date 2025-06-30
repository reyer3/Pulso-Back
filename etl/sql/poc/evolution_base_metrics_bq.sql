-- Evolution Base Metrics (POC BigQuery Direct)
-- Series temporales diarias basadas en query de referencia
-- Para gráficos de evolución con filtros dinámicos

WITH 
-- Reutilizar CTEs de calendario y asignación de la query de referencia
calendario_expandido as (
  select 
    c.ARCHIVO,
    c.TIPO_CARTERA,
    c.FECHA_ASIGNACION as fecha_asignacion_campana,
    c.FECHA_CIERRE,
    c.DURACION_CAMPANA_DIAS_HABILES,
    periodo_individual as PERIODO,
    DATE(EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', periodo_individual)), 
         EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', periodo_individual)), 
         1) as PERIODO_DATE,
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
  qualify ROW_NUMBER() OVER (
    PARTITION BY c.ARCHIVO, periodo_individual ORDER BY c.FECHA_ASIGNACION
  ) = 1
),

-- 📅 DÍAS HÁBILES DE CAMPAÑA (expandido de referencia)
dias_habiles_campana as (
  select 
    ce.ARCHIVO,
    ce.PERIODO,
    ce.PERIODO_DATE,
    ce.TIPO_CARTERA,
    ce.TIPO_SEGMENTO,
    ce.fecha_asignacion_campana,
    ce.FECHA_CIERRE,
    fecha_serie as fecha_foto,
    
    ROW_NUMBER() OVER (
      PARTITION BY ce.ARCHIVO ORDER BY fecha_serie
    ) AS dia_campana
    
  from calendario_expandido ce
  cross join unnest(
    generate_date_array(
      ce.fecha_asignacion_campana,
      LEAST(
        coalesce(ce.FECHA_CIERRE, '{fecha_fin}'), 
        '{fecha_fin}'
      ),
      interval 1 day
    )
  ) as fecha_serie
  where fecha_serie >= '{fecha_inicio}'
    AND fecha_serie <= '{fecha_fin}'
    AND extract(dayofweek from fecha_serie) between 2 and 6
),

asignacion_limpia as (
  select 
    a.archivo,
    a.cod_luna,
    a.cuenta,
    IF(a.negocio="MOVIL", a.negocio, "FIJA") as negocio,
    DATE(a.creado_el) as fecha_asignacion_real
  from `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` a
  where a.archivo IN (
    select concat(ce.ARCHIVO, ".txt") from calendario_expandido ce
  ) 
  AND a.creado_el >= '2025-06-11'
  qualify ROW_NUMBER() OVER (
    PARTITION BY a.archivo, a.cuenta ORDER BY a.creado_el DESC
  ) = 1
),

gestiones_unificadas as (
  SELECT
    SAFE_CAST(bot.document AS INT64) AS cod_luna,
    DATE(bot.date) AS fecha_gestion,
    'BOT' AS canal_origen,
    CASE 
      WHEN hom_bot.contactabilidad_homologada = 'Contacto Efectivo' THEN TRUE 
      ELSE FALSE 
    END AS es_contacto_efectivo,
    CASE WHEN hom_bot.es_pdp_homologado = 1 THEN TRUE ELSE FALSE END AS es_compromiso

  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e` AS bot
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` AS hom_bot
    ON bot.management = hom_bot.bot_management 
    AND COALESCE(bot.sub_management, '') = COALESCE(hom_bot.bot_sub_management, '')
    AND COALESCE(bot.compromiso, '') = COALESCE(hom_bot.bot_compromiso, '')
  WHERE bot.date >= '2025-05-14'
    AND DATE(bot.date) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'

  UNION ALL

  SELECT
    SAFE_CAST(humano.document AS INT64) AS cod_luna,
    DATE(humano.date) AS fecha_gestion,
    'HUMANO' AS canal_origen,
    CASE WHEN humano.n1 = 'Contacto_Efectivo' THEN TRUE ELSE FALSE END AS es_contacto_efectivo,
    CASE WHEN hom_humano.pdp = '1' OR hom_humano.pdp = 'SI' THEN TRUE ELSE FALSE END AS es_compromiso

  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e` AS humano
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` AS hom_humano
    ON humano.n1 = hom_humano.n_1 and humano.n2 = hom_humano.n_2 and humano.n3 = hom_humano.n_3
  WHERE humano.date >= '2025-05-14'
    AND DATE(humano.date) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
),

pagos_unicos as (
  SELECT 
    nro_documento, 
    monto_cancelado, 
    fecha_pago
  FROM `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos` 
  WHERE fecha_pago BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY nro_documento, fecha_pago, CAST(monto_cancelado AS STRING) 
    ORDER BY creado_el DESC
  ) = 1
)

-- 📊 EVOLUTION METRICS POR DÍA
SELECT 
  -- 📅 DIMENSIONES TEMPORALES PARA EVOLUCIÓN
  dhc.fecha_foto,
  dhc.dia_campana,
  dhc.PERIODO,
  dhc.PERIODO_DATE,
  dhc.ARCHIVO,
  dhc.TIPO_CARTERA,
  
  -- 🎯 CARTERA DERIVADA
  CASE
    WHEN CONTAINS_SUBSTR(UPPER(COALESCE(al.archivo, '')), 'TEMPRANA') THEN 'TEMPRANA'
    WHEN CONTAINS_SUBSTR(UPPER(COALESCE(al.archivo, '')), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO'
    WHEN CONTAINS_SUBSTR(UPPER(COALESCE(al.archivo, '')), 'AN') THEN 'ALTAS_NUEVAS'
    ELSE 'OTRAS'
  END AS CARTERA,
  
  al.negocio as SERVICIO,
  dhc.TIPO_SEGMENTO,
  
  -- 📊 MÉTRICAS DIARIAS (para el día específico)
  COUNT(DISTINCT CASE WHEN g.fecha_gestion = dhc.fecha_foto THEN al.cuenta END) as cuentas_gestionadas_dia,
  COUNT(CASE WHEN g.fecha_gestion = dhc.fecha_foto THEN g.cod_luna END) as gestiones_dia,
  SUM(CASE WHEN g.fecha_gestion = dhc.fecha_foto AND g.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos_dia,
  SUM(CASE WHEN g.fecha_gestion = dhc.fecha_foto AND g.es_compromiso THEN 1 ELSE 0 END) as compromisos_dia,
  COALESCE(SUM(CASE WHEN p.fecha_pago = dhc.fecha_foto THEN p.monto_cancelado ELSE 0 END), 0) as recupero_dia,
  
  -- 📊 MÉTRICAS ACUMULADAS (desde inicio de campaña hasta fecha_foto)
  COUNT(DISTINCT CASE WHEN g.fecha_gestion >= dhc.fecha_asignacion_campana 
                       AND g.fecha_gestion <= dhc.fecha_foto THEN al.cuenta END) as cuentas_gestionadas_acum,
  COUNT(CASE WHEN g.fecha_gestion >= dhc.fecha_asignacion_campana 
             AND g.fecha_gestion <= dhc.fecha_foto THEN g.cod_luna END) as gestiones_acum,
  SUM(CASE WHEN g.fecha_gestion >= dhc.fecha_asignacion_campana 
           AND g.fecha_gestion <= dhc.fecha_foto 
           AND g.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos_acum,
  SUM(CASE WHEN g.fecha_gestion >= dhc.fecha_asignacion_campana 
           AND g.fecha_gestion <= dhc.fecha_foto 
           AND g.es_compromiso THEN 1 ELSE 0 END) as compromisos_acum,
  COALESCE(SUM(CASE WHEN p.fecha_pago >= dhc.fecha_asignacion_campana 
                    AND p.fecha_pago <= dhc.fecha_foto THEN p.monto_cancelado ELSE 0 END), 0) as recupero_acum,
  
  -- 📊 MÉTRICAS BASE PARA DENOMINADORES
  COUNT(DISTINCT al.cuenta) as cuentas_asignadas

FROM dias_habiles_campana dhc
INNER JOIN asignacion_limpia al
  ON al.archivo = CONCAT(dhc.ARCHIVO, ".txt")
LEFT JOIN gestiones_unificadas g 
  ON al.cod_luna = g.cod_luna
LEFT JOIN pagos_unicos p
  ON al.cuenta = p.nro_documento

GROUP BY 
  dhc.fecha_foto, dhc.dia_campana, dhc.PERIODO, dhc.PERIODO_DATE, 
  dhc.ARCHIVO, dhc.TIPO_CARTERA, CARTERA, al.negocio, dhc.TIPO_SEGMENTO,
  dhc.fecha_asignacion_campana

HAVING cuentas_asignadas > 0

ORDER BY 
  dhc.fecha_foto,
  dhc.ARCHIVO,
  CARTERA,
  al.negocio