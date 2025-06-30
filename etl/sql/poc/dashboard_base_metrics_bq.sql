-- =====================================================
-- Dashboard Base Metrics - VERSIÓN OPTIMIZADA
-- Optimizaciones: Performance, Calidad de Datos, Escalabilidad
-- Autor: Optimización basada en query original
-- =====================================================

WITH

-- 📅 CALENDARIO DE FERIADOS PERÚ (mejora crítica para días hábiles)
feriados_peru AS (
  SELECT fecha_feriado FROM (
    SELECT DATE('2025-01-01') AS fecha_feriado UNION ALL -- Año Nuevo
    SELECT DATE('2025-04-17') UNION ALL -- Jueves Santo
    SELECT DATE('2025-04-18') UNION ALL -- Viernes Santo
    SELECT DATE('2025-05-01') UNION ALL -- Día del Trabajo
    SELECT DATE('2025-06-29') UNION ALL -- San Pedro y San Pablo
    SELECT DATE('2025-07-28') UNION ALL -- Independencia del Perú
    SELECT DATE('2025-07-29') UNION ALL -- Día de la Patria
    SELECT DATE('2025-08-30') UNION ALL -- Santa Rosa de Lima
    SELECT DATE('2025-10-08') UNION ALL -- Combate de Angamos
    SELECT DATE('2025-11-01') UNION ALL -- Todos los Santos
    SELECT DATE('2025-12-08') UNION ALL -- Inmaculada Concepción
    SELECT DATE('2025-12-25')          -- Navidad
  )
),

-- 🔍 VALIDACIÓN Y LIMPIEZA DE CALENDARIO BASE
calendario_validado AS (
  SELECT
    c.ARCHIVO,
    c.TIPO_CARTERA,
    c.fecha_apertura,
    c.FECHA_TRANDEUDA AS fecha_trandeuda_campana,
    COALESCE(c.FECHA_CIERRE, c.FECHA_CIERRE_PLANIFICADA) AS fecha_cierre_efectiva,
    c.DURACION_CAMPANA_DIAS_HABILES,
    c.ANNO_ASIGNACION,
    c.PERIODO_ASIGNACION,
    
    -- ✅ VALIDACIONES DE CALIDAD
    CASE 
      WHEN c.fecha_apertura IS NULL THEN 'ERROR_FECHA_APERTURA_NULL'
      WHEN c.fecha_apertura > CURRENT_DATE() THEN 'ERROR_FECHA_FUTURA'
      WHEN DATE_DIFF(COALESCE(c.FECHA_CIERRE, c.FECHA_CIERRE_PLANIFICADA), c.fecha_apertura, DAY) > 32 
        THEN 'WARNING_CAMPANA_MUY_LARGA'
      WHEN DATE_DIFF(COALESCE(c.FECHA_CIERRE, c.FECHA_CIERRE_PLANIFICADA), c.fecha_apertura, DAY) < 1 
        THEN 'ERROR_DURACION_INVALIDA'
      ELSE 'VALID'
    END AS validacion_estado
    
  FROM `BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` c
  WHERE c.fecha_apertura >=  DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND c.fecha_apertura <=  CURRENT_DATE()
),

-- 📅 CALENDARIO EXPANDIDO OPTIMIZADO (solo rangos válidos)
calendario_expandido_optimizado AS (
  SELECT
    cv.ARCHIVO,
    cv.TIPO_CARTERA,
    cv.fecha_apertura AS fecha_asignacion_campana,
    cv.fecha_trandeuda_campana,
    cv.fecha_cierre_efectiva,
    cv.DURACION_CAMPANA_DIAS_HABILES,
    cv.ANNO_ASIGNACION,
    cv.PERIODO_ASIGNACION,

    -- 🗓️ Granularidad diaria
    gen_date AS fecha,
    FORMAT_DATE('%Y-%m', gen_date) AS PERIODO,
    DATE_TRUNC(gen_date, MONTH) AS PERIODO_DATE,

    -- 🏷️ Flag de día hábil MEJORADO (excluye feriados)
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM gen_date) IN (1, 7) THEN 0  -- Dom=1, Sab=7
      WHEN f.fecha_feriado IS NOT NULL THEN 0
      ELSE 1
    END AS is_business,

    -- 🔢 Día de gestión acumulado (solo hábiles reales)
    SUM(CASE 
      WHEN EXTRACT(DAYOFWEEK FROM gen_date) NOT IN (1, 7) 
       AND f.fecha_feriado IS NULL THEN 1 
      ELSE 0 
    END) OVER (
      PARTITION BY cv.ARCHIVO, cv.TIPO_CARTERA
      ORDER BY gen_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dia_gestion,

    -- 🔢 Día hábil dentro del mes
    SUM(CASE 
      WHEN EXTRACT(DAYOFWEEK FROM gen_date) NOT IN (1, 7) 
       AND f.fecha_feriado IS NULL THEN 1 
      ELSE 0 
    END) OVER (
      PARTITION BY cv.ARCHIVO, cv.TIPO_CARTERA,
                   EXTRACT(YEAR FROM gen_date),
                   EXTRACT(MONTH FROM gen_date)
      ORDER BY gen_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dia_habil,

    -- 📸 Metadata de proceso
    CURRENT_DATETIME() AS fecha_foto_proceso,
    cv.validacion_estado

  FROM calendario_validado cv
  
  -- Generación controlada de fechas (solo campañas válidas)
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(
      cv.fecha_apertura,
      LEAST(cv.fecha_cierre_efectiva, DATE_ADD(cv.fecha_apertura, INTERVAL 32 DAY))
    )
  ) AS gen_date
  
  -- JOIN optimizado con feriados
  LEFT JOIN feriados_peru f
    ON f.fecha_feriado = gen_date
    
  WHERE cv.validacion_estado IN ('VALID', 'WARNING_CAMPANA_MUY_LARGA')
    AND gen_date >=  DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND gen_date <=  CURRENT_DATE()
),

-- 🏗️ ASIGNACIÓN LIMPIA CON VALIDACIONES (tipos corregidos según schema)
asignacion_limpia_v2 AS (
  SELECT
    a.archivo,
    a.cod_luna,
    a.cuenta,
    a.min_vto,
    CASE 
      WHEN UPPER(TRIM(a.negocio)) = 'MOVIL' THEN 'MOVIL'
      WHEN UPPER(TRIM(a.negocio)) = 'FIJA' THEN 'FIJA'
      WHEN a.negocio IS NULL THEN 'SIN_CLASIFICAR'
      ELSE 'OTROS'
    END AS negocio_normalizado,
    DATE(a.creado_el) AS fecha_asignacion_real,
    a.creado_el AS timestamp_asignacion,
    
    -- Campos adicionales del schema
    a.cliente,
    a.dni,
    a.decil_contacto,
    a.decil_pago,
    a.campania_act,
    a.fraccionamiento,
    a.tipo_alta,
    a.estado_pc,
    
    -- ✅ VALIDACIONES MEJORADAS
    CASE 
      WHEN a.cod_luna IS NULL THEN 'ERROR_COD_LUNA_NULL'
      WHEN a.cuenta IS NULL THEN 'ERROR_CUENTA_NULL'
      WHEN a.creado_el IS NULL THEN 'ERROR_FECHA_NULL'
      WHEN DATE(a.creado_el) > CURRENT_DATE() THEN 'ERROR_FECHA_FUTURA'
      WHEN a.telefono IS NULL OR a.telefono <= 0 THEN 'WARNING_SIN_TELEFONO'
      ELSE 'VALID'
    END AS validacion_asignacion
    
  FROM `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` a
  WHERE a.archivo IN (
    SELECT DISTINCT CONCAT(ce.ARCHIVO, '.txt') 
    FROM calendario_expandido_optimizado ce
  )
    AND a.creado_el >= DATETIME( DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY))
    AND DATE(a.creado_el) <=  CURRENT_DATE()
    
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.archivo, a.cuenta
    ORDER BY a.creado_el DESC
  ) = 1
),

-- 💰 DEUDA CONSOLIDADA CON AGREGACIONES OPTIMIZADAS
deuda_consolidada_v2 AS (
  SELECT
    d.cod_cuenta,
    DATE(d.creado_el) AS fecha_trandeuda_real,
    d.nro_documento,
    d.fecha_vencimiento,
    
    -- Agregaciones optimizadas
    SUM(COALESCE(d.monto_exigible, 0)) AS monto_exigible_total,
    COUNT(*) AS documentos_cuenta,
    MIN(d.fecha_vencimiento) AS fecha_vto_mas_antigua,
    MAX(d.fecha_vencimiento) AS fecha_vto_mas_reciente,
    
    -- Validaciones
    CASE 
      WHEN SUM(COALESCE(d.monto_exigible, 0)) <= 0 THEN 'ERROR_MONTO_INVALIDO'
      WHEN COUNT(*) = 0 THEN 'ERROR_SIN_DOCUMENTOS'
      WHEN d.cod_cuenta IS NULL THEN 'ERROR_CUENTA_NULL'
      ELSE 'VALID'
    END AS validacion_deuda
    
  FROM `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` d
  WHERE d.cod_cuenta IN (
    SELECT DISTINCT CAST(al.cuenta AS STRING)
    FROM asignacion_limpia_v2 al
    WHERE al.validacion_asignacion IN ('VALID', 'WARNING_SIN_TELEFONO')
  )
    AND DATE(d.creado_el) >=  DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND DATE(d.creado_el) <=  CURRENT_DATE()
    
  GROUP BY 
    d.cod_cuenta, 
    DATE(d.creado_el), 
    d.nro_documento, 
    d.fecha_vencimiento
    
  HAVING SUM(COALESCE(d.monto_exigible, 0)) > 0
),

-- 🎯 GESTIONES UNIFICADAS OPTIMIZADAS
gestiones_unificadas_v2 AS (
  -- BOT - Con validaciones mejoradas
  SELECT
    SAFE_CAST(bot.document AS INT64) AS cod_luna,
    DATE(bot.date) AS fecha_gestion,
    'BOT' AS canal_origen,
    COALESCE(bot.management, 'SIN_CLASIFICAR') AS tipo_gestion,
    
    -- Flags optimizados con COALESCE
    COALESCE(hom_bot.contactabilidad_homologada = 'Contacto Efectivo', FALSE) AS es_contacto_efectivo,
    COALESCE(hom_bot.contactabilidad_homologada = 'Contacto No Efectivo', FALSE) AS es_contacto_no_efectivo,
    COALESCE(hom_bot.es_pdp_homologado = 1, FALSE) AS es_compromiso,
    COALESCE(hom_bot.peso_homologado, 1) AS peso_gestion,
    
    -- Validación
    CASE 
      WHEN bot.document IS NULL THEN 'ERROR_DOCUMENT_NULL'
      WHEN bot.date IS NULL THEN 'ERROR_FECHA_NULL'
      WHEN DATE(bot.date) > CURRENT_DATE() THEN 'ERROR_FECHA_FUTURA'
      ELSE 'VALID'
    END AS validacion_gestion
    
  FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e` bot
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` hom_bot
    ON bot.management = hom_bot.bot_management
   AND COALESCE(bot.sub_management, '') = COALESCE(hom_bot.bot_sub_management, '')
   AND COALESCE(bot.compromiso, '') = COALESCE(hom_bot.bot_compromiso, '')
  WHERE DATE(bot.date) >=  DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND DATE(bot.date) <=  CURRENT_DATE()
    AND SAFE_CAST(bot.document AS INT64) IS NOT NULL

  UNION ALL

  -- HUMANO - Con validaciones mejoradas
  SELECT
    SAFE_CAST(humano.document AS INT64) AS cod_luna,
    DATE(humano.date) AS fecha_gestion,
    'HUMANO' AS canal_origen,
    COALESCE(humano.n1, 'SIN_CLASIFICAR') AS tipo_gestion,
    
    -- Flags optimizados
    COALESCE(humano.n1 = 'Contacto_Efectivo', FALSE) AS es_contacto_efectivo,
    COALESCE(humano.n1 = 'Contacto_No_Efectivo', FALSE) AS es_contacto_no_efectivo,
    COALESCE(UPPER(TRIM(hom_humano.pdp)) IN ('1','SI','TRUE'), FALSE) AS es_compromiso,
    COALESCE(SAFE_CAST(hom_humano.peso AS INT64), 1) AS peso_gestion,
    
    -- Validación
    CASE 
      WHEN humano.document IS NULL THEN 'ERROR_DOCUMENT_NULL'
      WHEN humano.date IS NULL THEN 'ERROR_FECHA_NULL'
      WHEN DATE(humano.date) > CURRENT_DATE() THEN 'ERROR_FECHA_FUTURA'
      ELSE 'VALID'
    END AS validacion_gestion
    
  FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e` humano
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` hom_humano
    ON humano.n1 = hom_humano.n_1
   AND humano.n2 = hom_humano.n_2
   AND humano.n3 = hom_humano.n_3
  WHERE DATE(humano.date) >=  DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND DATE(humano.date) <=  CURRENT_DATE()
    AND SAFE_CAST(humano.document AS INT64) IS NOT NULL
),

-- 💰 PAGOS ÚNICOS OPTIMIZADOS
pagos_consolidados_v2 AS (
  SELECT
    dl.cod_cuenta,
    pu.nro_documento,
    pu.fecha_pago,
    SUM(COALESCE(pu.monto_cancelado, 0)) AS monto_cancelado_total,
    COUNT(*) AS transacciones_pago,
    
    -- Validaciones
    CASE 
      WHEN SUM(COALESCE(pu.monto_cancelado, 0)) <= 0 THEN 'ERROR_MONTO_INVALIDO'
      WHEN pu.fecha_pago IS NULL THEN 'ERROR_FECHA_NULL'
      WHEN pu.fecha_pago > CURRENT_DATE() THEN 'ERROR_FECHA_FUTURA'
      ELSE 'VALID'
    END AS validacion_pago
    
  FROM `BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos` pu
  INNER JOIN deuda_consolidada_v2 dl
    ON pu.nro_documento = dl.nro_documento
   AND dl.validacion_deuda = 'VALID'
  WHERE pu.fecha_pago >=  DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND pu.fecha_pago <=  CURRENT_DATE()
    AND COALESCE(pu.monto_cancelado, 0) > 0
    
  GROUP BY 
    dl.cod_cuenta, 
    pu.nro_documento, 
    pu.fecha_pago
    
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pu.nro_documento, pu.fecha_pago
    ORDER BY SUM(COALESCE(pu.monto_cancelado, 0)) DESC
  ) = 1
),

-- 🎯 BASE CONSOLIDADA FINAL (performance optimizado)
base_cuenta_final AS (
  SELECT
    -- 📅 Dimensiones temporales
    ce.PERIODO,
    ce.PERIODO_DATE,
    ce.dia_gestion,
    ce.dia_habil,
    ce.fecha,

    -- 🏷️ Dimensiones de campaña
    ce.ARCHIVO,
    ce.TIPO_CARTERA,
    ce.fecha_foto_proceso,
    -- 🎯 Dimensiones de negocio normalizadas
    al.negocio_normalizado AS SERVICIO,
    al.min_vto AS VENCIMIENTO,

    -- 🎯 Identificadores
    al.cuenta,
    al.cod_luna,

    -- 💰 Métricas financieras
    COALESCE(dl.monto_exigible_total, 0) AS deuda_inicial,
    COALESCE(dl.documentos_cuenta, 0) AS documentos_por_cuenta,

    -- 📊 Flags de calidad
    CASE 
      WHEN dl.monto_exigible_total IS NULL THEN 0
      WHEN dl.monto_exigible_total <= 0 THEN 0
      WHEN dl.validacion_deuda != 'VALID' THEN 0
      ELSE 1
    END AS es_gestionable,
    
    -- Estado de validación general
    CASE 
      WHEN al.validacion_asignacion NOT IN ('VALID', 'WARNING_SIN_TELEFONO') THEN al.validacion_asignacion
      WHEN dl.validacion_de