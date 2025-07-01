-- UPSERT INTO aux_p3fv4dwnemkn5rjmhv8e.gestiones_unificadas
-- Voicebot y MibotAir Gestiones
INSERT INTO aux_p3fv4dwnemkn5rjmhv8e.gestiones_unificadas (
    gestion_uid, cod_luna, cuenta, timestamp_gestion, fecha_gestion,
    canal_origen, nombre_agente, documento_agente,
    nivel_1, nivel_2, nivel_3, contactabilidad,
    es_contacto_efectivo, es_compromiso, monto_compromiso,
    fecha_compromiso, archivo_campana, peso
)
-- Voicebot Gestiones
SELECT DISTINCT
    g.uid,
    a.cod_luna,
    a.cuenta,
    g."date",
    DATE(g."date"),
    'BOT',
    'VOICEBOT',
    '999',
    h.n1_homologado,
    h.n2_homologado,
    h.n3_homologado,
    COALESCE(h.contactabilidad_homologada, 'Sin Homologar'),
    (h.contactabilidad_homologada = 'Contacto Efectivo'),
    COALESCE(h.es_pdp_homologado, FALSE),
    CASE
        WHEN h.es_pdp_homologado THEN td.monto_exigible
        ELSE 0
    END,
    g.fecha_compromiso,
    a.archivo,
    h.peso_homologado
FROM raw_p3fv4dwnemkn5rjmhv8e.asignaciones a
INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.calendario c
    ON a.archivo = CONCAT(c.archivo, '.txt')
INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.voicebot_gestiones g
    ON g.document = a.cod_luna
    AND g."date" BETWEEN c.fecha_apertura
    AND COALESCE(c.fecha_cierre, (CURRENT_DATE AT TIME ZONE 'America/Lima'))
LEFT JOIN raw_p3fv4dwnemkn5rjmhv8e.trandeuda td
    ON td.cod_cuenta = a.cuenta
    AND DATE(td.creado_el) > DATE(c.fecha_trandeuda)
    AND DATE(g."date") = c.fecha_trandeuda
LEFT JOIN raw_p3fv4dwnemkn5rjmhv8e.homologacion_voicebot h
    ON g.management = h.bot_management
    AND COALESCE(g.compromiso, '') = h.bot_compromiso

UNION ALL

-- MibotAir Gestiones
SELECT DISTINCT
    g.uid,
    a.cod_luna,
    a.cuenta,
    g."date",
    DATE(g."date"),
    'CALL',
    e.nombre,
    e.document,
    h.n_1,
    h.n_2,
    h.n_3,
    COALESCE(h.contactabilidad, 'Sin Homologar'),
    (h.contactabilidad = 'Contacto Efectivo'),
    (UPPER(h.pdp) = 'SI'),
    CASE
        WHEN UPPER(h.pdp) = 'SI' THEN td.monto_exigible
        ELSE 0
    END,
    g.fecha_compromiso,
    a.archivo,
    h.peso
FROM raw_p3fv4dwnemkn5rjmhv8e.asignaciones a
INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.calendario c
    ON a.archivo = CONCAT(c.archivo, '.txt')
INNER JOIN raw_p3fv4dwnemkn5rjmhv8e.mibotair_gestiones g
    ON g.document = a.cod_luna
    AND g."date" BETWEEN c.fecha_apertura
    AND COALESCE(c.fecha_cierre, (CURRENT_DATE AT TIME ZONE 'America/Lima'))
LEFT JOIN raw_p3fv4dwnemkn5rjmhv8e.trandeuda td
    ON td.cod_cuenta = a.cuenta
    AND DATE(td.creado_el) > DATE(c.fecha_trandeuda)
    AND DATE(g."date") = c.fecha_trandeuda
LEFT JOIN raw_p3fv4dwnemkn5rjmhv8e.homologacion_mibotair h
    ON g.n1 = h.n_1 AND g.n2 = h.n_2 AND g.n3 = h.n_3
LEFT JOIN raw_p3fv4dwnemkn5rjmhv8e.ejecutivos e
    ON e.correo_name = g.correo_agente

-- UPSERT: En caso de conflicto con PK, actualizar todos los campos excepto la PK
ON CONFLICT (gestion_uid, timestamp_gestion, cuenta)
DO NOTHING;