-- Unifies Voicebot and MibotAir gestiones into the auxiliary table for a specific campaign.
-- Parameters:
-- {aux_schema}: The auxiliary schema name (e.g., aux_P3fV4dWNeMkN5RJMhV8e)
-- {raw_schema}: The raw schema name (e.g., raw_P3fV4dWNeMkN5RJMhV8e)
-- $1: campaign_archivo (TEXT)

-- Idempotency: Clean previous data for this campaign
DELETE FROM {aux_schema}.gestiones_unificadas WHERE archivo_campana = $1;

INSERT INTO {aux_schema}.gestiones_unificadas (
    gestion_uid, cod_luna, timestamp_gestion, fecha_gestion, canal_origen,
    nivel_1, nivel_2, nivel_3, contactabilidad, es_contacto_efectivo,
    es_compromiso, monto_compromiso, fecha_compromiso, archivo_campana
)
-- Voicebot Gestiones
SELECT
    g.uid, a.cod_luna, g."date", DATE(g."date"), 'BOT',
    h.n1_homologado, h.n2_homologado, h.n3_homologado,
    COALESCE(h.contactabilidad_homologada, 'SIN_CLASIFICAR'),
    (h.contactabilidad_homologada = 'Contacto Efectivo'),
    COALESCE(h.es_pdp_homologado, FALSE),
    NULL, g.fecha_compromiso, a.archivo
FROM {raw_schema}.voicebot_gestiones g
JOIN {raw_schema}.asignaciones a ON g.document = a.dni AND a.archivo = $1
LEFT JOIN {raw_schema}.homologacion_voicebot h ON g.management = h.bot_management AND g.sub_management = h.bot_sub_management AND g.compromiso = h.bot_compromiso

UNION ALL

-- MibotAir Gestiones
SELECT
    g.uid, a.cod_luna, g."date", DATE(g."date"), 'HUMANO',
    h.n_1, h.n_2, h.n_3,
    COALESCE(h.contactabilidad, 'SIN_CLASIFICAR'),
    (h.contactabilidad = 'Contacto Efectivo'),
    (UPPER(h.pdp) = 'SI'),
    g.monto_compromiso, g.fecha_compromiso, a.archivo
FROM {raw_schema}.mibotair_gestiones g
JOIN {raw_schema}.asignaciones a ON g.document = a.dni AND a.archivo = $1
LEFT JOIN {raw_schema}.homologacion_mibotair h ON g.n1 = h.n_1 AND g.n2 = h.n_2 AND g.n3 = h.n_3;