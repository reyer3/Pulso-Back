-- Builds the final dashboard_data mart for a specific day and campaign.
-- This query is complex and calculates all necessary KPIs.
-- Parameters:
-- {mart_schema}, {raw_schema}, {aux_schema}
-- $1: campaign_archivo (TEXT)
-- $2: fecha_proceso (DATE)

DELETE FROM {mart_schema}.dashboard_data WHERE archivo = $1 AND fecha_foto = $2;

INSERT INTO {mart_schema}.dashboard_data (
    fecha_foto, archivo, cartera, servicio, clientes, cuentas, deuda_asig, deuda_act,
    cuentas_gestionadas, cuentas_cd, cuentas_ci, cuentas_sc, cuentas_sg, cuentas_pdp,
    recupero, pct_cober, pct_contac, pct_cd, pct_ci, pct_conversion, pct_efectividad, pct_cierre, inten
)
WITH
base_asignacion AS (
    -- Universo de cuentas y clientes para la campaña
    SELECT
        a.archivo,
        c.tipo_cartera AS cartera,
        a.negocio AS servicio,
        COUNT(DISTINCT a.cod_luna) AS clientes,
        COUNT(DISTINCT a.cuenta) AS cuentas
    FROM {raw_schema}.asignaciones a
    JOIN {raw_schema}.calendario c ON a.archivo = c.archivo
    WHERE a.archivo = $1
    GROUP BY 1, 2, 3
),
deuda AS (
    -- Deuda asignada (inicial) y actual
    SELECT
        a.archivo,
        SUM(CASE WHEN td.fecha_proceso = c.fecha_apertura THEN td.monto_exigible ELSE 0 END) as deuda_asig,
        SUM(CASE WHEN td.fecha_proceso = $2 THEN td.monto_exigible ELSE 0 END) as deuda_act
    FROM {raw_schema}.asignaciones a
    JOIN {raw_schema}.calendario c ON a.archivo = c.archivo
    JOIN {raw_schema}.trandeuda td ON a.cuenta = td.cod_cuenta AND a.archivo = td.archivo
    WHERE a.archivo = $1 AND td.fecha_proceso IN (c.fecha_apertura, $2)
    GROUP BY 1
),
kpis_gestiones AS (
    -- KPIs de gestión acumulados hasta la fecha de proceso
    SELECT
        archivo_campana AS archivo,
        COUNT(DISTINCT cod_luna) as cuentas_gestionadas,
        SUM(CASE WHEN contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END) as cuentas_cd,
        SUM(CASE WHEN contactabilidad = 'Contacto No Efectivo' THEN 1 ELSE 0 END) as cuentas_ci,
        SUM(CASE WHEN contactabilidad = 'SIN_CLASIFICAR' THEN 1 ELSE 0 END) as cuentas_sc,
        SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END) as cuentas_pdp,
        COUNT(gestion_uid) as total_gestiones
    FROM {aux_schema}.gestion_cuenta_impact
    WHERE archivo_campana = $1 AND fecha_gestion <= $2
    GROUP BY 1
),
kpis_pagos AS (
    -- Recupero acumulado hasta la fecha de proceso
    SELECT
        archivo_campana AS archivo,
        SUM(monto_pagado) as recupero
    FROM {aux_schema}.pagos_diarios
    WHERE archivo_campana = $1 AND fecha_pago <= $2
    GROUP BY 1
)
SELECT
    $2 AS fecha_foto,
    b.archivo,
    b.cartera,
    b.servicio,
    b.clientes,
    b.cuentas,
    COALESCE(d.deuda_asig, 0) AS deuda_asig,
    COALESCE(d.deuda_act, 0) AS deuda_act,
    COALESCE(g.cuentas_gestionadas, 0) AS cuentas_gestionadas,
    COALESCE(g.cuentas_cd, 0) AS cuentas_cd,
    COALESCE(g.cuentas_ci, 0) AS cuentas_ci,
    COALESCE(g.cuentas_sc, 0) AS cuentas_sc,
    b.cuentas - COALESCE(g.cuentas_gestionadas, 0) AS cuentas_sg, -- Cuentas Sin Gestión
    COALESCE(g.cuentas_pdp, 0) AS cuentas_pdp,
    COALESCE(p.recupero, 0) AS recupero,
    -- Percentages
    (COALESCE(g.cuentas_gestionadas, 0) * 100.0 / NULLIF(b.cuentas, 0)) AS pct_cober,
    (COALESCE(g.cuentas_cd, 0) * 100.0 / NULLIF(g.total_gestiones, 0)) AS pct_contac,
    (COALESCE(g.cuentas_cd, 0) * 100.0 / NULLIF(b.cuentas, 0)) AS pct_cd,
    (COALESCE(g.cuentas_ci, 0) * 100.0 / NULLIF(b.cuentas, 0)) AS pct_ci,
    (COALESCE(g.cuentas_pdp, 0) * 100.0 / NULLIF(g.cuentas_cd, 0)) AS pct_conversion,
    (COALESCE(g.cuentas_pdp, 0) * 100.0 / NULLIF(g.cuentas_gestionadas, 0)) AS pct_efectividad,
    (COALESCE(p.recupero, 0) * 100.0 / NULLIF(d.deuda_asig, 0)) AS pct_cierre,
    (COALESCE(g.total_gestiones, 0) * 1.0 / NULLIF(g.cuentas_gestionadas, 0)) AS inten
FROM base_asignacion b
LEFT JOIN deuda d ON b.archivo = d.archivo
LEFT JOIN kpis_gestiones g ON b.archivo = g.archivo
LEFT JOIN kpis_pagos p ON b.archivo = p.archivo;