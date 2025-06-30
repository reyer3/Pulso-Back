-- Builds the final dashboard_data mart for a specific day and campaign.
-- Parameters:
-- {mart_schema}: e.g., mart_P3fV4dWNeMkN5RJMhV8e
-- {raw_schema}: e.g., raw_P3fV4dWNeMkN5RJMhV8e
-- {aux_schema}: e.g., aux_P3fV4dWNeMkN5RJMhV8e
-- $1: campaign_archivo (TEXT)
-- $2: fecha_proceso (DATE)

DELETE FROM {mart_schema}.dashboard_data WHERE archivo = $1 AND fecha_foto = $2;

INSERT INTO {mart_schema}.dashboard_data (fecha_foto, archivo, cartera, servicio, clientes, cuentas, deuda_asig, recupero, cuentas_gestionadas, cuentas_cd)
WITH campana_base AS (
    SELECT
        a.archivo, c.tipo_cartera as cartera, a.negocio as servicio,
        COUNT(DISTINCT a.cod_luna) as clientes,
        COUNT(a.cuenta) as cuentas,
        SUM(td.monto_exigible) as deuda_asig
    FROM {raw_schema}.asignaciones a
    JOIN {raw_schema}.calendario c ON a.archivo = c.archivo
    LEFT JOIN {raw_schema}.trandeuda td ON a.cuenta = td.cod_cuenta AND a.archivo = td.archivo
    WHERE a.archivo = $1
    GROUP BY 1, 2, 3
),
kpis_diarios AS (
    SELECT
        COALESCE(SUM(monto_pagado), 0) as recupero_dia
    FROM {aux_schema}.pagos_diarios
    WHERE archivo_campana = $1 AND fecha_pago = $2
),
kpis_gestiones AS (
     SELECT
         COUNT(DISTINCT cod_luna) as cuentas_gestionadas_dia,
         SUM(total_contactos_efectivos) as contactos_efectivos_dia
     FROM {aux_schema}.gestiones_diarias
     WHERE archivo_campana = $1 AND fecha_gestion = $2
)
SELECT
    $2,
    b.archivo,
    b.cartera,
    b.servicio,
    b.clientes,
    b.cuentas,
    COALESCE(b.deuda_asig, 0),
    COALESCE((SELECT recupero_dia FROM kpis_diarios), 0),
    COALESCE((SELECT cuentas_gestionadas_dia FROM kpis_gestiones), 0),
    COALESCE((SELECT contactos_efectivos_dia FROM kpis_gestiones), 0)
FROM campana_base b;