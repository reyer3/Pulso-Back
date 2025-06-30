-- Evolution Data Source Query
-- Extracts time series data for trend analysis from dashboard_data mart
-- Schemas: {mart_schema}
-- Parameters: {fecha_proceso}, {lookback_days} (default 30)

WITH date_range AS (
    SELECT 
        '{fecha_proceso}'::date as end_date,
        '{fecha_proceso}'::date - INTERVAL '{lookback_days} days' as start_date
),

daily_snapshots AS (
    SELECT 
        dd.fecha_foto,
        dd.archivo,
        dd.cartera,
        dd.servicio,
        dd.pct_cober,
        dd.pct_contac,
        dd.pct_efectividad,
        dd.pct_cierre,
        dd.recupero,
        dd.cuentas,
        
        -- Calculate day sequence for trend analysis
        ROW_NUMBER() OVER (
            PARTITION BY dd.archivo, dd.cartera, dd.servicio 
            ORDER BY dd.fecha_foto
        ) as day_sequence,
        
        -- Days from start for regression calculations
        EXTRACT(EPOCH FROM (dd.fecha_foto - dr.start_date)) / 86400 as days_from_start
        
    FROM {mart_schema}.dashboard_data dd
    CROSS JOIN date_range dr
    WHERE dd.fecha_foto BETWEEN dr.start_date AND dr.end_date
        AND ({archivo} IS NULL OR dd.archivo = '{archivo}')
    ORDER BY dd.archivo, dd.cartera, dd.servicio, dd.fecha_foto
)

SELECT 
    fecha_foto,
    archivo,
    cartera,
    servicio,
    pct_cober,
    pct_contac,
    pct_efectividad,
    pct_cierre,
    recupero,
    cuentas,
    day_sequence,
    days_from_start,
    
    -- Window functions for moving averages (7-day window)
    AVG(pct_cober) OVER (
        PARTITION BY archivo, cartera, servicio 
        ORDER BY fecha_foto 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as pct_cober_7d_avg,
    
    AVG(pct_contac) OVER (
        PARTITION BY archivo, cartera, servicio 
        ORDER BY fecha_foto 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as pct_contac_7d_avg,
    
    AVG(pct_efectividad) OVER (
        PARTITION BY archivo, cartera, servicio 
        ORDER BY fecha_foto 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as pct_efectividad_7d_avg,
    
    -- Standard deviation for volatility calculation
    STDDEV(pct_cober) OVER (
        PARTITION BY archivo, cartera, servicio 
        ORDER BY fecha_foto 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as pct_cober_volatility,
    
    STDDEV(pct_contac) OVER (
        PARTITION BY archivo, cartera, servicio 
        ORDER BY fecha_foto 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as pct_contac_volatility,
    
    STDDEV(pct_efectividad) OVER (
        PARTITION BY archivo, cartera, servicio 
        ORDER BY fecha_foto 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as pct_efectividad_volatility

FROM daily_snapshots
WHERE day_sequence >= 2  -- Need at least 2 points for trend analysis
ORDER BY archivo, cartera, servicio, fecha_foto