"""
🎯 ETL Configuration System - RAW SOURCES VERSION
Direct queries to raw BigQuery tables without intermediate views

APPROACH: Build dashboard data directly from raw sources:
- batch_..._asignacion: Assignment data
- batch_..._tran_deuda: Daily debt snapshots  
- batch_..._pagos: Payment transactions
- voicebot_...: Bot management data
- mibotair_...: Human agent management data
- homologacion_...: Business rules mapping
- bi_..._dash_calendario_v5: Campaign calendar
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass


class ExtractionMode(str, Enum):
    """Extraction modes for different scenarios"""
    INCREMENTAL = "incremental"        # Default: Extract only new/changed data
    FULL_REFRESH = "full_refresh"      # Force: Complete table refresh
    SLIDING_WINDOW = "sliding_window"  # Window: Re-process last N days


class TableType(str, Enum):
    """Table types for different dashboard purposes"""
    DASHBOARD = "dashboard"        # Main dashboard aggregation
    EVOLUTION = "evolution"        # Time series data
    ASSIGNMENT = "assignment"      # Monthly comparisons
    OPERATION = "operation"        # Hourly operations data
    PRODUCTIVITY = "productivity"  # Agent performance data


@dataclass
class ExtractionConfig:
    """Configuration for a specific table extraction"""
    
    # Table identification
    table_name: str
    table_type: TableType
    description: str
    
    # Primary key configuration
    primary_key: List[str]
    incremental_column: str
    
    # Extraction strategy
    default_mode: ExtractionMode = ExtractionMode.INCREMENTAL
    lookback_days: int = 7  # Days to re-process for data quality
    batch_size: int = 10000  # Records per batch
    
    # BigQuery specific
    source_dataset: str = "BI_USA"
    source_view: Optional[str] = None
    
    # Quality checks
    required_columns: List[str] = None
    min_expected_records: int = 0
    
    # Scheduling
    refresh_frequency_hours: int = 6  # How often to refresh
    max_execution_time_minutes: int = 30  # Timeout


class ETLConfig:
    """
    Centralized ETL configuration for Pulso Dashboard - RAW SOURCES VERSION
    
    DIRECT QUERIES to raw BigQuery tables without intermediate views.
    All business logic is built directly from source tables.
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    
    # 🔄 EXTRACTION CONFIGURATIONS
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📊 MAIN DASHBOARD DATA
        "dashboard_data": ExtractionConfig(
            table_name="dashboard_data",
            table_type=TableType.DASHBOARD,
            description="Main dashboard metrics built from raw sources",
            primary_key=["fecha_foto", "campaign_name", "cartera", "servicio"],
            incremental_column="fecha_foto",
            lookback_days=7,
            required_columns=[
                "fecha_foto", "campaign_name", "cartera", "servicio", 
                "cuentas", "clientes", "deuda_asig", "recupero"
            ],
            min_expected_records=100
        ),
        
        # 📈 EVOLUTION TIME SERIES
        "evolution_data": ExtractionConfig(
            table_name="evolution_data", 
            table_type=TableType.EVOLUTION,
            description="Time series data from raw sources",
            primary_key=["fecha_foto", "campaign_name"],
            incremental_column="fecha_foto",
            lookback_days=3,
            batch_size=50000,
            required_columns=["fecha_foto", "campaign_name", "pct_cober", "pct_contac", "pct_efectividad"],
            min_expected_records=50
        ),
        
        # 📋 ASSIGNMENT ANALYSIS
        "assignment_data": ExtractionConfig(
            table_name="assignment_data",
            table_type=TableType.ASSIGNMENT,
            description="Assignment analysis from raw batch data",
            primary_key=["periodo", "campaign_name", "cartera"],
            incremental_column="fecha_asignacion",
            lookback_days=30,
            refresh_frequency_hours=24,
            required_columns=["periodo", "campaign_name", "cartera", "cuentas", "clientes", "deuda_asig"],
            min_expected_records=20
        ),
        
        # ⚡ OPERATION HOURLY DATA
        "operation_data": ExtractionConfig(
            table_name="operation_data",
            table_type=TableType.OPERATION,
            description="Hourly operational metrics from raw management data",
            primary_key=["fecha_foto", "hora", "canal", "campaign_name"],
            incremental_column="fecha_foto",
            lookback_days=2,
            batch_size=5000,
            refresh_frequency_hours=2,
            max_execution_time_minutes=15,
            required_columns=["fecha_foto", "hora", "canal", "campaign_name", "total_gestiones"],
            min_expected_records=10
        ),
        
        # 👥 PRODUCTIVITY DATA
        "productivity_data": ExtractionConfig(
            table_name="productivity_data",
            table_type=TableType.PRODUCTIVITY, 
            description="Agent productivity from raw management data",
            primary_key=["fecha_foto", "correo_agente", "hora"],
            incremental_column="fecha_foto",
            lookback_days=5,
            refresh_frequency_hours=8,
            required_columns=["fecha_foto", "correo_agente", "nombre_agente", "total_gestiones"],
            min_expected_records=1
        )
    }
    
    # 🚨 GLOBAL SETTINGS
    DEFAULT_TIMEZONE = "America/Lima"
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 30
    
    # 🎯 EXTRACTION QUERIES - BUILT FROM RAW SOURCES
    EXTRACTION_QUERIES: Dict[str, str] = {
        
        "dashboard_data": f"""
        -- 📊 DASHBOARD DATA - Built from RAW sources
        WITH calendario_base AS (
            SELECT 
                ARCHIVO as campaign_file,
                REGEXP_EXTRACT(ARCHIVO, r'([^/]+)\.txt$') as campaign_name,
                FECHA_ASIGNACION as fecha_apertura,
                COALESCE(FECHA_CIERRE, CURRENT_DATE()) as fecha_cierre,
                TIPO_CARTERA as cartera_tipo,
                RANGO_VENCIMIENTO
            FROM `{PROJECT_ID}.{DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
            WHERE FECHA_ASIGNACION IS NOT NULL
        ),
        
        fechas_expansion AS (
            SELECT 
                cb.*,
                fecha_foto
            FROM calendario_base cb
            CROSS JOIN UNNEST(
                GENERATE_DATE_ARRAY(cb.fecha_apertura, cb.fecha_cierre, INTERVAL 1 DAY)
            ) as fecha_foto
        ),
        
        asignaciones_base AS (
            SELECT 
                a.archivo,
                REGEXP_EXTRACT(a.archivo, r'([^/]+)\.txt$') as campaign_name,
                a.cod_luna,
                a.cuenta,
                DATE(a.creado_el) as fecha_asignacion_real,
                CASE
                    WHEN CONTAINS_SUBSTR(UPPER(COALESCE(a.archivo, '')), 'TEMPRANA') THEN 'TEMPRANA'
                    WHEN CONTAINS_SUBSTR(UPPER(COALESCE(a.archivo, '')), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO'
                    WHEN CONTAINS_SUBSTR(UPPER(COALESCE(a.archivo, '')), 'AN') THEN 'ALTAS_NUEVAS'
                    ELSE 'OTRAS'
                END AS cartera,
                IF(a.negocio="MOVIL", a.negocio, "FIJA") as servicio
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` a
            WHERE DATE(a.creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ),
        
        deuda_diaria AS (
            SELECT 
                CAST(d.cod_cuenta AS STRING) as cuenta,
                DATE(d.creado_el) as fecha_trandeuda,
                d.nro_documento,
                SUM(d.monto_exigible) as monto_exigible_diario
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` d
            WHERE DATE(d.creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
              AND d.monto_exigible > 0
            GROUP BY 1,2,3
        ),
        
        gestiones_bot AS (
            SELECT 
                SAFE_CAST(vb.document AS INT64) AS cod_luna,
                DATE(vb.date) AS fecha_gestion,
                vb.campaign_name,
                hv.contactabilidad_homologada as contactabilidad,
                CASE WHEN hv.es_pdp_homologado = 1 THEN 1 ELSE 0 END as es_compromiso,
                CASE WHEN hv.contactabilidad_homologada = 'Contacto Efectivo' THEN 1 ELSE 0 END as es_contacto_efectivo
            FROM `{PROJECT_ID}.{DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e` vb
            LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` hv
                ON vb.management = hv.bot_management 
                AND COALESCE(vb.sub_management, '') = COALESCE(hv.bot_sub_management, '')
                AND COALESCE(vb.compromiso, '') = COALESCE(hv.bot_compromiso, '')
            WHERE DATE(vb.date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
              AND vb.campaign_name IS NOT NULL
        ),
        
        gestiones_humano AS (
            SELECT 
                SAFE_CAST(mh.document AS INT64) AS cod_luna,
                DATE(mh.date) AS fecha_gestion,
                mh.campaign_name,
                hh.contactabilidad as contactabilidad,
                CASE WHEN hh.pdp IN ('1', 'SI') THEN 1 ELSE 0 END as es_compromiso,
                CASE WHEN hh.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END as es_contacto_efectivo
            FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e` mh
            LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` hh
                ON mh.n1 = hh.n_1 
                AND mh.n2 = hh.n_2 
                AND mh.n3 = hh.n_3
            WHERE DATE(mh.date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
              AND mh.campaign_name IS NOT NULL
        ),
        
        gestiones_unificadas AS (
            SELECT * FROM gestiones_bot
            UNION ALL
            SELECT * FROM gestiones_humano
        ),
        
        pagos_clean AS (
            SELECT 
                p.nro_documento,
                p.fecha_pago,
                SUM(p.monto_cancelado) as monto_cancelado
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_pagos` p
            WHERE p.fecha_pago >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
              AND p.monto_cancelado > 0
            GROUP BY 1,2
        ),
        
        dashboard_base AS (
            SELECT 
                fe.fecha_foto,
                fe.campaign_name,
                ab.cartera,
                ab.servicio,
                
                -- Métricas base de asignación
                COUNT(DISTINCT ab.cuenta) as cuentas,
                COUNT(DISTINCT ab.cod_luna) as clientes,
                SUM(COALESCE(dd.monto_exigible_diario, 0)) as deuda_asig,
                
                -- Métricas de gestión
                COUNT(DISTINCT CASE WHEN gu.cod_luna IS NOT NULL THEN ab.cuenta END) as cuentas_gestionadas,
                COUNT(DISTINCT CASE WHEN gu.es_contacto_efectivo = 1 THEN ab.cuenta END) as cuentas_cd,
                COUNT(DISTINCT CASE WHEN gu.contactabilidad = 'Contacto No Efectivo' THEN ab.cuenta END) as cuentas_ci,
                COUNT(DISTINCT CASE WHEN gu.es_compromiso = 1 THEN ab.cuenta END) as cuentas_pdp,
                COUNT(gu.cod_luna) as total_gestiones,
                
                -- Métricas de recupero
                COALESCE(SUM(pc.monto_cancelado), 0) as recupero,
                COUNT(DISTINCT CASE WHEN pc.monto_cancelado > 0 THEN ab.cuenta END) as cuentas_pagadoras
                
            FROM fechas_expansion fe
            INNER JOIN asignaciones_base ab
                ON fe.campaign_name = ab.campaign_name
            LEFT JOIN deuda_diaria dd
                ON ab.cuenta = dd.cuenta
                AND fe.fecha_foto = dd.fecha_trandeuda
            LEFT JOIN gestiones_unificadas gu
                ON ab.cod_luna = gu.cod_luna
                AND ab.campaign_name = gu.campaign_name
                AND gu.fecha_gestion <= fe.fecha_foto
                AND gu.fecha_gestion >= ab.fecha_asignacion_real
            LEFT JOIN pagos_clean pc
                ON dd.nro_documento = pc.nro_documento
                AND pc.fecha_pago <= fe.fecha_foto
                AND pc.fecha_pago >= ab.fecha_asignacion_real
            
            WHERE {{incremental_filter}}
            GROUP BY 1,2,3,4
            HAVING cuentas > 0
        )
        
        SELECT 
            fecha_foto,
            campaign_name,
            cartera,
            servicio,
            cuentas,
            clientes,
            deuda_asig,
            cuentas_gestionadas,
            cuentas_cd,
            cuentas_ci,
            cuentas_pdp,
            recupero,
            cuentas_pagadoras,
            total_gestiones,
            
            -- KPIs calculados
            SAFE_DIVIDE(cuentas_gestionadas, cuentas) * 100 as pct_cober,
            SAFE_DIVIDE(cuentas_cd + cuentas_ci, cuentas_gestionadas) * 100 as pct_contac,
            SAFE_DIVIDE(cuentas_cd, cuentas_cd + cuentas_ci) * 100 as pct_cd,
            SAFE_DIVIDE(cuentas_ci, cuentas_cd + cuentas_ci) * 100 as pct_ci,
            SAFE_DIVIDE(cuentas_pdp, cuentas_cd) * 100 as pct_conversion,
            SAFE_DIVIDE(total_gestiones, cuentas_gestionadas) as inten,
            SAFE_DIVIDE(recupero, deuda_asig) * 100 as pct_efectividad,
            SAFE_DIVIDE(cuentas_pagadoras, cuentas) * 100 as pct_cierre,
            
            CURRENT_TIMESTAMP() as fecha_procesamiento
            
        FROM dashboard_base
        ORDER BY fecha_foto DESC, campaign_name, cartera, servicio
        """,
        
        "evolution_data": f"""
        -- 📈 EVOLUTION DATA - Simplified from dashboard query
        WITH dashboard_simplified AS (
            -- Reuse the dashboard logic but only for evolution metrics
            SELECT 
                fe.fecha_foto,
                fe.campaign_name,
                ab.cartera,
                ab.servicio,
                COUNT(DISTINCT ab.cuenta) as cuentas,
                COUNT(DISTINCT CASE WHEN gu.cod_luna IS NOT NULL THEN ab.cuenta END) as cuentas_gestionadas,
                COUNT(DISTINCT CASE WHEN gu.es_contacto_efectivo = 1 THEN ab.cuenta END) as cuentas_cd,
                COUNT(DISTINCT CASE WHEN gu.contactabilidad = 'Contacto No Efectivo' THEN ab.cuenta END) as cuentas_ci,
                COALESCE(SUM(pc.monto_cancelado), 0) as recupero,
                SUM(COALESCE(dd.monto_exigible_diario, 0)) as deuda_asig
            FROM (
                SELECT 
                    REGEXP_EXTRACT(ARCHIVO, r'([^/]+)\.txt$') as campaign_name,
                    fecha_foto
                FROM `{PROJECT_ID}.{DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5` cb
                CROSS JOIN UNNEST(
                    GENERATE_DATE_ARRAY(cb.FECHA_ASIGNACION, COALESCE(cb.FECHA_CIERRE, CURRENT_DATE()), INTERVAL 1 DAY)
                ) as fecha_foto
                WHERE cb.FECHA_ASIGNACION IS NOT NULL
            ) fe
            INNER JOIN (
                SELECT 
                    REGEXP_EXTRACT(archivo, r'([^/]+)\.txt$') as campaign_name,
                    cod_luna, cuenta,
                    CASE
                        WHEN CONTAINS_SUBSTR(UPPER(COALESCE(archivo, '')), 'TEMPRANA') THEN 'TEMPRANA'
                        WHEN CONTAINS_SUBSTR(UPPER(COALESCE(archivo, '')), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO' 
                        ELSE 'OTRAS'
                    END AS cartera,
                    IF(negocio="MOVIL", negocio, "FIJA") as servicio
                FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
                WHERE DATE(creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            ) ab ON fe.campaign_name = ab.campaign_name
            LEFT JOIN (
                SELECT 
                    CAST(cod_cuenta AS STRING) as cuenta,
                    DATE(creado_el) as fecha_trandeuda,
                    nro_documento,
                    SUM(monto_exigible) as monto_exigible_diario
                FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
                WHERE DATE(creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                GROUP BY 1,2,3
            ) dd ON ab.cuenta = dd.cuenta AND fe.fecha_foto = dd.fecha_trandeuda
            LEFT JOIN (
                SELECT cod_luna, fecha_gestion, campaign_name, es_contacto_efectivo, contactabilidad
                FROM (
                    SELECT 
                        SAFE_CAST(document AS INT64) AS cod_luna,
                        DATE(date) AS fecha_gestion,
                        campaign_name,
                        CASE WHEN hv.contactabilidad_homologada = 'Contacto Efectivo' THEN 1 ELSE 0 END as es_contacto_efectivo,
                        hv.contactabilidad_homologada as contactabilidad
                    FROM `{PROJECT_ID}.{DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e` vb
                    LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` hv
                        ON vb.management = hv.bot_management
                    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                    UNION ALL
                    SELECT 
                        SAFE_CAST(document AS INT64) AS cod_luna,
                        DATE(date) AS fecha_gestion,
                        campaign_name,
                        CASE WHEN hh.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END as es_contacto_efectivo,
                        hh.contactabilidad
                    FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e` mh
                    LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` hh
                        ON mh.n1 = hh.n_1 AND mh.n2 = hh.n_2 AND mh.n3 = hh.n_3
                    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                )
            ) gu ON ab.cod_luna = gu.cod_luna AND ab.campaign_name = gu.campaign_name AND gu.fecha_gestion <= fe.fecha_foto
            LEFT JOIN (
                SELECT nro_documento, fecha_pago, SUM(monto_cancelado) as monto_cancelado
                FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
                WHERE fecha_pago >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                GROUP BY 1,2
            ) pc ON dd.nro_documento = pc.nro_documento AND pc.fecha_pago <= fe.fecha_foto
            
            WHERE {{incremental_filter}}
            GROUP BY 1,2,3,4
        )
        
        SELECT 
            fecha_foto,
            campaign_name,
            cartera,
            servicio,
            SAFE_DIVIDE(cuentas_gestionadas, cuentas) * 100 as pct_cober,
            SAFE_DIVIDE(cuentas_cd + cuentas_ci, cuentas_gestionadas) * 100 as pct_contac,
            SAFE_DIVIDE(recupero, deuda_asig) * 100 as pct_efectividad,
            SAFE_DIVIDE(cuentas_cd + cuentas_ci, cuentas) * 100 as pct_cierre,
            recupero,
            cuentas,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM dashboard_simplified
        WHERE cuentas > 0
        ORDER BY fecha_foto DESC, campaign_name
        """,
        
        "assignment_data": f"""
        -- 📋 ASSIGNMENT DATA - Monthly aggregation from raw sources
        SELECT 
            FORMAT_DATE('%Y-%m', DATE(a.creado_el)) as periodo,
            REGEXP_EXTRACT(a.archivo, r'([^/]+)\.txt$') as campaign_name,
            CASE
                WHEN CONTAINS_SUBSTR(UPPER(COALESCE(a.archivo, '')), 'TEMPRANA') THEN 'TEMPRANA'
                WHEN CONTAINS_SUBSTR(UPPER(COALESCE(a.archivo, '')), 'CF_ANN') THEN 'CUOTA_FRACCIONAMIENTO'
                ELSE 'OTRAS'
            END AS cartera,
            IF(a.negocio="MOVIL", a.negocio, "FIJA") as servicio,
            COUNT(DISTINCT a.cuenta) as cuentas,
            COUNT(DISTINCT a.cod_luna) as clientes,
            AVG(COALESCE(d.monto_exigible, 0)) as ticket_promedio,
            SUM(COALESCE(d.monto_exigible, 0)) as deuda_asig,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` a
        LEFT JOIN `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` d
            ON CAST(a.cuenta AS STRING) = d.cod_cuenta
            AND DATE(a.creado_el) = DATE(d.creado_el)
        WHERE {{incremental_filter}}
        GROUP BY 1,2,3,4
        ORDER BY periodo DESC, campaign_name
        """,
        
        "operation_data": f"""
        -- ⚡ OPERATION DATA - Hourly breakdown from raw management data
        SELECT 
            DATE(date) as fecha_foto,
            EXTRACT(HOUR FROM date) as hora,
            'BOT' as canal,
            campaign_name,
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN hv.contactabilidad_homologada = 'Contacto Efectivo' THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN hv.es_pdp_homologado = 1 THEN 1 ELSE 0 END) as total_pdp,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e` vb
        LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` hv
            ON vb.management = hv.bot_management
        WHERE {{incremental_filter}}
          AND campaign_name IS NOT NULL
        GROUP BY 1,2,3,4
        
        UNION ALL
        
        SELECT 
            DATE(date) as fecha_foto,
            EXTRACT(HOUR FROM date) as hora,
            'HUMANO' as canal,
            campaign_name,
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN hh.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN hh.pdp IN ('1', 'SI') THEN 1 ELSE 0 END) as total_pdp,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e` mh
        LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` hh
            ON mh.n1 = hh.n_1 AND mh.n2 = hh.n_2 AND mh.n3 = hh.n_3
        WHERE {{incremental_filter}}
          AND campaign_name IS NOT NULL
        GROUP BY 1,2,3,4
        ORDER BY fecha_foto DESC, hora, canal
        """,
        
        "productivity_data": f"""
        -- 👥 PRODUCTIVITY DATA - Agent performance from raw data
        SELECT 
            DATE(date) as fecha_foto,
            EXTRACT(HOUR FROM date) as hora,
            correo_agente,
            COALESCE(hu.nombre_completo, 'AGENTE_DESCONOCIDO') as nombre_agente,
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN hh.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN hh.pdp IN ('1', 'SI') THEN 1 ELSE 0 END) as total_pdp,
            SAFE_DIVIDE(
                SUM(CASE WHEN hh.contactabilidad = 'Contacto Efectivo' THEN 1 ELSE 0 END),
                COUNT(*)
            ) * 100 as tasa_contacto,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e` mh
        LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` hh
            ON mh.n1 = hh.n_1 AND mh.n2 = hh.n_2 AND mh.n3 = hh.n_3
        LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_usuarios` hu
            ON mh.correo_agente = hu.usuario
        WHERE {{incremental_filter}}
          AND correo_agente IS NOT NULL
        GROUP BY 1,2,3,4
        HAVING total_gestiones > 0
        ORDER BY fecha_foto DESC, hora, correo_agente
        """
    }
    
    @classmethod
    def get_config(cls, table_name: str) -> ExtractionConfig:
        """Get configuration for a specific table"""
        if table_name not in cls.EXTRACTION_CONFIGS:
            raise ValueError(f"No configuration found for table: {table_name}")
        return cls.EXTRACTION_CONFIGS[table_name]
    
    @classmethod
    def get_query(cls, table_name: str) -> str:
        """Get extraction query for a specific table"""
        if table_name not in cls.EXTRACTION_QUERIES:
            raise ValueError(f"No query found for table: {table_name}")
        return cls.EXTRACTION_QUERIES[table_name]
    
    @classmethod
    def get_incremental_filter(cls, table_name: str, since_date: datetime) -> str:
        """
        Generate incremental filter for a specific table
        
        Args:
            table_name: Name of the table
            since_date: Extract data since this date
            
        Returns:
            SQL WHERE clause for incremental extraction
        """
        config = cls.get_config(table_name)
        
        # Apply lookback window for data quality
        lookback_date = since_date - timedelta(days=config.lookback_days)
        
        # Different filter logic based on table type
        if config.table_type == TableType.ASSIGNMENT:
            # Assignment uses creation date
            return f"DATE(a.creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif config.table_type == TableType.OPERATION:
            # Operation uses management date
            return f"DATE(date) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif config.table_type == TableType.PRODUCTIVITY:
            # Productivity uses management date
            return f"DATE(date) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        else:
            # Dashboard and evolution use foto date
            return f"fe.fecha_foto >= '{lookback_date.strftime('%Y-%m-%d')}'"
    
    @classmethod
    def list_tables(cls) -> List[str]:
        """List all configured tables"""
        return list(cls.EXTRACTION_CONFIGS.keys())
    
    @classmethod
    def get_dashboard_tables(cls) -> List[str]:
        """Get tables that feed the main dashboard"""
        return [
            name for name, config in cls.EXTRACTION_CONFIGS.items()
            if config.table_type in [TableType.DASHBOARD, TableType.EVOLUTION]
        ]


# 🎯 CONVENIENCE CONSTANTS FOR EASY IMPORTS
DASHBOARD_TABLES = ETLConfig.get_dashboard_tables()
ALL_TABLES = ETLConfig.list_tables()

# Default extraction configuration
DEFAULT_CONFIG = ExtractionConfig(
    table_name="default",
    table_type=TableType.DASHBOARD,
    description="Default configuration",
    primary_key=["id"],
    incremental_column="updated_at",
    lookback_days=1
)
