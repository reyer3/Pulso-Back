"""
🎯 ETL Configuration System - UPDATED with Real BigQuery Schema
Production-ready configuration for incremental extractions

Updated based on actual BigQuery table schemas:
- voicebot: campaign_name as queue identifier
- mibotair: nombre_agente + correo_agente available 
- Proper joins for productivity and operation data
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
    Centralized ETL configuration for Pulso Dashboard
    
    UPDATED: Based on real BigQuery schema analysis
    - campaign_name available for queue analysis
    - nombre_agente + correo_agente in mibotair
    - Proper productivity and operation queries
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    VIEW_PREFIX = "bi_P3fV4dWNeMkN5RJMhV8e_vw"
    
    # 🔄 EXTRACTION CONFIGURATIONS
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📊 MAIN DASHBOARD DATA
        "dashboard_data": ExtractionConfig(
            table_name="dashboard_data",
            table_type=TableType.DASHBOARD,
            description="Main dashboard metrics aggregated by date and campaign",
            primary_key=["fecha_foto", "archivo", "cartera", "servicio"],
            incremental_column="fecha_foto",
            source_view=f"{VIEW_PREFIX}_dashboard_cobranzas",
            lookback_days=7,
            required_columns=[
                "fecha_foto", "archivo", "cartera", "servicio", 
                "cuentas", "clientes", "deuda_asig", "recupero"
            ],
            min_expected_records=100
        ),
        
        # 📈 EVOLUTION TIME SERIES
        "evolution_data": ExtractionConfig(
            table_name="evolution_data", 
            table_type=TableType.EVOLUTION,
            description="Time series data for trending charts",
            primary_key=["fecha_foto", "archivo"],
            incremental_column="fecha_foto",
            source_view=f"{VIEW_PREFIX}_dashboard_cobranzas",
            lookback_days=3,
            batch_size=50000,
            required_columns=["fecha_foto", "archivo", "pct_cober", "pct_contac", "pct_efectividad"],
            min_expected_records=50
        ),
        
        # 📋 ASSIGNMENT ANALYSIS  
        "assignment_data": ExtractionConfig(
            table_name="assignment_data",
            table_type=TableType.ASSIGNMENT,
            description="Monthly assignment comparison data",
            primary_key=["periodo", "archivo", "cartera"],
            incremental_column="fecha_procesamiento",
            source_view=f"{VIEW_PREFIX}_asignaciones_clean",
            lookback_days=30,
            refresh_frequency_hours=24,
            required_columns=["periodo", "archivo", "cartera", "cuentas", "clientes", "deuda_asig"],
            min_expected_records=20
        ),
        
        # ⚡ OPERATION HOURLY DATA - UPDATED
        "operation_data": ExtractionConfig(
            table_name="operation_data",
            table_type=TableType.OPERATION,
            description="Hourly operational metrics by channel and queue",
            primary_key=["fecha_foto", "hora", "canal", "cola"],
            incremental_column="fecha_foto",
            source_view=f"{VIEW_PREFIX}_gestiones_unificadas",
            lookback_days=2,
            batch_size=5000,
            refresh_frequency_hours=2,
            max_execution_time_minutes=15,
            required_columns=["fecha_foto", "hora", "canal", "cola", "total_gestiones"],
            min_expected_records=10
        ),
        
        # 👥 PRODUCTIVITY DATA - UPDATED
        "productivity_data": ExtractionConfig(
            table_name="productivity_data",
            table_type=TableType.PRODUCTIVITY, 
            description="Agent productivity metrics with recovery data",
            primary_key=["fecha_foto", "correo_agente", "hora"],
            incremental_column="fecha_foto",
            source_view=f"{VIEW_PREFIX}_gestiones_unificadas",
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
    
    # 🎯 EXTRACTION QUERIES - UPDATED with Real Schema
    EXTRACTION_QUERIES: Dict[str, str] = {
        
        "dashboard_data": f"""
        SELECT 
            fecha_foto,
            archivo,
            cartera,
            servicio,
            cuentas,
            clientes, 
            deuda_asig,
            deuda_act,
            cuentas_gestionadas,
            cuentas_cd,
            cuentas_ci,
            cuentas_sc,
            cuentas_sg,
            cuentas_pdp,
            recupero,
            pct_cober,
            pct_contac,
            pct_cd,
            pct_ci,
            pct_conversion,
            pct_efectividad,
            pct_cierre,
            inten,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_dashboard_cobranzas`
        WHERE {{incremental_filter}}
        ORDER BY fecha_foto DESC, archivo, cartera, servicio
        """,
        
        "evolution_data": f"""
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
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_dashboard_cobranzas`
        WHERE {{incremental_filter}}
        ORDER BY fecha_foto DESC, archivo
        """,
        
        "assignment_data": f"""
        SELECT 
            periodo_mes as periodo,
            archivo,
            cartera,
            COUNT(DISTINCT cuenta) as cuentas,
            COUNT(DISTINCT cod_luna) as clientes,  -- ✅ FIXED: Added clients count
            SUM(deuda_inicial) as deuda_asig,
            SUM(deuda_actual) as deuda_actual,
            AVG(deuda_inicial / NULLIF(1, 0)) as ticket_promedio,  -- Calculate average ticket
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_asignaciones_clean`
        WHERE {{incremental_filter}}
        GROUP BY 1,2,3
        ORDER BY periodo DESC, archivo
        """,
        
        "operation_data": f"""
        -- ✅ UPDATED: With campaign_name as queue and attempt analysis
        WITH gestiones_con_intento AS (
            SELECT 
                DATE(g.timestamp_gestion) as fecha_foto,
                EXTRACT(HOUR FROM g.timestamp_gestion) as hora,
                g.canal_origen as canal,
                g.campaign_name as cola,  -- ✅ USING campaign_name as queue
                g.cod_luna,
                g.es_contacto_efectivo,
                g.es_compromiso,
                ROW_NUMBER() OVER (
                    PARTITION BY g.cod_luna, DATE(g.timestamp_gestion) 
                    ORDER BY g.timestamp_gestion
                ) as numero_intento
            FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_gestiones_unificadas` g
            WHERE {{incremental_filter}}
        )
        
        SELECT 
            fecha_foto,
            hora,
            canal,
            cola,
            numero_intento,
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
            COUNT(*) - SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_no_efectivos,
            SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END) as total_pdp,
            SAFE_DIVIDE(
                SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END),
                SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END)
            ) * 100 as tasa_cierre,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM gestiones_con_intento
        GROUP BY 1,2,3,4,5
        ORDER BY fecha_foto DESC, hora, canal, cola
        """,
        
        "productivity_data": f"""
        -- ✅ UPDATED: With agent names and recovery data
        WITH gestiones_agente AS (
            SELECT 
                DATE(g.timestamp_gestion) as fecha_foto,
                EXTRACT(HOUR FROM g.timestamp_gestion) as hora,
                g.correo_agente,
                -- ✅ USING nombre_agente from mibotair, fallback for bot
                CASE 
                    WHEN g.canal_origen = 'HUMANO' THEN g.nombre_agente
                    WHEN g.canal_origen = 'BOT' THEN 'SISTEMA_BOT'
                    ELSE 'AGENTE_DESCONOCIDO'
                END as nombre_agente,
                g.cod_luna,
                g.es_contacto_efectivo,
                g.es_compromiso,
                g.peso_gestion
            FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_gestiones_unificadas` g
            WHERE {{incremental_filter}}
              AND g.correo_agente IS NOT NULL
        ),
        
        pagos_por_agente AS (
            SELECT 
                p.fecha_pago,
                g.correo_agente,
                SUM(p.monto_cancelado) as monto_recuperado
            FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_pagos_unicos` p
            INNER JOIN gestiones_agente g
                ON p.nro_documento = g.cod_luna  -- Assuming document linkage
                AND p.fecha_pago >= g.fecha_foto - 7  -- Within 7 days of management
            GROUP BY 1,2
        )
        
        SELECT 
            ga.fecha_foto,
            ga.hora,
            ga.correo_agente,
            ga.nombre_agente,
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN ga.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN ga.es_compromiso THEN 1 ELSE 0 END) as total_pdp,
            SUM(ga.peso_gestion) as peso_total,
            COALESCE(pa.monto_recuperado, 0) as monto_recuperado,  -- ✅ RECOVERY DATA
            
            -- Performance calculations
            SAFE_DIVIDE(
                SUM(CASE WHEN ga.es_contacto_efectivo THEN 1 ELSE 0 END),
                COUNT(*)
            ) * 100 as tasa_contacto,
            
            SAFE_DIVIDE(
                SUM(CASE WHEN ga.es_compromiso THEN 1 ELSE 0 END),
                SUM(CASE WHEN ga.es_contacto_efectivo THEN 1 ELSE 0 END)
            ) * 100 as tasa_conversion,
            
            CURRENT_TIMESTAMP() as fecha_procesamiento
            
        FROM gestiones_agente ga
        LEFT JOIN pagos_por_agente pa
            ON ga.fecha_foto = pa.fecha_pago
            AND ga.correo_agente = pa.correo_agente
        GROUP BY 1,2,3,4,9  -- Include monto_recuperado in GROUP BY
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
        incremental_col = config.incremental_column
        
        # Apply lookback window for data quality
        lookback_date = since_date - timedelta(days=config.lookback_days)
        
        return f"{incremental_col} >= '{lookback_date.strftime('%Y-%m-%d')}'"
    
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
