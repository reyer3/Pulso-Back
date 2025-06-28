"""
🎯 ETL Configuration System
Production-ready configuration for incremental extractions

Defines extraction strategies, primary keys, and incremental logic 
for each dashboard data mart.
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
    
    Following KISS and DRY principles:
    - Single source of truth for all extraction configs
    - Reusable patterns across similar tables
    - Production-ready defaults
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
            lookback_days=7,  # Re-process last week for data completeness
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
            lookback_days=3,  # Shorter window for time series
            batch_size=50000,  # Larger batches for aggregated data
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
            source_view=f"{VIEW_PREFIX}_calendario_maestro",  # Source from calendar
            lookback_days=30,  # Month-level data needs longer window
            refresh_frequency_hours=24,  # Daily refresh sufficient
            required_columns=["periodo", "archivo", "cartera", "cuentas", "deuda_asig"],
            min_expected_records=20
        ),
        
        # ⚡ OPERATION HOURLY DATA
        "operation_data": ExtractionConfig(
            table_name="operation_data",
            table_type=TableType.OPERATION,
            description="Hourly operational metrics by channel",
            primary_key=["fecha_foto", "hora", "canal", "archivo"],
            incremental_column="fecha_foto",
            source_view=f"{VIEW_PREFIX}_gestiones_unificadas",  # Source from gestiones
            lookback_days=2,  # Short window for operational data
            batch_size=5000,  # Smaller batches for granular data
            refresh_frequency_hours=2,  # More frequent refresh
            max_execution_time_minutes=15,  # Shorter timeout
            required_columns=["fecha_foto", "hora", "canal", "total_gestiones"],
            min_expected_records=10
        ),
        
        # 👥 PRODUCTIVITY DATA
        "productivity_data": ExtractionConfig(
            table_name="productivity_data",
            table_type=TableType.PRODUCTIVITY, 
            description="Agent productivity metrics",
            primary_key=["fecha_foto", "correo_agente", "archivo"],
            incremental_column="fecha_foto",
            source_view=f"{VIEW_PREFIX}_gestiones_unificadas",
            lookback_days=5,  # Week window for agent data
            refresh_frequency_hours=8,  # 3x daily refresh
            required_columns=["fecha_foto", "correo_agente", "total_gestiones", "contactos_efectivos"],
            min_expected_records=1  # Could have days with few agents
        )
    }
    
    # 🚨 GLOBAL SETTINGS
    DEFAULT_TIMEZONE = "America/Lima"
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 30
    
    # 🎯 EXTRACTION QUERIES - Mapped to configs above
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
            tipo_cartera as cartera,
            COUNT(DISTINCT cuenta) as cuentas,
            SUM(deuda_inicial) as deuda_asig,
            SUM(deuda_actual) as deuda_actual,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_asignaciones_clean`
        WHERE {{incremental_filter}}
        GROUP BY 1,2,3
        ORDER BY periodo DESC, archivo
        """,
        
        "operation_data": f"""
        SELECT 
            fecha_gestion as fecha_foto,
            EXTRACT(HOUR FROM timestamp_gestion) as hora,
            canal_origen as canal,
            'GENERAL' as archivo,  -- Aggregated across campaigns
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END) as total_pdp,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_gestiones_unificadas`
        WHERE {{incremental_filter}}
        GROUP BY 1,2,3,4
        ORDER BY fecha_foto DESC, hora, canal
        """,
        
        "productivity_data": f"""
        SELECT 
            g.fecha_gestion as fecha_foto,
            COALESCE(h.usuario, 'SISTEMA') as correo_agente,
            'GENERAL' as archivo,
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN g.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN g.es_compromiso THEN 1 ELSE 0 END) as total_pdp,
            SUM(g.peso_gestion) as peso_total,
            CURRENT_TIMESTAMP() as fecha_procesamiento
        FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_gestiones_unificadas` g
        LEFT JOIN `{PROJECT_ID}.{DATASET}.homologacion_P3fV4dWNeMkN5RJMhV8e_usuarios` h
            ON g.canal_origen = 'HUMANO'  -- Only join for human agents
        WHERE {{incremental_filter}}
        GROUP BY 1,2,3
        HAVING total_gestiones > 0
        ORDER BY fecha_foto DESC, correo_agente
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
