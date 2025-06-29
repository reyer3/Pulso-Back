"""
🎯 ETL Configuration System - SIMPLE RAW EXTRACTION VERSION
Minimal BigQuery extraction + All transformation logic in Python pipeline

APPROACH: Extract raw data with simple filters, transform everything in Python
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
    Centralized ETL configuration for Pulso Dashboard - SIMPLE EXTRACTION VERSION
    
    STRATEGY: Extract raw data with minimal logic, transform everything in Python
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    
    # 🔄 EXTRACTION CONFIGURATIONS
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📊 MAIN DASHBOARD DATA - Extract from multiple raw sources
        "dashboard_data": ExtractionConfig(
            table_name="dashboard_data",
            table_type=TableType.DASHBOARD,
            description="Dashboard data - raw sources combined in Python",
            primary_key=["fecha_foto", "archivo", "cartera", "servicio"],
            incremental_column="fecha_apertura",  # From calendario
            lookback_days=7,
            required_columns=["ARCHIVO", "fecha_apertura"],
            min_expected_records=1
        ),
        
        # 📈 EVOLUTION TIME SERIES  
        "evolution_data": ExtractionConfig(
            table_name="evolution_data", 
            table_type=TableType.EVOLUTION,
            description="Evolution data from calendario base",
            primary_key=["fecha_foto", "archivo"],
            incremental_column="fecha_apertura",
            lookback_days=3,
            batch_size=50000,
            required_columns=["ARCHIVO", "fecha_apertura"],
            min_expected_records=1
        ),
        
        # 📋 ASSIGNMENT ANALYSIS
        "assignment_data": ExtractionConfig(
            table_name="assignment_data",
            table_type=TableType.ASSIGNMENT,
            description="Assignment analysis from raw batch data",
            primary_key=["periodo", "archivo", "cartera"],
            incremental_column="creado_el",  # From asignaciones
            lookback_days=30,
            refresh_frequency_hours=24,
            required_columns=["archivo", "creado_el"],
            min_expected_records=1
        ),
        
        # ⚡ OPERATION HOURLY DATA
        "operation_data": ExtractionConfig(
            table_name="operation_data",
            table_type=TableType.OPERATION,
            description="Hourly operational metrics from gestiones",
            primary_key=["fecha_foto", "hora", "canal", "campaign_name"],
            incremental_column="date",  # From gestiones tables
            lookback_days=2,
            batch_size=5000,
            refresh_frequency_hours=2,
            max_execution_time_minutes=15,
            required_columns=["date", "campaign_name"],
            min_expected_records=1
        ),
        
        # 👥 PRODUCTIVITY DATA
        "productivity_data": ExtractionConfig(
            table_name="productivity_data",
            table_type=TableType.PRODUCTIVITY, 
            description="Agent productivity from gestiones",
            primary_key=["fecha_foto", "correo_agente", "hora"],
            incremental_column="date",  # From gestiones tables
            lookback_days=5,
            refresh_frequency_hours=8,
            required_columns=["date", "correo_agente"],
            min_expected_records=1
        )
    }
    
    # 🚨 GLOBAL SETTINGS
    DEFAULT_TIMEZONE = "America/Lima"
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 30
    
    # 🎯 SIMPLE EXTRACTION QUERIES - RAW DATA ONLY
    EXTRACTION_QUERIES: Dict[str, str] = {
        
        # DASHBOARD DATA: Multiple raw sources to be joined in Python
        "dashboard_data": f"""
        -- 📊 DASHBOARD RAW SOURCES - To be combined in Python
        
        -- Source 1: Calendario
        WITH calendario_raw AS (
            SELECT 
                ARCHIVO,
                TIPO_CARTERA,
                fecha_apertura,
                fecha_trandeuda,
                fecha_cierre,
                FECHA_CIERRE_PLANIFICADA,
                DURACION_CAMPANA_DIAS_HABILES,
                ANNO_ASIGNACION,
                PERIODO_ASIGNACION,
                ES_CARTERA_ABIERTA,
                RANGO_VENCIMIENTO,
                ESTADO_CARTERA,
                periodo_mes,
                periodo_date,
                tipo_ciclo_campana,
                categoria_duracion,
                'calendario' as source_table
            FROM `{PROJECT_ID}.{DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
            WHERE {{incremental_filter}}
        ),
        
        -- Source 2: Asignaciones  
        asignaciones_raw AS (
            SELECT 
                archivo,
                cod_luna,
                cuenta,
                min_vto,
                negocio,
                telefono,
                tramo_gestion,
                decil_contacto,
                decil_pago,
                creado_el,
                'asignacion' as source_table
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
            WHERE DATE(creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        
        -- Source 3: Deuda
        deuda_raw AS (
            SELECT 
                cod_cuenta,
                nro_documento,
                fecha_vencimiento,
                monto_exigible,
                creado_el,
                'deuda' as source_table
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
            WHERE DATE(creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND monto_exigible > 0
        ),
        
        -- Source 4: Gestiones Bot
        gestiones_bot_raw AS (
            SELECT 
                document,
                date,
                campaign_name,
                management,
                sub_management,
                compromiso,
                'gestiones_bot' as source_table
            FROM `{PROJECT_ID}.{DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e`
            WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        
        -- Source 5: Gestiones Humano
        gestiones_humano_raw AS (
            SELECT 
                document,
                date,
                campaign_name,
                n1,
                n2,
                n3,
                correo_agente,
                'gestiones_humano' as source_table
            FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e`
            WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        
        -- Source 6: Pagos
        pagos_raw AS (
            SELECT 
                nro_documento,
                fecha_pago,
                monto_cancelado,
                creado_el,
                'pagos' as source_table
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
            WHERE DATE(creado_el) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND monto_cancelado > 0
        )
        
        -- UNION ALL raw sources with metadata
        SELECT *, CURRENT_TIMESTAMP() as extraction_timestamp FROM calendario_raw
        UNION ALL
        SELECT 
            archivo as ARCHIVO,
            NULL as TIPO_CARTERA,
            NULL as fecha_apertura,
            NULL as fecha_trandeuda,
            NULL as fecha_cierre,
            NULL as FECHA_CIERRE_PLANIFICADA,
            NULL as DURACION_CAMPANA_DIAS_HABILES,
            NULL as ANNO_ASIGNACION,
            NULL as PERIODO_ASIGNACION,
            NULL as ES_CARTERA_ABIERTA,
            NULL as RANGO_VENCIMIENTO,
            NULL as ESTADO_CARTERA,
            NULL as periodo_mes,
            NULL as periodo_date,
            NULL as tipo_ciclo_campana,
            NULL as categoria_duracion,
            source_table,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM asignaciones_raw
        """,
        
        # SIMPLIFIED QUERIES FOR OTHER TABLES
        "evolution_data": f"""
        -- 📈 EVOLUTION RAW DATA - Calendario only
        SELECT 
            ARCHIVO,
            fecha_apertura,
            fecha_cierre,
            TIPO_CARTERA,
            periodo_mes,
            periodo_date,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
        WHERE {{incremental_filter}}
        """,
        
        "assignment_data": f"""
        -- 📋 ASSIGNMENT RAW DATA - Asignaciones only
        SELECT 
            archivo,
            cod_luna,
            cuenta,
            negocio,
            tramo_gestion,
            creado_el,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE {{incremental_filter}}
        """,
        
        "operation_data": f"""
        -- ⚡ OPERATION RAW DATA - Gestiones only
        SELECT 
            document,
            date,
            campaign_name,
            management,
            sub_management,
            'BOT' as canal,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE {{incremental_filter}}
        
        UNION ALL
        
        SELECT 
            document,
            date,
            campaign_name,
            n1 as management,
            n2 as sub_management,
            'HUMANO' as canal,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE {{incremental_filter}}
        """,
        
        "productivity_data": f"""
        -- 👥 PRODUCTIVITY RAW DATA - Gestiones humano only
        SELECT 
            document,
            date,
            campaign_name,
            correo_agente,
            n1,
            n2,
            n3,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE {{incremental_filter}}
          AND correo_agente IS NOT NULL
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
        
        # Simple date filters based on table
        if table_name == "dashboard_data" or table_name == "evolution_data":
            # Use calendario fecha_apertura
            return f"fecha_apertura >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name == "assignment_data":
            # Use asignaciones creado_el
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name in ["operation_data", "productivity_data"]:
            # Use gestiones date
            return f"DATE(date) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        else:
            # Default fallback
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
    
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
