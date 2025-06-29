"""
🎯 ETL Configuration System - WORKING PLACEHOLDER VERSION
Fixed placeholder substitution and SQL syntax errors

ISSUE FIXED: {incremental_filter} placeholder substitution working correctly
TESTED: Queries will be properly formatted before BigQuery execution
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
    source_table: str = None
    
    # Quality checks
    required_columns: List[str] = None
    min_expected_records: int = 0
    
    # Scheduling
    refresh_frequency_hours: int = 6  # How often to refresh
    max_execution_time_minutes: int = 30  # Timeout


class ETLConfig:
    """
    Centralized ETL configuration for Pulso Dashboard - FIXED PLACEHOLDER VERSION
    
    STRATEGY: Working queries with proper placeholder substitution
    FIXED: Placeholder syntax and SQL generation
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    
    # 🔄 RAW SOURCE CONFIGURATIONS
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📅 CALENDARIO
        "raw_calendario": ExtractionConfig(
            table_name="raw_calendario",
            table_type=TableType.DASHBOARD,
            description="Campaign calendar",
            primary_key=["ARCHIVO"],
            incremental_column="fecha_apertura",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5",
            lookback_days=7,
            required_columns=["ARCHIVO", "fecha_apertura"],
            min_expected_records=1
        ),
        
        # 👥 ASIGNACIONES
        "raw_asignaciones": ExtractionConfig(
            table_name="raw_asignaciones",
            table_type=TableType.ASSIGNMENT,
            description="Client assignments",
            primary_key=["cod_luna", "cuenta", "archivo"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_asignacion",
            lookback_days=30,
            batch_size=50000,
            required_columns=["cod_luna", "cuenta", "archivo"],
            min_expected_records=1
        ),
        
        # 💰 TRANDEUDA
        "raw_trandeuda": ExtractionConfig(
            table_name="raw_trandeuda", 
            table_type=TableType.DASHBOARD,
            description="Daily debt snapshots",
            primary_key=["cod_cuenta", "nro_documento", "archivo"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda",
            lookback_days=14,
            batch_size=100000,
            required_columns=["cod_cuenta", "monto_exigible"],
            min_expected_records=1
        ),
        
        # 💳 PAGOS
        "raw_pagos": ExtractionConfig(
            table_name="raw_pagos",
            table_type=TableType.DASHBOARD,
            description="Payment transactions", 
            primary_key=["nro_documento", "fecha_pago", "monto_cancelado"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_pagos",
            lookback_days=30,
            batch_size=25000,
            required_columns=["nro_documento", "fecha_pago", "monto_cancelado"],
            min_expected_records=1
        ),
        
        # 🎯 GESTIONES UNIFICADAS
        "gestiones_unificadas": ExtractionConfig(
            table_name="gestiones_unificadas",
            table_type=TableType.OPERATION,
            description="Unified gestiones view",
            primary_key=["cod_luna", "timestamp_gestion"],
            incremental_column="timestamp_gestion",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_vw_gestiones_unificadas", 
            lookback_days=3,
            batch_size=75000,
            refresh_frequency_hours=1,
            required_columns=["cod_luna", "fecha_gestion"],
            min_expected_records=1
        )
    }
    
    # 🎯 QUERY TEMPLATES - Will be formatted with actual filters
    EXTRACTION_QUERY_TEMPLATES: Dict[str, str] = {
        
        # 📅 CALENDARIO
        "raw_calendario": """
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
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
        WHERE {incremental_filter}
        """,
        
        # 👥 ASIGNACIONES
        "raw_asignaciones": """
        SELECT 
            CAST(cliente AS STRING) as cliente,
            CAST(cuenta AS STRING) as cuenta,
            CAST(cod_luna AS STRING) as cod_luna,
            CAST(telefono AS STRING) as telefono,
            tramo_gestion,
            min_vto,
            negocio,
            dias_sin_trafico,
            decil_contacto,
            decil_pago,
            zona,
            rango_renta,
            campania_act,
            archivo,
            creado_el,
            DATE(creado_el) as fecha_asignacion,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE {incremental_filter}
        """,
        
        # 💰 TRANDEUDA
        "raw_trandeuda": """
        SELECT 
            cod_cuenta,
            nro_documento,
            fecha_vencimiento,
            monto_exigible,
            archivo,
            creado_el,
            DATE(creado_el) as fecha_proceso,
            motivo_rechazo,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE {incremental_filter}
          AND monto_exigible > 0
          AND motivo_rechazo IS NULL
        """,
        
        # 💳 PAGOS
        "raw_pagos": """
        SELECT 
            cod_sistema,
            nro_documento,
            monto_cancelado,
            fecha_pago,
            archivo,
            creado_el,
            motivo_rechazo,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE {incremental_filter}
          AND monto_cancelado > 0
          AND motivo_rechazo IS NULL
        """,
        
        # 🎯 GESTIONES UNIFICADAS
        "gestiones_unificadas": """
        SELECT 
            CAST(cod_luna AS STRING) as cod_luna,
            fecha_gestion,
            timestamp_gestion,
            canal_origen,
            management_original,
            sub_management_original,
            compromiso_original,
            nivel_1,
            nivel_2,
            contactabilidad,
            es_contacto_efectivo,
            es_contacto_no_efectivo,
            es_compromiso,
            peso_gestion,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_vw_gestiones_unificadas`
        WHERE {incremental_filter}
        """
    }
    
    # 🚨 GLOBAL SETTINGS
    DEFAULT_TIMEZONE = "America/Lima"
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 30
    
    @classmethod
    def get_config(cls, table_name: str) -> ExtractionConfig:
        """Get configuration for a specific table"""
        if table_name not in cls.EXTRACTION_CONFIGS:
            raise ValueError(f"No configuration found for table: {table_name}")
        return cls.EXTRACTION_CONFIGS[table_name]
    
    @classmethod
    def get_query_template(cls, table_name: str) -> str:
        """Get query template for a specific table"""
        if table_name not in cls.EXTRACTION_QUERY_TEMPLATES:
            raise ValueError(f"No query template found for table: {table_name}")
        return cls.EXTRACTION_QUERY_TEMPLATES[table_name]
    
    @classmethod
    def get_query(cls, table_name: str, since_date: datetime = None) -> str:
        """
        Get formatted extraction query for a specific table
        
        Args:
            table_name: Name of the table
            since_date: Extract data since this date (None for full refresh)
            
        Returns:
            Formatted SQL query ready for BigQuery execution
        """
        template = cls.get_query_template(table_name)
        
        if since_date is None:
            # Full refresh - no filter
            incremental_filter = "1=1"  # Always true
        else:
            # Incremental - use date filter
            incremental_filter = cls.get_incremental_filter(table_name, since_date)
        
        # Format the template with the actual filter
        formatted_query = template.format(incremental_filter=incremental_filter)
        
        return formatted_query.strip()
    
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
        
        # Generate filters based on table type
        if table_name == "raw_calendario":
            # Use business date: fecha_apertura
            return f"fecha_apertura >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name in ["raw_asignaciones", "raw_trandeuda", "raw_pagos"]:
            # Use technical date: creado_el (we'll add filename date logic later)
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name == "gestiones_unificadas":
            # Use business timestamp: timestamp_gestion
            return f"DATE(timestamp_gestion) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        else:
            # Default fallback
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
    
    @classmethod
    def list_tables(cls) -> List[str]:
        """List all configured tables"""
        return list(cls.EXTRACTION_CONFIGS.keys())
    
    @classmethod
    def get_dashboard_tables(cls) -> List[str]:
        """Get core tables needed for dashboard calculation"""
        return [
            "raw_calendario",           # Campaign definitions
            "raw_asignaciones",         # Client assignments
            "raw_trandeuda",           # Debt snapshots
            "raw_pagos",               # Payments
            "gestiones_unificadas"     # All gestiones with homologation
        ]
    
    @classmethod
    def get_raw_source_tables(cls) -> List[str]:
        """Get all raw source tables for initial extraction"""
        return [name for name in cls.EXTRACTION_CONFIGS.keys() if name.startswith("raw_")]


# 🎯 CONVENIENCE CONSTANTS FOR EASY IMPORTS
DASHBOARD_TABLES = ETLConfig.get_dashboard_tables()
RAW_SOURCE_TABLES = ETLConfig.get_raw_source_tables()
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
