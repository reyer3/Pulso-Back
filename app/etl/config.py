"""
🎯 ETL Configuration System - SIMPLIFIED RAW SOURCES
Start with simple table exploration before complex queries

First phase: Verify table structures and basic connectivity
Second phase: Build complex business logic gradually
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
    ETL configuration for Pulso Dashboard - SIMPLIFIED VERSION
    
    Start with basic table exploration to understand real schema
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    
    # 🔄 EXTRACTION CONFIGURATIONS
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📊 MAIN DASHBOARD DATA - SIMPLIFIED
        "dashboard_data": ExtractionConfig(
            table_name="dashboard_data",
            table_type=TableType.DASHBOARD,
            description="Simple dashboard data exploration",
            primary_key=["fecha_procesamiento", "campaign_name"],
            incremental_column="fecha_procesamiento",
            lookback_days=7,
            required_columns=["fecha_procesamiento", "campaign_name"],
            min_expected_records=1
        ),
        
        # 📈 EVOLUTION TIME SERIES - SIMPLIFIED
        "evolution_data": ExtractionConfig(
            table_name="evolution_data", 
            table_type=TableType.EVOLUTION,
            description="Simple evolution data",
            primary_key=["fecha_procesamiento", "campaign_name"],
            incremental_column="fecha_procesamiento",
            lookback_days=3,
            batch_size=50000,
            required_columns=["fecha_procesamiento", "campaign_name"],
            min_expected_records=1
        )
    }
    
    # 🚨 GLOBAL SETTINGS
    DEFAULT_TIMEZONE = "America/Lima"
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 30
    
    # 🎯 SIMPLIFIED EXPLORATION QUERIES
    EXTRACTION_QUERIES: Dict[str, str] = {
        
        "dashboard_data": f"""
        -- 📊 SIMPLE EXPLORATION QUERY - Test basic connectivity
        WITH test_asignacion AS (
            SELECT 
                archivo,
                cod_luna,
                cuenta,
                negocio,
                creado_el,
                DATE(creado_el) as fecha_procesamiento
            FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` 
            WHERE {{incremental_filter}}
            LIMIT 100
        ),
        
        campaign_extract AS (
            SELECT 
                archivo,
                REGEXP_EXTRACT(archivo, r'([^/]+)\\.txt$') as campaign_name,
                fecha_procesamiento,
                COUNT(*) as total_cuentas,
                COUNT(DISTINCT cod_luna) as total_clientes,
                COUNT(DISTINCT negocio) as tipos_negocio
            FROM test_asignacion
            GROUP BY 1,2,3
        )
        
        SELECT 
            fecha_procesamiento,
            COALESCE(campaign_name, 'UNKNOWN_CAMPAIGN') as campaign_name,
            'TODAS' as cartera,
            'TODOS' as servicio,
            total_cuentas as cuentas,
            total_clientes as clientes,
            0.0 as deuda_asig,
            0 as cuentas_gestionadas,
            0 as cuentas_cd,
            0 as cuentas_ci,
            0 as cuentas_pdp,
            0.0 as recupero,
            0 as cuentas_pagadoras,
            0 as total_gestiones,
            
            -- Simple KPIs
            0.0 as pct_cober,
            0.0 as pct_contac,
            0.0 as pct_cd,
            0.0 as pct_ci,
            0.0 as pct_conversion,
            0.0 as inten,
            0.0 as pct_efectividad,
            0.0 as pct_cierre,
            
            CURRENT_TIMESTAMP() as fecha_procesamiento_final
            
        FROM campaign_extract
        WHERE campaign_name IS NOT NULL
        ORDER BY fecha_procesamiento DESC
        """,
        
        "evolution_data": f"""
        -- 📈 SIMPLE EVOLUTION QUERY
        SELECT 
            DATE(creado_el) as fecha_procesamiento,
            COALESCE(REGEXP_EXTRACT(archivo, r'([^/]+)\\.txt$'), 'UNKNOWN') as campaign_name,
            'TODAS' as cartera,
            'TODOS' as servicio,
            0.0 as pct_cober,
            0.0 as pct_contac,
            0.0 as pct_efectividad,
            0.0 as pct_cierre,
            0.0 as recupero,
            COUNT(*) as cuentas,
            CURRENT_TIMESTAMP() as fecha_procesamiento_final
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE {{incremental_filter}}
        GROUP BY 1,2,3,4
        ORDER BY fecha_procesamiento DESC
        LIMIT 100
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
        
        # Use creado_el for all tables for now (simplification)
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
