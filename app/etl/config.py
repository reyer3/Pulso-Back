"""
🎯 ETL Configuration System - CASE MISMATCH FIXED
Fixed primary key names to match transformer output (lowercase)

ISSUE FIXED: Config used "ARCHIVO" but transformer outputs "archivo"  
ROOT CAUSE: Case mismatch between config primary_key and transformer output
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional


class ExtractionMode(str, Enum):
    """Extraction modes for different scenarios"""
    INCREMENTAL = "incremental"        # Default: Extract only new/changed data
    FULL_REFRESH = "full_refresh"      # Force: Complete table refresh
    SLIDING_WINDOW = "sliding_window"  # Window: Re-process last N days


class TableType(str, Enum):
    """Table types for different dashboard purposes"""
    DIMENSION = "dimansion"
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
    
    # Primary key configuration - ✅ CASE FIXED FOR TRANSFORMER OUTPUT
    primary_key: List[str]
    incremental_column: Optional[str]
    
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
    Centralized ETL configuration for Pulso Dashboard - CASE MISMATCH FIXED
    
    STRATEGY: Primary key names now match transformer output (lowercase)
    FIXED: All primary keys use lowercase to match transformer dict keys
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    
    # 🔄 RAW SOURCE CONFIGURATIONS - ✅ CASE FIXED TO MATCH TRANSFORMER
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📅 CALENDARIO - ✅ PRIMARY KEY NAMES FIXED TO LOWERCASE
        "raw_calendario": ExtractionConfig(
            table_name="raw_calendario",
            table_type=TableType.DASHBOARD,
            description="Campaign calendar",
            primary_key=["archivo", "periodo_date"],  # ✅ FIXED: lowercase to match transformer
            incremental_column="fecha_apertura",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5",
            lookback_days=7,
            required_columns=["archivo", "fecha_apertura"],  # ✅ FIXED: lowercase
            min_expected_records=1
        ),
        
        # 👥 ASIGNACIONES - ✅ PRIMARY KEY NAMES FIXED TO LOWERCASE
        "raw_asignaciones": ExtractionConfig(
            table_name="raw_asignaciones",
            table_type=TableType.ASSIGNMENT,
            description="Client assignments",
            primary_key=["cod_luna", "cuenta", "archivo", "fecha_asignacion"],  # ✅ FIXED: all lowercase
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_asignacion",
            lookback_days=30,
            batch_size=50000,
            required_columns=["cod_luna", "cuenta", "archivo"],  # ✅ FIXED: lowercase
            min_expected_records=1
        ),
        
        # 💰 TRANDEUDA - ✅ PRIMARY KEY NAMES FIXED TO LOWERCASE
        "raw_trandeuda": ExtractionConfig(
            table_name="raw_trandeuda", 
            table_type=TableType.DASHBOARD,
            description="Daily debt snapshots",
            primary_key=["cod_cuenta", "nro_documento", "archivo", "fecha_proceso"],  # ✅ FIXED: lowercase
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda",
            lookback_days=14,
            batch_size=100000,
            required_columns=["cod_cuenta", "monto_exigible"],  # ✅ FIXED: lowercase
            min_expected_records=1
        ),
        
        # 💳 PAGOS - ✅ PRIMARY KEY NAMES ALREADY LOWERCASE (correct)
        "raw_pagos": ExtractionConfig(
            table_name="raw_pagos",
            table_type=TableType.DASHBOARD,
            description="Payment transactions", 
            primary_key=["nro_documento", "fecha_pago", "monto_cancelado"],  # ✅ Already correct
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_pagos",
            lookback_days=30,
            batch_size=25000,
            required_columns=["nro_documento", "fecha_pago", "monto_cancelado"],  # ✅ lowercase
            min_expected_records=1
        ),
        
        # 🎯 GESTIONES UNIFICADAS - ✅ PRIMARY KEY NAMES ALREADY LOWERCASE (correct)
        "gestiones_unificadas": ExtractionConfig(
            table_name="gestiones_unificadas",
            table_type=TableType.OPERATION,
            description="Unified gestiones view",
            primary_key=["cod_luna", "timestamp_gestion"],  # ✅ Already correct
            incremental_column="timestamp_gestion",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_vw_gestiones_unificadas", 
            lookback_days=3,
            batch_size=75000,
            refresh_frequency_hours=1,
            required_columns=["cod_luna", "fecha_gestion"],  # ✅ lowercase
            min_expected_records=1
        ),

        # 👨‍💼 HOMOLOGACION AGENTES (MIBOTAIR)
        "raw_homologacion_mibotair": ExtractionConfig(
            table_name="raw_homologacion_mibotair",
            table_type=TableType.DIMENSION,
            description="Homologation rules for human agent interactions (MibotAir)",
            primary_key=["n_1", "n_2", "n_3"],
            incremental_column=None,  # This is a dimension table, always full load
            source_table="homologacion_P3fV4dWNeMkN5RJMhV8e_v2",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24,  # Refreshed daily
        ),

        # 🤖 HOMOLOGACION VOICEBOT
        "raw_homologacion_voicebot": ExtractionConfig(
            table_name="raw_homologacion_voicebot",
            table_type=TableType.DIMENSION,
            description="Homologation rules for Voicebot interactions",
            primary_key=["bot_management", "bot_sub_management", "bot_compromiso"],
            incremental_column=None,
            source_table="homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24,
        ),

        # 🤵 EJECUTIVOS (AGENTES)
        "raw_ejecutivos": ExtractionConfig(
            table_name="raw_ejecutivos",
            table_type=TableType.DIMENSION,
            description="Agent and executive information, mapping email to document ID",
            primary_key=["correo_name"],
            incremental_column=None,
            source_table="sync_mibotair_batch_SYS_user",  # Filtered by client_id in query
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24,
        )
    }
    
    # 🎯 QUERY TEMPLATES - Using REAL BigQuery field names
    EXTRACTION_QUERY_TEMPLATES: Dict[str, str] = {
        
        # 📅 CALENDARIO - ✅ REAL SCHEMA
        "raw_calendario": """
        SELECT 
            ARCHIVO,                           -- ✅ Real field name
            TIPO_CARTERA,                      -- ✅ Real field name
            fecha_apertura,                    -- ✅ Real field name
            fecha_trandeuda,                   -- ✅ Real field name
            fecha_cierre,                      -- ✅ Real field name
            FECHA_CIERRE_PLANIFICADA,          -- ✅ Real field name
            DURACION_CAMPANA_DIAS_HABILES,     -- ✅ Real field name
            ANNO_ASIGNACION,                   -- ✅ Real field name (NOT FECHA_ASIGNACION)
            PERIODO_ASIGNACION,                -- ✅ Real field name
            ES_CARTERA_ABIERTA,                -- ✅ Real field name
            RANGO_VENCIMIENTO,                 -- ✅ Real field name
            ESTADO_CARTERA,                    -- ✅ Real field name
            periodo_mes,                       -- ✅ Real field name
            periodo_date,                      -- ✅ Real field name (partition column)
            tipo_ciclo_campana,                -- ✅ Real field name
            categoria_duracion,                -- ✅ Real field name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
        WHERE {incremental_filter}
        """,
        
        # 👥 ASIGNACIONES - ✅ REAL SCHEMA
        "raw_asignaciones": """
        SELECT 
            CAST(cliente AS STRING) as cliente,        -- ✅ Real field name (INT64 → STRING)
            CAST(cuenta AS STRING) as cuenta,          -- ✅ Real field name (INT64 → STRING)
            CAST(cod_luna AS STRING) as cod_luna,      -- ✅ Real field name (INT64 → STRING)
            CAST(telefono AS STRING) as telefono,      -- ✅ Real field name (INT64 → STRING)
            tramo_gestion,                             -- ✅ Real field name
            min_vto,                                   -- ✅ Real field name
            negocio,                                   -- ✅ Real field name
            dias_sin_trafico,                          -- ✅ Real field name
            decil_contacto,                            -- ✅ Real field name
            decil_pago,                                -- ✅ Real field name
            zona,                                      -- ✅ Real field name
            rango_renta,                               -- ✅ Real field name
            campania_act,                              -- ✅ Real field name
            archivo,                                   -- ✅ Real field name
            creado_el,                                 -- ✅ Real field name
            DATE(creado_el) as fecha_asignacion,       -- ✅ Derived from creado_el (partition column)
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE {incremental_filter}
        """,
        
        # 💰 TRANDEUDA - ✅ REAL SCHEMA
        "raw_trandeuda": """
        SELECT 
            cod_cuenta,                                -- ✅ Real field name (STRING, not INT64)
            nro_documento,                             -- ✅ Real field name
            fecha_vencimiento,                         -- ✅ Real field name
            monto_exigible,                            -- ✅ Real field name (FLOAT64)
            archivo,                                   -- ✅ Real field name
            creado_el,                                 -- ✅ Real field name
            DATE(creado_el) as fecha_proceso,          -- ✅ Derived from creado_el (partition column)
            motivo_rechazo,                            -- ✅ Real field name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE {incremental_filter}
          AND monto_exigible > 0
          AND (motivo_rechazo IS NULL OR motivo_rechazo = '')
        """,
        
        # 💳 PAGOS - ✅ REAL SCHEMA
        "raw_pagos": """
        SELECT 
            cod_sistema,                               -- ✅ Real field name (STRING)
            nro_documento,                             -- ✅ Real field name
            monto_cancelado,                           -- ✅ Real field name (FLOAT64)
            fecha_pago,                                -- ✅ Real field name (partition column)
            archivo,                                   -- ✅ Real field name
            creado_el,                                 -- ✅ Real field name
            motivo_rechazo,                            -- ✅ Real field name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE {incremental_filter}
          AND monto_cancelado > 0
          AND (motivo_rechazo IS NULL OR motivo_rechazo = '')
        """,
        
        # 🎯 GESTIONES UNIFICADAS - ✅ REAL SCHEMA FROM VIEW
        "gestiones_unificadas": """
        SELECT 
            CAST(cod_luna AS STRING) as cod_luna,      -- ✅ Real field name (INT64 → STRING)
            fecha_gestion,                             -- ✅ Real field name (DATE)
            timestamp_gestion,                         -- ✅ Real field name (TIMESTAMP, partition column)
            canal_origen,                              -- ✅ Real field name ('BOT'|'HUMANO')
            management_original,                       -- ✅ Real field name
            sub_management_original,                   -- ✅ Real field name
            compromiso_original,                       -- ✅ Real field name
            nivel_1,                                   -- ✅ Real field name (homologated)
            nivel_2,                                   -- ✅ Real field name (homologated)
            contactabilidad,                           -- ✅ Real field name (homologated)
            es_contacto_efectivo,                      -- ✅ Real field name (BOOLEAN)
            es_contacto_no_efectivo,                   -- ✅ Real field name (BOOLEAN)
            es_compromiso,                             -- ✅ Real field name (BOOLEAN)
            peso_gestion,                              -- ✅ Real field name (INT64)
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_vw_gestiones_unificadas`
        WHERE {incremental_filter}
        """,
        # 👨‍💼 HOMOLOGACION AGENTES (MIBOTAIR) - ✅ Explicit Columns
        "raw_homologacion_mibotair": """
         SELECT management,
                n_1,
                n_2,
                n_3,
                peso,
                contactabilidad,
                tipo_gestion,
                codigo_rpta,
                pdp,
                gestor,
                CURRENT_TIMESTAMP() as extraction_timestamp
         FROM ` mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2 `
         WHERE {incremental_filter} -- This will be 1=1 for full refresh
         """,

        # 🤖 HOMOLOGACION VOICEBOT - ✅ Explicit Columns
        "raw_homologacion_voicebot": """
         SELECT bot_management,
                bot_sub_management,
                bot_compromiso,
                n1_homologado,
                n2_homologado,
                n3_homologado,
                contactabilidad_homologada,
                es_pdp_homologado,
                peso_homologado,
                CURRENT_TIMESTAMP() as extraction_timestamp
         FROM ` mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot `
         WHERE {incremental_filter} -- This will be 1=1 for full refresh
         """,

        # 🤵 EJECUTIVOS (AGENTES) - ✅ Explicit Columns & Business Filter
        "raw_ejecutivos": """
          SELECT DISTINCT correo_name,
                          TRIM(nombre)        as nombre,
                          document,
                          CURRENT_TIMESTAMP() as extraction_timestamp
          FROM ` mibot-222814.BI_USA.sync_mibotair_batch_SYS_user `
          WHERE id_cliente = 145
            AND {incremental_filter} -- This will be 1=1 for full refresh
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

        if not config.incremental_column:
            return "1=1"  # Devuelve un filtro que no hace nada
        
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
