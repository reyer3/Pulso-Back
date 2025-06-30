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
    RAW = "raw"                    # Raw data layer
    AUX = "aux"                    # Auxiliary data layer
    MART = "mart"                  # Data mart layer
    DIMENSION = "dimension"        # Dimension table (often in raw or public)
    DASHBOARD = "dashboard"        # Main dashboard aggregation (likely a type of mart)
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
    PROJECT_UID = "P3fV4dWNeMkN5RJMhV8e"  # Added Project UID
    DATASET = "BI_USA"
    
    # 🔄 RAW SOURCE CONFIGURATIONS - Table names are now base names
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        "calendario": ExtractionConfig(
            table_name="calendario",  # Base name
            table_type=TableType.RAW, # Type updated
            description="Campaign calendar",
            primary_key=["archivo", "periodo_date"],
            incremental_column="fecha_apertura",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5",
            lookback_days=7,
            required_columns=["archivo", "fecha_apertura"],
            min_expected_records=1
        ),
        
        "asignaciones": ExtractionConfig(
            table_name="asignaciones", # Base name
            table_type=TableType.RAW,   # Type updated
            description="Client assignments",
            primary_key=["cod_luna", "cuenta", "archivo", "fecha_asignacion"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_asignacion",
            lookback_days=30,
            batch_size=50000,
            required_columns=["cod_luna", "cuenta", "archivo"],
            min_expected_records=1
        ),
        
        "trandeuda": ExtractionConfig(
            table_name="trandeuda", # Base name
            table_type=TableType.RAW, # Type updated
            description="Daily debt snapshots",
            primary_key=["cod_cuenta", "nro_documento", "archivo", "fecha_proceso"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda",
            lookback_days=14,
            batch_size=100000,
            required_columns=["cod_cuenta", "monto_exigible"],
            min_expected_records=1
        ),
        
        "pagos": ExtractionConfig(
            table_name="pagos", # Base name
            table_type=TableType.RAW, # Type updated
            description="Payment transactions", 
            primary_key=["nro_documento", "fecha_pago", "monto_cancelado"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_pagos",
            lookback_days=30,
            batch_size=25000,
            required_columns=["nro_documento", "fecha_pago", "monto_cancelado"],
            min_expected_records=1
        ),
        
        # Configuration for gestiones_unificadas is REMOVED as per new plan

        "homologacion_mibotair": ExtractionConfig(
            table_name="homologacion_mibotair", # Base name
            table_type=TableType.DIMENSION, # DIMENSION type can be considered RAW for extraction
            description="Homologation rules for human agent interactions (MibotAir)",
            primary_key=["n_1", "n_2", "n_3"],
            incremental_column=None,
            source_table="homologacion_P3fV4dWNeMkN5RJMhV8e_v2",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24,
        ),

        "homologacion_voicebot": ExtractionConfig(
            table_name="homologacion_voicebot", # Base name
            table_type=TableType.DIMENSION, # DIMENSION type can be considered RAW for extraction
            description="Homologation rules for Voicebot interactions",
            primary_key=["bot_management", "bot_sub_management", "bot_compromiso"],
            incremental_column=None,
            source_table="homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24,
        ),

        "ejecutivos": ExtractionConfig(
            table_name="ejecutivos", # Base name
            table_type=TableType.DIMENSION, # DIMENSION type can be considered RAW for extraction
            description="Agent and executive information, mapping email to document ID",
            primary_key=["correo_name"],
            incremental_column=None,
            source_table="sync_mibotair_batch_SYS_user",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24,
        ),

        # NEW: voicebot_gestiones
        "voicebot_gestiones": ExtractionConfig(
            table_name="voicebot_gestiones",
            table_type=TableType.RAW,
            description="Raw Voicebot interactions from flat source",
            primary_key=["uid"],
            incremental_column="date", # Assuming 'date' is the BQ column name
            source_table="sync_voicebot_batch", # Actual BQ table name
            default_mode=ExtractionMode.INCREMENTAL,
            batch_size=50000,
            required_columns=["uid", "date"],
            min_expected_records=0 # Can be 0 if no interactions on a given day
        ),

        # NEW: mibotair_gestiones
        "mibotair_gestiones": ExtractionConfig(
            table_name="mibotair_gestiones",
            table_type=TableType.RAW,
            description="Raw MibotAir interactions from flat source",
            primary_key=["uid"],
            incremental_column="date", # Assuming 'date' is the BQ column name
            source_table="sync_mibotair_batch", # Actual BQ table name
            default_mode=ExtractionMode.INCREMENTAL,
            batch_size=50000,
            required_columns=["uid", "date"],
            min_expected_records=0 # Can be 0 if no interactions on a given day
        )
    }
    
    # 🎯 QUERY TEMPLATES - Using REAL BigQuery field names, keys are now base names
    EXTRACTION_QUERY_TEMPLATES: Dict[str, str] = {
        
        "calendario": """
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
        
        "asignaciones": """
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
        
        "trandeuda": """
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
        
        "pagos": """
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
        
        # Query template for "gestiones_unificadas" is REMOVED

        "homologacion_mibotair": """
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

        "homologacion_voicebot": """
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

        "ejecutivos": """
          SELECT DISTINCT correo_name,
                          TRIM(nombre)        as nombre,
                          document,
                          CURRENT_TIMESTAMP() as extraction_timestamp
          FROM ` mibot-222814.BI_USA.sync_mibotair_batch_SYS_user `
          WHERE id_cliente = 145
            AND {incremental_filter} -- This will be 1=1 for full refresh
          """,

        # NEW: voicebot_gestiones
        "voicebot_gestiones": """
        SELECT
            uid, campaign_id, campaign_name, document, phone, date, management, sub_management,
            weight, origin, fecha_compromiso, compromiso, observacion, project, client,
            duracion, id_telephony, url_record_bot,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{project_id}.{dataset_id}.sync_voicebot_batch`
        WHERE DATE(date) {incremental_filter}
        """,

        # NEW: mibotair_gestiones
        "mibotair_gestiones": """
        SELECT
            uid, campaign_id, campaign_name, document, phone, date, management, sub_management,
            weight, origin, n1, n2, n3, observacion, extra, project, client, nombre_agente,
            correo_agente, duracion, monto_compromiso, fecha_compromiso, url,
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{project_id}.{dataset_id}.sync_mibotair_batch`
        WHERE DATE(date) {incremental_filter}
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
        # For new tables, project_id and dataset_id are also needed for the template
        if table_name in ["voicebot_gestiones", "mibotair_gestiones"]:
            formatted_query = template.format(
                project_id=cls.PROJECT_ID,
                dataset_id=cls.DATASET, # Assuming these new tables are in the same dataset
                incremental_filter=incremental_filter
            )
        else:
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
        
        # Generate filters based on table type and incremental column
        # Assuming incremental_column is 'date' for new tables as per their config
        if table_name in ["voicebot_gestiones", "mibotair_gestiones"]:
             return f"DATE({config.incremental_column}) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name == "calendario": # Base name
            # Use business date: fecha_apertura
            return f"fecha_apertura >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name in ["asignaciones", "trandeuda", "pagos"]: # Base names
            # Use technical date: creado_el (we'll add filename date logic later)
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        # gestiones_unificadas case is removed
        elif config.incremental_column: # Default fallback for other tables with incremental_column
            return f"DATE({config.incremental_column}) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        else: # Should not happen if mode is incremental and no incremental_column
            return "1=1"
    
    @classmethod
    def list_tables(cls) -> List[str]:
        """List all configured tables"""
        return list(cls.EXTRACTION_CONFIGS.keys())
    
    @classmethod
    def get_dashboard_tables(cls) -> List[str]: # TODO: Review which tables are truly for "dashboard"
        """Get core tables needed for dashboard calculation - to be reviewed post-refactor"""
        # This list will likely change significantly with the new architecture.
        # For now, returning a subset of the new base names.
        return [
            "calendario",
            "asignaciones",
            "trandeuda",
            "pagos",
            "voicebot_gestiones",
            "mibotair_gestiones"
            # aux_gestiones_unificadas and mart tables will be built by MartBuilder
        ]
    
    @classmethod
    def get_raw_source_tables(cls) -> List[str]:
        """Get all raw source tables for initial extraction (base names)"""
        # Returns base names of tables configured with TableType.RAW or TableType.DIMENSION
        # as dimensions are also extracted to the raw layer initially.
        return [
            name for name, config in cls.EXTRACTION_CONFIGS.items()
            if config.table_type in [TableType.RAW, TableType.DIMENSION]
        ]

    @classmethod
    def get_fq_table_name(cls, table_base_name: str, table_type: TableType) -> str:
        """
        Constructs the Fully Qualified Table Name (FQN) including schema and project UID.
        Example: raw_P3fV4dWNeMkN5RJMhV8e.calendario
        """
        if table_base_name in ["etl_watermarks", "etl_execution_log", "extraction_metrics"]: # Global tables
            return f"public.{table_base_name}"

        schema_prefix_map = {
            TableType.RAW: "raw",
            TableType.AUX: "aux",
            TableType.MART: "mart",
            TableType.DIMENSION: "raw", # Dimensions are stored in the raw schema for this project
            TableType.DASHBOARD: "mart", # Dashboard tables are a type of mart
            TableType.EVOLUTION: "mart",
            TableType.ASSIGNMENT: "mart",
            TableType.OPERATION: "mart",
            TableType.PRODUCTIVITY: "mart",
        }

        schema_prefix = schema_prefix_map.get(table_type)

        if not schema_prefix:
            # Fallback or error for unmapped TableType
            # For now, defaulting to raw if type is somehow not in map, or raise error
            # This case should ideally be handled by ensuring all configs have valid types
            raise ValueError(f"Unknown table type '{table_type}' for FQN construction of '{table_base_name}'.")

        return f"{schema_prefix}_{cls.PROJECT_UID}.{table_base_name}"


# 🎯 CONVENIENCE CONSTANTS FOR EASY IMPORTS
# These might need adjustment based on how they are used post-refactor
DASHBOARD_TABLES = ETLConfig.get_dashboard_tables()
RAW_SOURCE_TABLES = ETLConfig.get_raw_source_tables()
ALL_TABLES = ETLConfig.list_tables()

# Default extraction configuration (can be removed if not used, or updated)
DEFAULT_CONFIG = ExtractionConfig(
    table_name="default_base_name", # Using a base name now
    table_type=TableType.RAW, # Defaulting to RAW, adjust as needed
    description="Default configuration",
    primary_key=["id"],
    incremental_column="updated_at",
    lookback_days=1
)
