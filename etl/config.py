# app/etl/config.py

"""
🎯 ETL Configuration System (Metadata-Only)

This file defines the METADATA and STRATEGY for each table in the ETL process.
The SQL logic for extraction is now decoupled and managed in dedicated .sql files
within the `app/etl/sql/` directory, adhering to the Separation of Concerns principle.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

# --- ENUMERATIONS FOR TYPE SAFETY ---

class ExtractionMode(str, Enum):
    """Defines the mode of data extraction."""
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"

class TableType(str, Enum):
    """Defines the architectural layer of a table."""
    RAW = "raw"
    AUX = "aux"
    MART = "mart"
    DIMENSION = "dimension" # Stored in RAW schema, but logically a dimension.

# --- DATA CLASSES FOR CONFIGURATION ---

@dataclass
class ExtractionConfig:
    """Configuration for a specific table extraction."""
    table_name: str                 # Base name of the table (e.g., "calendario")
    table_type: TableType             # Architectural layer (e.g., TableType.RAW)
    description: str                # Human-readable description
    primary_key: List[str]            # List of primary key columns for upserts
    incremental_column: Optional[str] # Column used for incremental date filtering
    source_table: str                 # Source table name in BigQuery
    default_mode: ExtractionMode = ExtractionMode.INCREMENTAL
    lookback_days: int = 7
    batch_size: int = 10000
    refresh_frequency_hours: int = 6

# --- MAIN CONFIGURATION CLASS ---

class ETLConfig:
    """
    Centralized ETL configuration for the Pulso-Back project.
    This class holds all table metadata. The SQL logic is externalized.
    """
    
    # --- GLOBAL PROJECT CONSTANTS ---
    PROJECT_ID = "mibot-222814"
    PROJECT_UID = "P3fV4dWNeMkN5RJMhV8e"
    BQ_DATASET = "BI_USA"
    
    # --- TABLE METADATA CONFIGURATIONS ---
    # Keys are the base names of the tables, used to find the corresponding .sql file.
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        "calendario": ExtractionConfig(
            table_name="calendario",
            table_type=TableType.RAW,
            description="Campaign calendar definitions.",
            primary_key=["archivo"],
            incremental_column="fecha_apertura",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5"
        ),
        
        "asignaciones": ExtractionConfig(
            table_name="asignaciones",
            table_type=TableType.RAW,
            description="Client account assignments to campaigns.",
            primary_key=["cod_luna", "cuenta", "archivo"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_asignacion",
            batch_size=50000
        ),
        
        "trandeuda": ExtractionConfig(
            table_name="trandeuda",
            table_type=TableType.RAW,
            description="Daily debt snapshots.",
            primary_key=["cod_cuenta", "nro_documento", "archivo", "fecha_proceso"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda",
            batch_size=100000
        ),
        
        "pagos": ExtractionConfig(
            table_name="pagos",
            table_type=TableType.RAW,
            description="Payment transactions.",
            primary_key=["nro_documento", "fecha_pago", "monto_cancelado"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_pagos",
            batch_size=25000
        ),
        
        "voicebot_gestiones": ExtractionConfig(
            table_name="voicebot_gestiones",
            table_type=TableType.RAW,
            description="Raw Voicebot interactions from flat source.",
            primary_key=["uid"],
            incremental_column="date",
            source_table="sync_voicebot_batch",
            batch_size=50000
        ),

        "mibotair_gestiones": ExtractionConfig(
            table_name="mibotair_gestiones",
            table_type=TableType.RAW,
            description="Raw MibotAir interactions from flat source.",
            primary_key=["uid"],
            incremental_column="date",
            source_table="sync_mibotair_batch",
            batch_size=50000
        ),

        "homologacion_mibotair": ExtractionConfig(
            table_name="homologacion_mibotair",
            table_type=TableType.DIMENSION,
            description="Homologation rules for human agents (MibotAir).",
            primary_key=["n_1", "n_2", "n_3"],
            incremental_column=None,
            source_table="homologacion_P3fV4dWNeMkN5RJMhV8e_v2",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24
        ),

        "homologacion_voicebot": ExtractionConfig(
            table_name="homologacion_voicebot",
            table_type=TableType.DIMENSION,
            description="Homologation rules for Voicebot interactions.",
            primary_key=["bot_management", "bot_sub_management", "bot_compromiso"],
            incremental_column=None,
            source_table="homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24
        ),

        "ejecutivos": ExtractionConfig(
            table_name="ejecutivos",
            table_type=TableType.DIMENSION,
            description="Agent and executive information.",
            primary_key=["correo_name"],
            incremental_column=None,
            source_table="sync_mibotair_batch_SYS_user",
            default_mode=ExtractionMode.FULL_REFRESH,
            refresh_frequency_hours=24
        )
    }

    # --- HELPER METHODS CORREGIDOS Y NUEVOS ---

    @classmethod
    def get_config(cls, table_name: str) -> ExtractionConfig:
        """Retrieves the configuration for a specific table by its base name."""
        config = cls.EXTRACTION_CONFIGS.get(table_name)
        if not config:
            raise ValueError(f"No configuration found for table: {table_name}")
        return config

    @classmethod
    def get_raw_source_tables(cls) -> List[str]:
        """
        🆕 NUEVO: Lista las tablas que son fuentes RAW para extracción.
        Retorna nombres base de tablas, no nombres completos con schema.
        """
        raw_tables = [
            table_name for table_name, config in cls.EXTRACTION_CONFIGS.items()
            if config.table_type in [TableType.RAW, TableType.DIMENSION]
        ]
        return raw_tables

    @classmethod
    def get_fq_table_name(cls, table_base_name: str) -> str:
        """Constructs the Fully Qualified Table Name (FQN) including schema."""
        if table_base_name in ["etl_watermarks", "etl_execution_log"]:
            return f"public.{table_base_name}"

        config = cls.get_config(table_base_name)
        table_type = config.table_type

        schema_prefix_map = {
            TableType.RAW: "raw",
            TableType.AUX: "aux",
            TableType.MART: "mart",
            TableType.DIMENSION: "raw",  # Dimensions are stored in the raw schema
        }

        schema_prefix = schema_prefix_map.get(table_type)
        if not schema_prefix:
            raise ValueError(f"Unknown table type '{table_type}' for FQN construction of '{table_base_name}'.")

        return f"{schema_prefix}_{cls.PROJECT_UID}.{table_base_name}"

    @classmethod
    def list_extractable_tables(cls) -> List[str]:
        """Lists all tables that are directly extracted from a source."""
        return list(cls.EXTRACTION_CONFIGS.keys())