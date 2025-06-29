"""
🎯 ETL Configuration System - CORRECTED SCHEMA VERSION
Raw data extraction with real BigQuery column names and simplified approach

SCHEMA VERIFIED: All column names and types confirmed from actual BigQuery tables
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
    Centralized ETL configuration for Pulso Dashboard - SCHEMA CORRECTED VERSION
    
    STRATEGY: Extract raw sources separately, transform in Python pipeline
    VERIFIED: All column names match real BigQuery schemas
    """
    
    # 🌟 PROJECT CONFIGURATION
    PROJECT_ID = "mibot-222814"
    DATASET = "BI_USA"
    
    # 🔄 RAW SOURCE CONFIGURATIONS - One per BigQuery table
    EXTRACTION_CONFIGS: Dict[str, ExtractionConfig] = {
        
        # 📅 CALENDARIO - Campaign definitions
        "raw_calendario": ExtractionConfig(
            table_name="raw_calendario",
            table_type=TableType.DASHBOARD,
            description="Campaign calendar - corrected schema",
            primary_key=["ARCHIVO"],
            incremental_column="fecha_apertura",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5",
            lookback_days=7,
            required_columns=["ARCHIVO", "fecha_apertura"],
            min_expected_records=1
        ),
        
        # 👥 ASIGNACIONES - Client assignments
        "raw_asignaciones": ExtractionConfig(
            table_name="raw_asignaciones",
            table_type=TableType.ASSIGNMENT,
            description="Client assignments - corrected schema",
            primary_key=["cod_luna", "cuenta", "archivo"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_asignacion",
            lookback_days=30,
            batch_size=50000,
            required_columns=["cod_luna", "cuenta", "archivo"],
            min_expected_records=1
        ),
        
        # 💰 TRANDEUDA - Daily debt snapshots
        "raw_trandeuda": ExtractionConfig(
            table_name="raw_trandeuda", 
            table_type=TableType.DASHBOARD,
            description="Daily debt snapshots - corrected schema",
            primary_key=["cod_cuenta", "nro_documento", "archivo"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda",
            lookback_days=14,
            batch_size=100000,
            required_columns=["cod_cuenta", "monto_exigible"],
            min_expected_records=1
        ),
        
        # 💳 PAGOS - Payment transactions
        "raw_pagos": ExtractionConfig(
            table_name="raw_pagos",
            table_type=TableType.DASHBOARD,
            description="Payment transactions - corrected schema", 
            primary_key=["nro_documento", "fecha_pago", "monto_cancelado"],
            incremental_column="creado_el",
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_pagos",
            lookback_days=30,
            batch_size=25000,
            required_columns=["nro_documento", "fecha_pago", "monto_cancelado"],
            min_expected_records=1
        ),
        
        # 🤖 GESTIONES BOT - Bot management actions
        "raw_gestiones_bot": ExtractionConfig(
            table_name="raw_gestiones_bot",
            table_type=TableType.OPERATION,
            description="Bot gestiones - corrected schema",
            primary_key=["document", "date", "uid"],
            incremental_column="date",
            source_table="voicebot_P3fV4dWNeMkN5RJMhV8e",
            lookback_days=5,
            batch_size=50000,
            refresh_frequency_hours=2,
            required_columns=["document", "date", "management"],
            min_expected_records=1
        ),
        
        # 👨‍💼 GESTIONES HUMANO - Human agent actions  
        "raw_gestiones_humano": ExtractionConfig(
            table_name="raw_gestiones_humano",
            table_type=TableType.PRODUCTIVITY,
            description="Human agent gestiones - corrected schema",
            primary_key=["document", "date", "uid"],
            incremental_column="date",
            source_table="mibotair_P3fV4dWNeMkN5RJMhV8e",
            lookback_days=5,
            batch_size=25000,
            refresh_frequency_hours=2,
            required_columns=["document", "date", "correo_agente"],
            min_expected_records=1
        ),
        
        # 📞 CONTACTOS - Contact effectiveness data
        "raw_contactos": ExtractionConfig(
            table_name="raw_contactos",
            table_type=TableType.OPERATION,
            description="Contact effectiveness master data",
            primary_key=["cod_luna", "archivo"],
            incremental_column="creado_el", 
            source_table="batch_P3fV4dWNeMkN5RJMhV8e_master_contacto",
            lookback_days=30,
            required_columns=["cod_luna", "valor_contacto"],
            min_expected_records=1
        ),
        
        # 🎯 GESTIONES UNIFICADAS - Pre-built view with homologation
        "gestiones_unificadas": ExtractionConfig(
            table_name="gestiones_unificadas",
            table_type=TableType.OPERATION,
            description="Unified gestiones view - bot + human with homologation",
            primary_key=["cod_luna", "timestamp_gestion"],
            incremental_column="timestamp_gestion",
            source_table="bi_P3fV4dWNeMkN5RJMhV8e_vw_gestiones_unificadas", 
            lookback_days=3,
            batch_size=75000,
            refresh_frequency_hours=1,
            required_columns=["cod_luna", "fecha_gestion", "contactabilidad"],
            min_expected_records=1
        )
    }
    
    # 🎯 SIMPLIFIED RAW EXTRACTION QUERIES - CORRECTED SCHEMA
    EXTRACTION_QUERIES: Dict[str, str] = {
        
        # 📅 CALENDARIO - Campaign calendar with CORRECT column names
        "raw_calendario": f"""
        SELECT 
            ARCHIVO,                          -- ✅ Real column name
            TIPO_CARTERA,                     -- ✅ Real column name
            fecha_apertura,                   -- ✅ Real column name
            fecha_trandeuda,                  -- ✅ Real column name  
            fecha_cierre,                     -- ✅ Real column name
            FECHA_CIERRE_PLANIFICADA,         -- ✅ Real column name
            DURACION_CAMPANA_DIAS_HABILES,    -- ✅ Real column name
            ANNO_ASIGNACION,                  -- ✅ CORRECTED: Not FECHA_ASIGNACION
            PERIODO_ASIGNACION,               -- ✅ Real column name
            ES_CARTERA_ABIERTA,               -- ✅ Real column name
            RANGO_VENCIMIENTO,                -- ✅ Real column name
            ESTADO_CARTERA,                   -- ✅ Real column name
            periodo_mes,                      -- ✅ Real column name
            periodo_date,                     -- ✅ Real column name
            tipo_ciclo_campana,               -- ✅ Real column name
            categoria_duracion,               -- ✅ Real column name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
        WHERE {{incremental_filter}}
        """,
        
        # 👥 ASIGNACIONES - Client assignments with CORRECT column names and types
        "raw_asignaciones": f"""
        SELECT 
            CAST(cliente AS STRING) as cliente,        -- ✅ Convert INT64 to STRING
            CAST(cuenta AS STRING) as cuenta,          -- ✅ Convert INT64 to STRING  
            CAST(cod_luna AS STRING) as cod_luna,      -- ✅ Convert INT64 to STRING
            CAST(telefono AS STRING) as telefono,      -- ✅ Convert INT64 to STRING
            tramo_gestion,                             -- ✅ Real column name
            min_vto,                                   -- ✅ Real column name
            negocio,                                   -- ✅ Real column name
            dias_sin_trafico,                          -- ✅ Real column name
            decil_contacto,                            -- ✅ Real column name
            decil_pago,                                -- ✅ Real column name
            zona,                                      -- ✅ Real column name
            rango_renta,                               -- ✅ Real column name
            campania_act,                              -- ✅ Real column name
            archivo,                                   -- ✅ Real column name
            creado_el,                                 -- ✅ Real column name
            DATE(creado_el) as fecha_asignacion,       -- ✅ Derived date field
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE {{incremental_filter}}
        """,
        
        # 💰 TRANDEUDA - Debt snapshots with CORRECT column names and types
        "raw_trandeuda": f"""
        SELECT 
            cod_cuenta,                                -- ✅ Real column name (STRING)
            nro_documento,                             -- ✅ Real column name
            fecha_vencimiento,                         -- ✅ Real column name
            monto_exigible,                            -- ✅ Real column name (FLOAT64)
            archivo,                                   -- ✅ Real column name
            creado_el,                                 -- ✅ Real column name
            DATE(creado_el) as fecha_proceso,          -- ✅ Derived date field
            motivo_rechazo,                            -- ✅ Real column name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE {{incremental_filter}}
          AND monto_exigible > 0                      -- ✅ Only active debt
          AND motivo_rechazo IS NULL                  -- ✅ Only valid records
        """,
        
        # 💳 PAGOS - Payment transactions with CORRECT column names
        "raw_pagos": f"""
        SELECT 
            cod_sistema,                               -- ✅ Real column name
            nro_documento,                             -- ✅ Real column name
            monto_cancelado,                           -- ✅ Real column name (FLOAT64)
            fecha_pago,                                -- ✅ Real column name
            archivo,                                   -- ✅ Real column name
            creado_el,                                 -- ✅ Real column name
            motivo_rechazo,                            -- ✅ Real column name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE {{incremental_filter}}
          AND monto_cancelado > 0                     -- ✅ Only positive payments
          AND motivo_rechazo IS NULL                  -- ✅ Only valid records
        """,
        
        # 🤖 GESTIONES BOT - Bot management with CORRECT table and columns
        "raw_gestiones_bot": f"""
        SELECT 
            document,                                  -- ✅ Real column name (STRING)
            date,                                      -- ✅ Real column name (DATETIME)
            campaign_id,                               -- ✅ Real column name
            campaign_name,                             -- ✅ Real column name
            CAST(phone AS STRING) as phone,            -- ✅ Convert FLOAT64 to STRING
            management,                                -- ✅ Real column name
            sub_management,                            -- ✅ Real column name
            weight,                                    -- ✅ Real column name (INT64)
            origin,                                    -- ✅ Real column name
            fecha_compromiso,                          -- ✅ Real column name
            interes,                                   -- ✅ Real column name
            compromiso,                                -- ✅ Real column name
            observacion,                               -- ✅ Real column name
            project,                                   -- ✅ Real column name
            client,                                    -- ✅ Real column name
            uid,                                       -- ✅ Real column name
            duracion,                                  -- ✅ Real column name
            DATE(date) as fecha_gestion,               -- ✅ Derived date field
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE {{incremental_filter}}
        """,
        
        # 👨‍💼 GESTIONES HUMANO - Human agent management with CORRECT table and columns
        "raw_gestiones_humano": f"""
        SELECT 
            document,                                  -- ✅ Real column name (STRING)
            date,                                      -- ✅ Real column name (DATETIME)
            campaign_id,                               -- ✅ Real column name
            campaign_name,                             -- ✅ Real column name
            CAST(phone AS STRING) as phone,            -- ✅ Convert FLOAT64 to STRING
            management,                                -- ✅ Real column name
            sub_management,                            -- ✅ Real column name
            weight,                                    -- ✅ Real column name (INT64)
            origin,                                    -- ✅ Real column name
            n1,                                        -- ✅ Real column name
            n2,                                        -- ✅ Real column name
            n3,                                        -- ✅ Real column name
            observacion,                               -- ✅ Real column name
            extra,                                     -- ✅ Real column name
            project,                                   -- ✅ Real column name
            client,                                    -- ✅ Real column name
            uid,                                       -- ✅ Real column name
            nombre_agente,                             -- ✅ Real column name
            correo_agente,                             -- ✅ Real column name
            duracion,                                  -- ✅ Real column name
            monto_compromiso,                          -- ✅ Real column name (FLOAT64)
            fecha_compromiso,                          -- ✅ Real column name (DATE)
            DATE(date) as fecha_gestion,               -- ✅ Derived date field
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE {{incremental_filter}}
          AND correo_agente IS NOT NULL              -- ✅ Only identified agents
        """,
        
        # 📞 CONTACTOS - Contact effectiveness data
        "raw_contactos": f"""
        SELECT 
            cod_luna,                                  -- ✅ Real column name
            valor_contacto,                            -- ✅ Real column name
            archivo,                                   -- ✅ Real column name
            creado_el,                                 -- ✅ Real column name
            motivo_rechazo,                            -- ✅ Real column name
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.batch_P3fV4dWNeMkN5RJMhV8e_master_contacto`
        WHERE {{incremental_filter}}
          AND motivo_rechazo IS NULL                  -- ✅ Only valid records
        """,
        
        # 🎯 GESTIONES UNIFICADAS - Pre-built view with homologation (BEST FOR KPIs)
        "gestiones_unificadas": f"""
        SELECT 
            CAST(cod_luna AS STRING) as cod_luna,      -- ✅ Ensure STRING type
            fecha_gestion,                             -- ✅ Real column name (DATE)
            timestamp_gestion,                         -- ✅ Real column name (TIMESTAMP)
            canal_origen,                              -- ✅ Real column name ('BOT'|'HUMANO')
            management_original,                       -- ✅ Real column name
            sub_management_original,                   -- ✅ Real column name
            compromiso_original,                       -- ✅ Real column name
            nivel_1,                                   -- ✅ Real column name (homologated)
            nivel_2,                                   -- ✅ Real column name (homologated)
            contactabilidad,                           -- ✅ Real column name (homologated)
            es_contacto_efectivo,                      -- ✅ Real column name (BOOL) - FOR PCT_CONTAC
            es_contacto_no_efectivo,                   -- ✅ Real column name (BOOL)
            es_compromiso,                             -- ✅ Real column name (BOOL) - FOR PCT_EFECTIVIDAD
            peso_gestion,                              -- ✅ Real column name (INT64)
            CURRENT_TIMESTAMP() as extraction_timestamp
        FROM `{PROJECT_ID}.{DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_vw_gestiones_unificadas`
        WHERE {{incremental_filter}}
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
    def get_query(cls, table_name: str) -> str:
        """Get extraction query for a specific table"""
        if table_name not in cls.EXTRACTION_QUERIES:
            raise ValueError(f"No query found for table: {table_name}")
        return cls.EXTRACTION_QUERIES[table_name]
    
    @classmethod
    def get_incremental_filter(cls, table_name: str, since_date: datetime) -> str:
        """
        Generate incremental filter for a specific table with CORRECTED column names
        
        Args:
            table_name: Name of the table
            since_date: Extract data since this date
            
        Returns:
            SQL WHERE clause for incremental extraction
        """
        config = cls.get_config(table_name)
        
        # Apply lookback window for data quality
        lookback_date = since_date - timedelta(days=config.lookback_days)
        
        # CORRECTED filters based on real column names
        if table_name == "raw_calendario":
            # Use real column: fecha_apertura
            return f"fecha_apertura >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name == "raw_asignaciones":
            # Use real column: creado_el
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name in ["raw_trandeuda", "raw_pagos", "raw_contactos"]:
            # Use real column: creado_el
            return f"DATE(creado_el) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name in ["raw_gestiones_bot", "raw_gestiones_humano"]:
            # Use real column: date (DATETIME)
            return f"DATE(date) >= '{lookback_date.strftime('%Y-%m-%d')}'"
        elif table_name == "gestiones_unificadas":
            # Use real column: timestamp_gestion (TIMESTAMP)
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
