"""
🎯 ETL Configuration System - FIXED Business Logic
Corrected separation between Assignment vs Management logic

CRITICAL FIX: Assignment ≠ Management
- Assignments come from calendario + asignaciones + cuentas  
- Management happens on "gestionable" clients only
- Gestionability = Assigned + Has debt >= 1 + Came in trandeuda
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
    
    CORRECTED: Proper separation of Assignment vs Management logic
    - Assignments = Calendar + Asignaciones + Cuentas + Trandeuda (state)
    - Management = Gestiones on "gestionable" clients only
    - Payments = Independent but linked to cuentas
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
            description="Main dashboard metrics - Assignment-based with Management overlay",
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
        
        # 📋 ASSIGNMENT ANALYSIS - BASED ON ASSIGNMENTS NOT MANAGEMENT
        "assignment_data": ExtractionConfig(
            table_name="assignment_data",
            table_type=TableType.ASSIGNMENT,
            description="Assignment analysis - from Calendar + Asignaciones + Cuentas",
            primary_key=["periodo", "archivo", "cartera"],
            incremental_column="fecha_asignacion_real",  # From assignment, not management
            source_view=f"{VIEW_PREFIX}_asignaciones_clean",
            lookback_days=30,
            refresh_frequency_hours=24,
            required_columns=["periodo", "archivo", "cartera", "cuentas", "clientes", "deuda_asig"],
            min_expected_records=20
        ),
        
        # ⚡ OPERATION HOURLY DATA - MANAGEMENT ONLY
        "operation_data": ExtractionConfig(
            table_name="operation_data",
            table_type=TableType.OPERATION,
            description="Hourly operational metrics - Management actions only",
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
        
        # 👥 PRODUCTIVITY DATA - AGENT MANAGEMENT PERFORMANCE
        "productivity_data": ExtractionConfig(
            table_name="productivity_data",
            table_type=TableType.PRODUCTIVITY, 
            description="Agent productivity - Management performance only",
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
    
    # 🎯 EXTRACTION QUERIES - CORRECTED BUSINESS LOGIC
    EXTRACTION_QUERIES: Dict[str, str] = {
        
        "dashboard_data": f"""
        -- ✅ CORRECTED: Dashboard from Assignment base + Management overlay
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
        -- ✅ CORRECTED: Evolution from Assignment base
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
        -- ✅ CORRECTED: Pure Assignment analysis - NO management mixing
        WITH asignacion_base AS (
            SELECT 
                ac.archivo,
                ac.fecha_asignacion_real,
                ac.cartera,
                ac.negocio as servicio,
                ac.cod_luna,
                ac.cuenta,
                ac.deuda_inicial,
                ac.monto_exigible_diario as deuda_actual,
                ac.estado_deudor,
                EXTRACT(YEAR FROM ac.fecha_asignacion_real) as anno,
                FORMAT_DATE('%Y-%m', ac.fecha_asignacion_real) as periodo
            FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_asignaciones_clean` ac
            WHERE {{incremental_filter}}
        ),
        
        pagos_por_cuenta AS (
            SELECT 
                p.nro_documento,
                SUM(p.monto_cancelado) as monto_recuperado
            FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_pagos_unicos` p
            WHERE p.fecha_pago >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY p.nro_documento
        )
        
        SELECT 
            ab.periodo,
            ab.archivo,
            ab.cartera,
            ab.servicio,
            
            -- Assignment metrics (not management)
            COUNT(DISTINCT ab.cuenta) as cuentas,
            COUNT(DISTINCT ab.cod_luna) as clientes,
            SUM(ab.deuda_inicial) as deuda_asig,
            SUM(ab.deuda_actual) as deuda_actual,
            AVG(ab.deuda_inicial) as ticket_promedio,
            
            -- Gestionability analysis
            COUNT(DISTINCT CASE WHEN ab.estado_deudor = 'GESTIONABLE' THEN ab.cuenta END) as cuentas_gestionables,
            COUNT(DISTINCT CASE WHEN ab.estado_deudor = 'NO_VINO_EN_TRANDEUDA' THEN ab.cuenta END) as cuentas_sin_trandeuda,
            COUNT(DISTINCT CASE WHEN ab.estado_deudor = 'DEUDA_MENOR_1' THEN ab.cuenta END) as cuentas_deuda_menor,
            COUNT(DISTINCT CASE WHEN ab.estado_deudor = 'SIN_MORA' THEN ab.cuenta END) as cuentas_sin_mora,
            COUNT(DISTINCT CASE WHEN ab.estado_deudor = 'NO_VENCIDO' THEN ab.cuenta END) as cuentas_no_vencidas,
            
            -- Recovery (independent of management)
            COALESCE(SUM(ppc.monto_recuperado), 0) as recupero_total,
            
            CURRENT_TIMESTAMP() as fecha_procesamiento
            
        FROM asignacion_base ab
        LEFT JOIN pagos_por_cuenta ppc
            ON CAST(ab.cuenta AS STRING) = ppc.nro_documento
        GROUP BY 1,2,3,4
        ORDER BY periodo DESC, archivo
        """,
        
        "operation_data": f"""
        -- ✅ CORRECTED: Pure Management operations analysis
        WITH gestiones_base AS (
            SELECT 
                DATE(g.timestamp_gestion) as fecha_foto,
                EXTRACT(HOUR FROM g.timestamp_gestion) as hora,
                g.canal_origen as canal,
                g.campaign_name as cola,
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
            
            -- Management metrics only
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
            COUNT(*) - SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_no_efectivos,
            SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END) as total_pdp,
            
            -- Performance calculations
            SAFE_DIVIDE(
                SUM(CASE WHEN es_compromiso THEN 1 ELSE 0 END),
                SUM(CASE WHEN es_contacto_efectivo THEN 1 ELSE 0 END)
            ) * 100 as tasa_cierre,
            
            CURRENT_TIMESTAMP() as fecha_procesamiento
            
        FROM gestiones_base
        GROUP BY 1,2,3,4,5
        ORDER BY fecha_foto DESC, hora, canal, cola
        """,
        
        "productivity_data": f"""
        -- ✅ CORRECTED: Pure Agent productivity in Management
        WITH gestiones_agente AS (
            SELECT 
                DATE(g.timestamp_gestion) as fecha_foto,
                EXTRACT(HOUR FROM g.timestamp_gestion) as hora,
                g.correo_agente,
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
        
        -- Recovery attribution: Only link payments that happened AFTER management
        pagos_atribuibles AS (
            SELECT 
                p.fecha_pago,
                p.nro_documento,
                SUM(p.monto_cancelado) as monto_recuperado
            FROM `{PROJECT_ID}.{DATASET}.{VIEW_PREFIX}_pagos_unicos` p
            WHERE p.fecha_pago >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY 1,2
        ),
        
        -- Link payments to agents who managed those clients
        recupero_por_agente AS (
            SELECT 
                ga.fecha_foto,
                ga.correo_agente,
                SUM(pa.monto_recuperado) as monto_recuperado
            FROM gestiones_agente ga
            INNER JOIN pagos_atribuibles pa
                ON CAST(ga.cod_luna AS STRING) = pa.nro_documento
                AND pa.fecha_pago >= ga.fecha_foto  -- Payment after management
                AND pa.fecha_pago <= DATE_ADD(ga.fecha_foto, INTERVAL 7 DAY)  -- Within 7 days
            GROUP BY 1,2
        )
        
        SELECT 
            ga.fecha_foto,
            ga.hora,
            ga.correo_agente,
            ga.nombre_agente,
            
            -- Agent performance metrics
            COUNT(*) as total_gestiones,
            SUM(CASE WHEN ga.es_contacto_efectivo THEN 1 ELSE 0 END) as contactos_efectivos,
            SUM(CASE WHEN ga.es_compromiso THEN 1 ELSE 0 END) as total_pdp,
            SUM(ga.peso_gestion) as peso_total,
            COALESCE(rpa.monto_recuperado, 0) as monto_recuperado,
            
            -- Performance ratios
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
        LEFT JOIN recupero_por_agente rpa
            ON ga.fecha_foto = rpa.fecha_foto
            AND ga.correo_agente = rpa.correo_agente
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
        
        # Different filter logic based on table type
        if config.table_type == TableType.ASSIGNMENT:
            # Assignment uses assignment date, not management date
            return f"fecha_asignacion_real >= '{lookback_date.strftime('%Y-%m-%d')}'"
        else:
            # Management tables use management/processing date
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
