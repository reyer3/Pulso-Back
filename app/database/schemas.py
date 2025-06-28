"""
🗃️ Database Schemas for TimescaleDB
Production-ready table schemas optimized for time-series data

Features:
- TimescaleDB hypertables for time-series optimization
- Proper indexing for query performance
- Data retention policies
- Partitioning strategies
"""

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, Date, Text, Index,
    PrimaryKeyConstraint, UniqueConstraint, CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
import uuid

Base = declarative_base()

# =============================================================================
# MAIN DASHBOARD TABLES
# =============================================================================

class DashboardData(Base):
    """
    Main dashboard data table - TimescaleDB hypertable
    
    Primary key: (fecha_foto, archivo, cartera, servicio)
    Partitioned by: fecha_foto (time dimension)
    """
    __tablename__ = 'dashboard_data'
    
    # Primary key components (matching ETL config)
    fecha_foto = Column(Date, nullable=False, comment="Snapshot date")
    archivo = Column(String(100), nullable=False, comment="Campaign file identifier")
    cartera = Column(String(50), nullable=False, comment="Portfolio type")
    servicio = Column(String(20), nullable=False, comment="Service type (MOVIL/FIJA)")
    
    # Volume metrics
    cuentas = Column(Integer, nullable=False, default=0, comment="Total accounts")
    clientes = Column(Integer, nullable=False, default=0, comment="Total clients")
    deuda_asig = Column(Float, nullable=False, default=0.0, comment="Assigned debt amount")
    deuda_act = Column(Float, nullable=False, default=0.0, comment="Current debt amount")
    
    # Management metrics  
    cuentas_gestionadas = Column(Integer, nullable=False, default=0, comment="Managed accounts")
    cuentas_cd = Column(Integer, nullable=False, default=0, comment="Direct contact accounts")
    cuentas_ci = Column(Integer, nullable=False, default=0, comment="Indirect contact accounts")
    cuentas_sc = Column(Integer, nullable=False, default=0, comment="No contact accounts")
    cuentas_sg = Column(Integer, nullable=False, default=0, comment="No management accounts")
    cuentas_pdp = Column(Integer, nullable=False, default=0, comment="PDP accounts")
    
    # Recovery metrics
    recupero = Column(Float, nullable=False, default=0.0, comment="Amount recovered")
    
    # Calculated KPIs (percentages)
    pct_cober = Column(Float, nullable=False, default=0.0, comment="Coverage percentage")
    pct_contac = Column(Float, nullable=False, default=0.0, comment="Contact percentage") 
    pct_cd = Column(Float, nullable=False, default=0.0, comment="Direct contact percentage")
    pct_ci = Column(Float, nullable=False, default=0.0, comment="Indirect contact percentage")
    pct_conversion = Column(Float, nullable=False, default=0.0, comment="PDP conversion percentage")
    pct_efectividad = Column(Float, nullable=False, default=0.0, comment="Effectiveness percentage")
    pct_cierre = Column(Float, nullable=False, default=0.0, comment="Closure percentage")
    inten = Column(Float, nullable=False, default=0.0, comment="Management intensity")
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now(), comment="Processing timestamp")
    
    # Primary key constraint
    __table_args__ = (
        PrimaryKeyConstraint('fecha_foto', 'archivo', 'cartera', 'servicio'),
        Index('idx_dashboard_data_fecha_foto', 'fecha_foto'),
        Index('idx_dashboard_data_cartera', 'cartera'),
        Index('idx_dashboard_data_servicio', 'servicio'),
        Index('idx_dashboard_data_procesamiento', 'fecha_procesamiento'),
        CheckConstraint('cuentas >= 0', name='chk_dashboard_cuentas_positive'),
        CheckConstraint('deuda_asig >= 0', name='chk_dashboard_deuda_positive'),
        {'comment': 'Main dashboard metrics by date, campaign, portfolio and service'}
    )


class EvolutionData(Base):
    """
    Evolution/trending data table - TimescaleDB hypertable
    
    Primary key: (fecha_foto, archivo)
    Partitioned by: fecha_foto (time dimension)
    """
    __tablename__ = 'evolution_data'
    
    # Primary key components
    fecha_foto = Column(Date, nullable=False, comment="Snapshot date")
    archivo = Column(String(100), nullable=False, comment="Campaign file identifier")
    
    # Dimension info
    cartera = Column(String(50), nullable=False, comment="Portfolio type")
    servicio = Column(String(20), nullable=False, comment="Service type")
    
    # Evolution metrics
    pct_cober = Column(Float, nullable=False, default=0.0, comment="Coverage percentage")
    pct_contac = Column(Float, nullable=False, default=0.0, comment="Contact percentage")
    pct_efectividad = Column(Float, nullable=False, default=0.0, comment="Effectiveness percentage")
    pct_cierre = Column(Float, nullable=False, default=0.0, comment="Closure percentage")
    recupero = Column(Float, nullable=False, default=0.0, comment="Amount recovered")
    cuentas = Column(Integer, nullable=False, default=0, comment="Total accounts")
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        PrimaryKeyConstraint('fecha_foto', 'archivo'),
        Index('idx_evolution_data_fecha_foto', 'fecha_foto'),
        Index('idx_evolution_data_cartera', 'cartera'),
        Index('idx_evolution_data_composite', 'fecha_foto', 'cartera'),
        {'comment': 'Time series data for evolution analysis'}
    )


class AssignmentData(Base):
    """
    Assignment analysis data table
    
    Primary key: (periodo, archivo, cartera)
    Partitioned by: periodo (monthly partitioning)
    """
    __tablename__ = 'assignment_data'
    
    # Primary key components
    periodo = Column(String(7), nullable=False, comment="Period YYYY-MM")
    archivo = Column(String(100), nullable=False, comment="Campaign file identifier")
    cartera = Column(String(50), nullable=False, comment="Portfolio type")
    
    # Volume metrics
    clientes = Column(Integer, nullable=False, default=0, comment="Total clients")
    cuentas = Column(Integer, nullable=False, default=0, comment="Total accounts")
    deuda_asig = Column(Float, nullable=False, default=0.0, comment="Assigned debt")
    deuda_actual = Column(Float, nullable=False, default=0.0, comment="Current debt")
    
    # Calculated metrics
    ticket_promedio = Column(Float, nullable=False, default=0.0, comment="Average ticket")
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        PrimaryKeyConstraint('periodo', 'archivo', 'cartera'),
        Index('idx_assignment_data_periodo', 'periodo'),
        Index('idx_assignment_data_cartera', 'cartera'),
        {'comment': 'Assignment composition data by period and portfolio'}
    )


class OperationData(Base):
    """
    Operation/GTR data table - TimescaleDB hypertable
    
    Primary key: (fecha_foto, hora, canal, archivo)
    Partitioned by: fecha_foto (time dimension)
    """
    __tablename__ = 'operation_data'
    
    # Primary key components
    fecha_foto = Column(Date, nullable=False, comment="Operation date")
    hora = Column(Integer, nullable=False, comment="Hour (0-23)")
    canal = Column(String(20), nullable=False, comment="Channel (BOT/HUMANO)")
    archivo = Column(String(100), nullable=False, default='GENERAL', comment="Campaign identifier")
    
    # Operation metrics
    total_gestiones = Column(Integer, nullable=False, default=0, comment="Total management actions")
    contactos_efectivos = Column(Integer, nullable=False, default=0, comment="Effective contacts")
    contactos_no_efectivos = Column(Integer, nullable=False, default=0, comment="Non-effective contacts")
    total_pdp = Column(Integer, nullable=False, default=0, comment="Payment promises")
    
    # Calculated metrics
    tasa_contacto = Column(Float, nullable=False, default=0.0, comment="Contact rate percentage")
    tasa_conversion = Column(Float, nullable=False, default=0.0, comment="PDP conversion rate")
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        PrimaryKeyConstraint('fecha_foto', 'hora', 'canal', 'archivo'),
        Index('idx_operation_data_fecha_foto', 'fecha_foto'),
        Index('idx_operation_data_hora', 'hora'),
        Index('idx_operation_data_canal', 'canal'),
        Index('idx_operation_data_composite', 'fecha_foto', 'canal'),
        CheckConstraint('hora >= 0 AND hora <= 23', name='chk_operation_hora_valid'),
        {'comment': 'Hourly operational metrics by channel'}
    )


class ProductivityData(Base):
    """
    Productivity data table - TimescaleDB hypertable
    
    Primary key: (fecha_foto, correo_agente, archivo)
    Partitioned by: fecha_foto (time dimension)
    """
    __tablename__ = 'productivity_data'
    
    # Primary key components
    fecha_foto = Column(Date, nullable=False, comment="Performance date")
    correo_agente = Column(String(100), nullable=False, comment="Agent email")
    archivo = Column(String(100), nullable=False, default='GENERAL', comment="Campaign identifier")
    
    # Performance metrics
    total_gestiones = Column(Integer, nullable=False, default=0, comment="Total management actions")
    contactos_efectivos = Column(Integer, nullable=False, default=0, comment="Effective contacts")
    total_pdp = Column(Integer, nullable=False, default=0, comment="Payment promises")
    peso_total = Column(Float, nullable=False, default=0.0, comment="Total weight/score")
    
    # Calculated metrics
    tasa_contacto = Column(Float, nullable=False, default=0.0, comment="Contact rate")
    tasa_conversion = Column(Float, nullable=False, default=0.0, comment="PDP conversion rate")
    score_productividad = Column(Float, nullable=False, default=0.0, comment="Productivity score")
    
    # Agent info (denormalized for performance)
    nombre_agente = Column(String(100), comment="Agent full name")
    dni_agente = Column(String(20), comment="Agent DNI")
    equipo = Column(String(50), comment="Team name")
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        PrimaryKeyConstraint('fecha_foto', 'correo_agente', 'archivo'),
        Index('idx_productivity_data_fecha_foto', 'fecha_foto'),
        Index('idx_productivity_data_agente', 'correo_agente'),
        Index('idx_productivity_data_equipo', 'equipo'),
        Index('idx_productivity_data_composite', 'fecha_foto', 'equipo'),
        {'comment': 'Daily productivity metrics by agent'}
    )


# =============================================================================
# ETL CONTROL TABLES
# =============================================================================

class ETLWatermarks(Base):
    """
    ETL watermark tracking table (already defined in watermarks.py but here for completeness)
    """
    __tablename__ = 'etl_watermarks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(100), nullable=False, unique=True, comment="Target table name")
    last_extracted_at = Column(DateTime(timezone=True), nullable=False, comment="Last extraction timestamp")
    last_extraction_status = Column(String(20), nullable=False, default='success', comment="Last extraction status")
    records_extracted = Column(Integer, default=0, comment="Records in last extraction")
    extraction_duration_seconds = Column(Float, default=0.0, comment="Last extraction duration")
    error_message = Column(Text, comment="Error message if failed")
    extraction_id = Column(String(50), comment="Unique extraction identifier")
    metadata = Column(JSONB, comment="Additional extraction metadata")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_etl_watermarks_table_name', 'table_name'),
        Index('idx_etl_watermarks_status', 'last_extraction_status'),
        Index('idx_etl_watermarks_updated', 'updated_at'),
        {'comment': 'ETL watermark tracking for incremental extractions'}
    )


class ETLExecutionLog(Base):
    """
    ETL execution log for monitoring and debugging
    """
    __tablename__ = 'etl_execution_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(50), nullable=False, comment="Unique execution identifier")
    table_name = Column(String(100), nullable=False, comment="Target table name")
    execution_type = Column(String(20), nullable=False, comment="incremental/full_refresh/manual")
    status = Column(String(20), nullable=False, comment="running/success/failed")
    
    # Execution metrics
    records_processed = Column(Integer, default=0, comment="Total records processed")
    records_inserted = Column(Integer, default=0, comment="Records inserted")
    records_updated = Column(Integer, default=0, comment="Records updated")
    records_skipped = Column(Integer, default=0, comment="Records skipped")
    
    # Timing
    started_at = Column(DateTime(timezone=True), nullable=False, comment="Execution start time")
    completed_at = Column(DateTime(timezone=True), comment="Execution completion time")
    duration_seconds = Column(Float, comment="Total execution duration")
    
    # Error handling
    error_message = Column(Text, comment="Error message if failed")
    stack_trace = Column(Text, comment="Full stack trace if failed")
    
    # Metadata
    extraction_mode = Column(String(20), comment="Extraction mode used")
    source_query = Column(Text, comment="SQL query executed on source")
    metadata = Column(JSONB, comment="Additional execution metadata")
    
    __table_args__ = (
        Index('idx_etl_execution_log_execution_id', 'execution_id'),
        Index('idx_etl_execution_log_table_name', 'table_name'),
        Index('idx_etl_execution_log_status', 'status'),
        Index('idx_etl_execution_log_started', 'started_at'),
        {'comment': 'Detailed ETL execution log for monitoring and debugging'}
    )


# =============================================================================
# TIMESCALEDB CONFIGURATION HELPERS
# =============================================================================

# Tables that should be converted to TimescaleDB hypertables
TIMESCALE_HYPERTABLES = {
    'dashboard_data': {
        'time_column': 'fecha_foto',
        'chunk_time_interval': '7 days',  # Weekly chunks
        'retention_policy': '2 years'
    },
    'evolution_data': {
        'time_column': 'fecha_foto', 
        'chunk_time_interval': '7 days',
        'retention_policy': '2 years'
    },
    'operation_data': {
        'time_column': 'fecha_foto',
        'chunk_time_interval': '1 day',   # Daily chunks for high frequency data
        'retention_policy': '1 year'
    },
    'productivity_data': {
        'time_column': 'fecha_foto',
        'chunk_time_interval': '7 days',
        'retention_policy': '2 years'
    },
    'etl_execution_log': {
        'time_column': 'started_at',
        'chunk_time_interval': '1 month',  # Monthly chunks for logs
        'retention_policy': '6 months'
    }
}

# Continuous aggregates for performance optimization
CONTINUOUS_AGGREGATES = {
    'dashboard_data_weekly': {
        'source_table': 'dashboard_data',
        'time_bucket': '1 week',
        'group_by': ['cartera', 'servicio'],
        'aggregates': ['SUM(cuentas)', 'AVG(pct_cober)', 'SUM(recupero)']
    },
    'operation_data_daily': {
        'source_table': 'operation_data', 
        'time_bucket': '1 day',
        'group_by': ['canal'],
        'aggregates': ['SUM(total_gestiones)', 'AVG(tasa_contacto)']
    }
}
