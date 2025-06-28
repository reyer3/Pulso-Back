"""
🗃️ Database Models for TimescaleDB Tables
SQLAlchemy models that map database schemas to application objects

These models correspond to the schemas defined in app/database/schemas.py
and provide the ORM interface for the ETL and API layers.
"""

from datetime import date, datetime
from typing import Optional, Dict, Any
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Date, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.models.base import TimestampMixin

Base = declarative_base()


class DashboardDataModel(Base, TimestampMixin):
    """
    SQLAlchemy model for dashboard_data table
    Maps to TypeScript: DataRow interface
    """
    __tablename__ = 'dashboard_data'
    
    # Primary key (composite)
    fecha_foto = Column(Date, primary_key=True, comment="Snapshot date")
    archivo = Column(String(100), primary_key=True, comment="Campaign file identifier")
    cartera = Column(String(50), primary_key=True, comment="Portfolio type")
    servicio = Column(String(20), primary_key=True, comment="Service type")
    
    # Volume metrics
    cuentas = Column(Integer, nullable=False, default=0)
    clientes = Column(Integer, nullable=False, default=0)
    deuda_asig = Column(Float, nullable=False, default=0.0)
    deuda_act = Column(Float, nullable=False, default=0.0)
    
    # Management metrics
    cuentas_gestionadas = Column(Integer, nullable=False, default=0)
    cuentas_cd = Column(Integer, nullable=False, default=0)
    cuentas_ci = Column(Integer, nullable=False, default=0)
    cuentas_sc = Column(Integer, nullable=False, default=0)
    cuentas_sg = Column(Integer, nullable=False, default=0)
    cuentas_pdp = Column(Integer, nullable=False, default=0)
    
    # Recovery metrics
    recupero = Column(Float, nullable=False, default=0.0)
    
    # KPI percentages
    pct_cober = Column(Float, nullable=False, default=0.0)
    pct_contac = Column(Float, nullable=False, default=0.0)
    pct_cd = Column(Float, nullable=False, default=0.0)
    pct_ci = Column(Float, nullable=False, default=0.0)
    pct_conversion = Column(Float, nullable=False, default=0.0)
    pct_efectividad = Column(Float, nullable=False, default=0.0)
    pct_cierre = Column(Float, nullable=False, default=0.0)
    inten = Column(Float, nullable=False, default=0.0)
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    def to_frontend_row(self, row_id: str, name: str, status: str = 'ok') -> Dict[str, Any]:
        """Convert to TypeScript DataRow interface"""
        return {
            'id': row_id,
            'name': name,
            'status': status,
            'cuentas': self.cuentas,
            'porcentajeCuentas': 0,  # Calculated at service level
            'deudaAsig': self.deuda_asig,
            'porcentajeDeuda': 0,  # Calculated at service level
            'porcentajeDeudaStatus': 'neutral',
            'deudaAct': self.deuda_act,
            'porcentajeDeudaAct': 0,  # Calculated at service level
            'porcentajeDeudaActStatus': 'neutral',
            'cobertura': round(self.pct_cober, 2),
            'contacto': round(self.pct_contac, 2),
            'contactoStatus': 'ok' if self.pct_contac >= 70 else 'warning',
            'cd': round(self.pct_cd, 2),
            'ci': round(self.pct_ci, 2),
            'sc': round(100 - self.pct_contac, 2),
            'cierre': round(self.pct_cierre, 2),
            'cierreStatus': 'ok' if self.pct_cierre >= 15 else 'warning',
            'inten': round(self.inten, 2),
            'intenStatus': 'ok' if self.inten >= 2.0 else 'warning',
            'cdCount': self.cuentas_cd,
            'ciCount': self.cuentas_ci,
            'scCount': self.cuentas_sc,
            'sgCount': self.cuentas_sg,
            'pdpCount': self.cuentas_pdp,
            'fracCount': 0,  # Not available in current schema
            'pdpFracCount': 0,  # Not available in current schema
        }
    
    def to_integral_chart_point(self) -> Dict[str, Any]:
        """Convert to TypeScript IntegralChartDataPoint interface"""
        return {
            'name': f"{self.cartera}_{self.servicio}",
            'cobertura': round(self.pct_cober, 2),
            'contacto': round(self.pct_contac, 2),
            'contactoDirecto': round(self.pct_cd, 2),
            'contactoIndirecto': round(self.pct_ci, 2),
            'tasaDeCierre': round(self.pct_cierre, 2),
            'intensidad': round(self.inten, 2)
        }


class EvolutionDataModel(Base, TimestampMixin):
    """
    SQLAlchemy model for evolution_data table
    Maps to TypeScript: EvolutionDataPoint, EvolutionSeries
    """
    __tablename__ = 'evolution_data'
    
    # Primary key (composite)
    fecha_foto = Column(Date, primary_key=True)
    archivo = Column(String(100), primary_key=True)
    
    # Dimension info
    cartera = Column(String(50), nullable=False)
    servicio = Column(String(20), nullable=False)
    
    # Evolution metrics
    pct_cober = Column(Float, nullable=False, default=0.0)
    pct_contac = Column(Float, nullable=False, default=0.0)
    pct_efectividad = Column(Float, nullable=False, default=0.0)
    pct_cierre = Column(Float, nullable=False, default=0.0)
    recupero = Column(Float, nullable=False, default=0.0)
    cuentas = Column(Integer, nullable=False, default=0)
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    def to_evolution_point(self, day_number: int) -> Dict[str, Any]:
        """Convert to TypeScript EvolutionDataPoint interface"""
        return {
            'day': day_number,
            'value': round(self.pct_cober, 2)  # Default to coverage, specific metric chosen at service level
        }


class AssignmentDataModel(Base, TimestampMixin):
    """
    SQLAlchemy model for assignment_data table
    Maps to TypeScript: AssignmentKPI, CompositionDataPoint, DetailBreakdownRow
    """
    __tablename__ = 'assignment_data'
    
    # Primary key (composite)
    periodo = Column(String(7), primary_key=True, comment="YYYY-MM format")
    archivo = Column(String(100), primary_key=True)
    cartera = Column(String(50), primary_key=True)
    
    # Volume metrics
    clientes = Column(Integer, nullable=False, default=0)
    cuentas = Column(Integer, nullable=False, default=0)
    deuda_asig = Column(Float, nullable=False, default=0.0)
    deuda_actual = Column(Float, nullable=False, default=0.0)
    ticket_promedio = Column(Float, nullable=False, default=0.0)
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    def to_composition_point(self) -> Dict[str, Any]:
        """Convert to TypeScript CompositionDataPoint interface"""
        return {
            'name': self.cartera,
            'value': round(self.deuda_asig, 2)
        }
    
    def to_breakdown_row(self, anterior_data: Optional['AssignmentDataModel'] = None) -> Dict[str, Any]:
        """Convert to TypeScript DetailBreakdownRow interface"""
        return {
            'id': f"{self.periodo}_{self.cartera}",
            'name': self.cartera,
            'clientesActual': self.clientes,
            'clientesAnterior': anterior_data.clientes if anterior_data else 0,
            'cuentasActual': self.cuentas,
            'cuentasAnterior': anterior_data.cuentas if anterior_data else 0,
            'saldoActual': round(self.deuda_asig, 2),
            'saldoAnterior': round(anterior_data.deuda_asig, 2) if anterior_data else 0,
            'ticketPromedioActual': round(self.ticket_promedio, 2),
            'ticketPromedioAnterior': round(anterior_data.ticket_promedio, 2) if anterior_data else 0,
        }


class OperationDataModel(Base, TimestampMixin):
    """
    SQLAlchemy model for operation_data table
    Maps to TypeScript: ChannelMetric, HourlyPerformance, OperationDayKPI
    """
    __tablename__ = 'operation_data'
    
    # Primary key (composite)
    fecha_foto = Column(Date, primary_key=True)
    hora = Column(Integer, primary_key=True)
    canal = Column(String(20), primary_key=True)
    archivo = Column(String(100), primary_key=True, default='GENERAL')
    
    # Operation metrics
    total_gestiones = Column(Integer, nullable=False, default=0)
    contactos_efectivos = Column(Integer, nullable=False, default=0)
    contactos_no_efectivos = Column(Integer, nullable=False, default=0)
    total_pdp = Column(Integer, nullable=False, default=0)
    tasa_contacto = Column(Float, nullable=False, default=0.0)
    tasa_conversion = Column(Float, nullable=False, default=0.0)
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    def to_channel_metric(self) -> Dict[str, Any]:
        """Convert to TypeScript ChannelMetric interface"""
        # Map database channel names to frontend expected names
        channel_mapping = {
            'BOT': 'Voicebot',
            'HUMANO': 'Call Center'
        }
        
        return {
            'channel': channel_mapping.get(self.canal, self.canal),
            'calls': self.total_gestiones,
            'effectiveContacts': self.contactos_efectivos,
            'nonEffectiveContacts': self.contactos_no_efectivos,
            'pdp': self.total_pdp,
            'cierreRate': round(self.tasa_conversion, 2)
        }
    
    def to_hourly_performance(self) -> Dict[str, Any]:
        """Convert to TypeScript HourlyPerformance interface"""
        return {
            'hour': f"{self.hora:02d}:00",
            'effectiveContacts': self.contactos_efectivos,
            'nonEffectiveContacts': self.contactos_no_efectivos,
            'pdp': self.total_pdp
        }


class ProductivityDataModel(Base, TimestampMixin):
    """
    SQLAlchemy model for productivity_data table
    Maps to TypeScript: AgentRankingRow, AgentHeatmapRow, ProductivityTrendPoint
    """
    __tablename__ = 'productivity_data'
    
    # Primary key (composite)
    fecha_foto = Column(Date, primary_key=True)
    correo_agente = Column(String(100), primary_key=True)
    archivo = Column(String(100), primary_key=True, default='GENERAL')
    
    # Performance metrics
    total_gestiones = Column(Integer, nullable=False, default=0)
    contactos_efectivos = Column(Integer, nullable=False, default=0)
    total_pdp = Column(Integer, nullable=False, default=0)
    peso_total = Column(Float, nullable=False, default=0.0)
    tasa_contacto = Column(Float, nullable=False, default=0.0)
    tasa_conversion = Column(Float, nullable=False, default=0.0)
    score_productividad = Column(Float, nullable=False, default=0.0)
    
    # Agent info (denormalized)
    nombre_agente = Column(String(100))
    dni_agente = Column(String(20))
    equipo = Column(String(50))
    
    # Metadata
    fecha_procesamiento = Column(DateTime(timezone=True), server_default=func.now())
    
    def to_ranking_row(self, rank: int, quartile: int, amount_recovered: float = 0.0) -> Dict[str, Any]:
        """Convert to TypeScript AgentRankingRow interface"""
        return {
            'id': f"{self.correo_agente}_{self.fecha_foto}",
            'rank': rank,
            'agentName': self.nombre_agente or self.correo_agente,
            'calls': self.total_gestiones,
            'directContacts': self.contactos_efectivos,
            'commitments': self.total_pdp,
            'amountRecovered': round(amount_recovered, 2),
            'closingRate': round(self.tasa_contacto, 2),
            'commitmentConversion': round(self.tasa_conversion, 2),
            'quartile': quartile
        }
    
    def to_heatmap_performance(self, day_number: int) -> Dict[str, Any]:
        """Convert to TypeScript AgentDailyPerformance interface"""
        return {
            'gestiones': self.total_gestiones if self.total_gestiones > 0 else None,
            'contactosEfectivos': self.contactos_efectivos if self.contactos_efectivos > 0 else None,
            'compromisos': self.total_pdp if self.total_pdp > 0 else None
        }
    
    def to_trend_point(self, day_number: int) -> Dict[str, Any]:
        """Convert to TypeScript ProductivityTrendPoint interface"""
        return {
            'day': day_number,
            'llamadas': self.total_gestiones,
            'compromisos': self.total_pdp,
            'recupero': round(self.peso_total, 2)  # Using peso_total as recovery proxy
        }


# =============================================================================
# ETL CONTROL MODELS
# =============================================================================

class ETLWatermarkModel(Base, TimestampMixin):
    """
    SQLAlchemy model for ETL watermarks table
    """
    __tablename__ = 'etl_watermarks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(100), nullable=False, unique=True)
    last_extracted_at = Column(DateTime(timezone=True), nullable=False)
    last_extraction_status = Column(String(20), nullable=False, default='success')
    records_extracted = Column(Integer, default=0)
    extraction_duration_seconds = Column(Float, default=0.0)
    error_message = Column(Text)
    extraction_id = Column(String(50))
    metadata = Column(JSONB)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'table_name': self.table_name,
            'last_extracted_at': self.last_extracted_at.isoformat(),
            'status': self.last_extraction_status,
            'records_extracted': self.records_extracted,
            'duration_seconds': self.extraction_duration_seconds,
            'extraction_id': self.extraction_id,
            'error_message': self.error_message
        }


class ETLExecutionLogModel(Base, TimestampMixin):
    """
    SQLAlchemy model for ETL execution log table
    """
    __tablename__ = 'etl_execution_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(50), nullable=False)
    table_name = Column(String(100), nullable=False)
    execution_type = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False)
    
    # Execution metrics
    records_processed = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    
    # Timing
    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)
    
    # Error handling
    error_message = Column(Text)
    stack_trace = Column(Text)
    
    # Metadata
    extraction_mode = Column(String(20))
    source_query = Column(Text)
    metadata = Column(JSONB)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'execution_id': self.execution_id,
            'table_name': self.table_name,
            'execution_type': self.execution_type,
            'status': self.status,
            'records_processed': self.records_processed,
            'records_inserted': self.records_inserted,
            'records_updated': self.records_updated,
            'started_at': self.started_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message
        }


# =============================================================================
# MODEL REGISTRY FOR ETL OPERATIONS
# =============================================================================

# Map table names to their corresponding SQLAlchemy models
TABLE_MODEL_MAPPING = {
    'dashboard_data': DashboardDataModel,
    'evolution_data': EvolutionDataModel,
    'assignment_data': AssignmentDataModel,
    'operation_data': OperationDataModel,
    'productivity_data': ProductivityDataModel,
    'etl_watermarks': ETLWatermarkModel,
    'etl_execution_log': ETLExecutionLogModel
}

# Export commonly used models
__all__ = [
    'Base',
    'DashboardDataModel',
    'EvolutionDataModel', 
    'AssignmentDataModel',
    'OperationDataModel',
    'ProductivityDataModel',
    'ETLWatermarkModel',
    'ETLExecutionLogModel',
    'TABLE_MODEL_MAPPING'
]
