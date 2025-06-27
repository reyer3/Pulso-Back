"""
📋 Assignment analysis data models
Pydantic models for assignment composition API responses
"""

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field, validator

from app.models.base import BaseResponse, CacheInfo, Amount, Count, Percentage


# Request models
class AssignmentFilters(BaseModel):
    """
    Assignment analysis filter parameters
    """
    periodo_actual: Optional[str] = Field(default=None, description="Current period (YYYY-MM)")
    periodo_anterior: Optional[str] = Field(default=None, description="Previous period (YYYY-MM)")
    cartera: Optional[List[str]] = Field(default=None)
    servicio: Optional[List[str]] = Field(default=None)
    include_closed: bool = Field(default=True, description="Include closed portfolios")


# Data models
class AssignmentKPI(BaseModel):
    """
    Assignment KPI with comparison
    """
    label: str = Field(description="KPI label")
    valor_actual: float = Field(description="Current period value")
    valor_anterior: float = Field(description="Previous period value")
    variacion: float = Field(description="Absolute change")
    variacion_porcentual: float = Field(description="Percentage change")
    value_type: str = Field(description="Value type: currency, number, percent")
    trend: str = Field(description="UP, DOWN, STABLE")
    
    @validator('trend', pre=True, always=True)
    def determine_trend(cls, v, values):
        if v:
            return v
        
        variacion = values.get('variacion_porcentual', 0)
        if abs(variacion) < 2:  # Less than 2% change
            return "STABLE"
        elif variacion > 0:
            return "UP"
        else:
            return "DOWN"


class CompositionDataPoint(BaseModel):
    """
    Data point for composition breakdown
    """
    name: str = Field(description="Category name")
    valor_actual: Amount = Field(description="Current value")
    valor_anterior: Amount = Field(description="Previous value")
    participacion_actual: Percentage = Field(description="Current participation %")
    participacion_anterior: Percentage = Field(description="Previous participation %")
    variacion: float = Field(description="Absolute change")
    variacion_porcentual: float = Field(description="Percentage change")


class DetailBreakdownRow(BaseModel):
    """
    Detailed breakdown row by portfolio
    """
    id: str
    archivo: str = Field(description="Portfolio file name")
    cartera: str = Field(description="Portfolio type")
    servicio: str = Field(description="Service type")
    estado: str = Field(description="Portfolio status")
    
    # Current period metrics
    clientes_actual: Count = Field(description="Current clients")
    cuentas_actual: Count = Field(description="Current accounts")
    saldo_actual: Amount = Field(description="Current debt balance")
    ticket_promedio_actual: Amount = Field(description="Current average ticket")
    
    # Previous period metrics
    clientes_anterior: Count = Field(description="Previous clients")
    cuentas_anterior: Count = Field(description="Previous accounts")
    saldo_anterior: Amount = Field(description="Previous debt balance")
    ticket_promedio_anterior: Amount = Field(description="Previous average ticket")
    
    # Date information
    fecha_asignacion_actual: Optional[date] = Field(description="Current assignment date")
    fecha_asignacion_anterior: Optional[date] = Field(description="Previous assignment date")
    fecha_cierre_actual: Optional[date] = Field(description="Current closure date")
    fecha_cierre_anterior: Optional[date] = Field(description="Previous closure date")
    
    # Calculated fields
    @property
    def variacion_clientes(self) -> float:
        if self.clientes_anterior == 0:
            return 100.0 if self.clientes_actual > 0 else 0.0
        return ((self.clientes_actual - self.clientes_anterior) / self.clientes_anterior) * 100
    
    @property
    def variacion_cuentas(self) -> float:
        if self.cuentas_anterior == 0:
            return 100.0 if self.cuentas_actual > 0 else 0.0
        return ((self.cuentas_actual - self.cuentas_anterior) / self.cuentas_anterior) * 100
    
    @property
    def variacion_saldo(self) -> float:
        if self.saldo_anterior == 0:
            return 100.0 if self.saldo_actual > 0 else 0.0
        return ((self.saldo_actual - self.saldo_anterior) / self.saldo_anterior) * 100


class PortfolioComparison(BaseModel):
    """
    Portfolio comparison between periods
    """
    cartera: str
    
    # Volume comparison
    portfolios_actual: int
    portfolios_anterior: int
    portfolios_nuevos: int
    portfolios_descontinuados: int
    
    # Performance comparison
    efectividad_promedio_actual: Percentage
    efectividad_promedio_anterior: Percentage
    cobertura_promedio_actual: Percentage
    cobertura_promedio_anterior: Percentage
    
    # Size analysis
    tamano_promedio_actual: Count  # Average portfolio size
    tamano_promedio_anterior: Count
    concentracion_actual: float  # Concentration index
    concentracion_anterior: float


# Trend analysis models
class VolumeAnalysis(BaseModel):
    """
    Volume analysis across periods
    """
    total_clientes_actual: Count
    total_clientes_anterior: Count
    total_cuentas_actual: Count
    total_cuentas_anterior: Count
    total_saldo_actual: Amount
    total_saldo_anterior: Amount
    
    # Growth rates
    crecimiento_clientes: float
    crecimiento_cuentas: float
    crecimiento_saldo: float
    
    # Distribution metrics
    clientes_por_cuenta_actual: float
    clientes_por_cuenta_anterior: float
    ticket_promedio_global_actual: Amount
    ticket_promedio_global_anterior: Amount


class SegmentAnalysis(BaseModel):
    """
    Segment distribution analysis
    """
    segmento: str
    
    # Current period
    participacion_clientes_actual: Percentage
    participacion_cuentas_actual: Percentage
    participacion_saldo_actual: Percentage
    
    # Previous period
    participacion_clientes_anterior: Percentage
    participacion_cuentas_anterior: Percentage
    participacion_saldo_anterior: Percentage
    
    # Changes
    cambio_participacion_clientes: float
    cambio_participacion_cuentas: float
    cambio_participacion_saldo: float
    
    # Performance
    ticket_promedio_actual: Amount
    ticket_promedio_anterior: Amount
    densidad_actual: float  # Accounts per client
    densidad_anterior: float


# Response models
class AssignmentAnalysisData(BaseModel):
    """
    Complete assignment analysis data
    """
    # High-level KPIs
    kpis: List[AssignmentKPI] = Field(description="Key performance indicators")
    
    # Composition breakdown
    composition_por_cartera: List[CompositionDataPoint] = Field(description="By portfolio type")
    composition_por_servicio: List[CompositionDataPoint] = Field(description="By service type")
    composition_por_vencimiento: List[CompositionDataPoint] = Field(description="By due date")
    
    # Detailed breakdown
    detail_breakdown: List[DetailBreakdownRow] = Field(description="Detailed portfolio breakdown")
    
    # Analysis components
    volume_analysis: VolumeAnalysis = Field(description="Volume trend analysis")
    segment_analysis: List[SegmentAnalysis] = Field(description="Segment distribution analysis")
    portfolio_comparison: List[PortfolioComparison] = Field(description="Portfolio comparison")
    
    # Metadata
    periodo_actual: str = Field(description="Current period")
    periodo_anterior: str = Field(description="Previous period")
    total_portfolios_actual: int = Field(description="Current period portfolios")
    total_portfolios_anterior: int = Field(description="Previous period portfolios")
    fecha_corte: date = Field(description="Analysis cutoff date")


class AssignmentResponse(BaseResponse):
    """
    Assignment analysis API response
    """
    data: AssignmentAnalysisData = Field(description="Assignment analysis data")
    cache_info: Optional[CacheInfo] = Field(description="Cache information")
    query_time: Optional[float] = Field(description="Query execution time")


# Summary and insights
class AssignmentInsight(BaseModel):
    """
    Assignment analysis insight
    """
    type: str  # GROWTH, DECLINE, SHIFT, CONCENTRATION, DIVERSIFICATION
    priority: str  # HIGH, MEDIUM, LOW
    title: str
    description: str
    affected_segments: List[str]
    impact_level: str  # MAJOR, MODERATE, MINOR
    recommendation: Optional[str] = None
    
    # Supporting data
    current_value: Optional[float] = None
    previous_value: Optional[float] = None
    change_magnitude: Optional[float] = None


class AssignmentSummary(BaseModel):
    """
    Assignment analysis executive summary
    """
    # Overall trends
    overall_growth_trend: str  # GROWING, DECLINING, STABLE
    dominant_portfolio_type: str
    fastest_growing_segment: str
    largest_decline_segment: Optional[str] = None
    
    # Key metrics
    total_volume_change: float
    average_portfolio_size_change: float
    concentration_change: float
    
    # Insights
    key_insights: List[AssignmentInsight]
    risk_areas: List[str]
    opportunities: List[str]
    
    # Recommendations
    strategic_recommendations: List[str]
    operational_recommendations: List[str]


# Historical tracking
class HistoricalAssignment(BaseModel):
    """
    Historical assignment tracking
    """
    periodo: str
    total_clientes: Count
    total_cuentas: Count
    total_saldo: Amount
    portfolios_activos: int
    ticket_promedio: Amount
    concentracion_index: float
    
    # Month-over-month changes
    mom_clientes: Optional[float] = None
    mom_cuentas: Optional[float] = None
    mom_saldo: Optional[float] = None


class AssignmentTrend(BaseModel):
    """
    Assignment trend analysis
    """
    historical_data: List[HistoricalAssignment]
    trend_direction: str  # UPWARD, DOWNWARD, STABLE, VOLATILE
    seasonality_detected: bool
    growth_rate_annual: float
    volatility_index: float
    
    # Projections
    next_period_projection: Optional[HistoricalAssignment] = None
    confidence_level: Optional[float] = None