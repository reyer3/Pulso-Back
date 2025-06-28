"""
📋 Assignment Analysis Data Models
Pydantic models matching Frontend AssignmentAnalysisPage types EXACTLY
"""

from typing import List
from pydantic import BaseModel, Field

from app.models.base import BaseResponse
from app.models.common import FrontendCompatibleModel, ValueType


# =============================================================================
# ASSIGNMENT DATA MODELS - EXACT FRONTEND MATCH
# =============================================================================

class AssignmentKPI(FrontendCompatibleModel):
    """
    Assignment KPI with comparison - EXACT match with Frontend AssignmentKPI interface
    """
    label: str = Field(description="KPI label")
    valorActual: float = Field(description="Current period value")
    valorAnterior: float = Field(description="Previous period value")
    variacion: float = Field(description="Percentage change")
    valueType: ValueType = Field(description="Value type (currency or number)")


class CompositionDataPoint(FrontendCompatibleModel):
    """
    Composition data point - EXACT match with Frontend CompositionDataPoint interface
    """
    name: str = Field(description="Portfolio name (e.g., 'TEMPRANA')")
    value: float = Field(description="Portfolio value (e.g., debt balance)")


class DetailBreakdownRow(FrontendCompatibleModel):
    """
    Detail breakdown row - EXACT match with Frontend DetailBreakdownRow interface
    """
    id: str = Field(description="Unique identifier")
    name: str = Field(description="Portfolio name")
    clientesActual: int = Field(description="Current period clients")
    clientesAnterior: int = Field(description="Previous period clients")
    cuentasActual: int = Field(description="Current period accounts")
    cuentasAnterior: int = Field(description="Previous period accounts")
    saldoActual: float = Field(description="Current period balance")
    saldoAnterior: float = Field(description="Previous period balance")
    ticketPromedioActual: float = Field(description="Current period average ticket")
    ticketPromedioAnterior: float = Field(description="Previous period average ticket")


# =============================================================================
# ASSIGNMENT RESPONSE MODELS - EXACT FRONTEND MATCH
# =============================================================================

class AssignmentAnalysisData(FrontendCompatibleModel):
    """
    Complete assignment analysis data - EXACT match with Frontend AssignmentAnalysisData interface
    """
    kpis: List[AssignmentKPI] = Field(description="Executive KPIs with period comparison")
    compositionData: List[CompositionDataPoint] = Field(description="Portfolio composition data")
    detailBreakdown: List[DetailBreakdownRow] = Field(description="Detailed breakdown by portfolio")


class AssignmentAnalysisResponse(BaseResponse):
    """
    Assignment analysis API response
    """
    data: AssignmentAnalysisData = Field(description="Assignment analysis data")
    currentPeriod: str = Field(description="Current analysis period")
    previousPeriod: str = Field(description="Previous comparison period")
    queryTime: float = Field(description="Query execution time")


# =============================================================================
# ASSIGNMENT REQUEST MODELS
# =============================================================================

class AssignmentAnalysisRequest(BaseModel):
    """
    Request model for assignment analysis
    """
    filters: dict = Field(default_factory=dict, description="Filter criteria")
    currentPeriod: str = Field(description="Current period (YYYY-MM)")
    previousPeriod: str = Field(description="Previous period for comparison (YYYY-MM)")
    includeDetailBreakdown: bool = Field(default=True, description="Include detailed breakdown")
    includeComposition: bool = Field(default=True, description="Include composition analysis")


class AssignmentFilters(BaseModel):
    """
    Assignment analysis filters
    """
    cartera: List[str] = Field(default=[], description="Portfolios to include")
    servicio: List[str] = Field(default=[], description="Services to include")
    includeHistorical: bool = Field(default=True, description="Include historical comparison")
    groupByServicio: bool = Field(default=False, description="Group results by service")


# =============================================================================
# ASSIGNMENT SUMMARY MODELS
# =============================================================================

class AssignmentSummary(FrontendCompatibleModel):
    """
    Assignment summary statistics
    """
    totalCarteras: int = Field(description="Total portfolios analyzed")
    totalClientes: int = Field(description="Total clients in current period")
    totalCuentas: int = Field(description="Total accounts in current period")
    totalSaldo: float = Field(description="Total balance in current period")
    clientesVariacion: float = Field(description="Clients variation percentage")
    cuentasVariacion: float = Field(description="Accounts variation percentage")
    saldoVariacion: float = Field(description="Balance variation percentage")
    ticketPromedioGlobal: float = Field(description="Global average ticket")
    carteraConMayorCrecimiento: str = Field(description="Portfolio with highest growth")
    carteraConMenorCrecimiento: str = Field(description="Portfolio with lowest growth")


class PortfolioComparison(FrontendCompatibleModel):
    """
    Portfolio comparison between periods
    """
    carteraNombre: str = Field(description="Portfolio name")
    participacionActual: float = Field(description="Current period participation %")
    participacionAnterior: float = Field(description="Previous period participation %")
    crecimientoClientes: float = Field(description="Clients growth %")
    crecimientoCuentas: float = Field(description="Accounts growth %")
    crecimientoSaldo: float = Field(description="Balance growth %")
    ticketEvolucion: float = Field(description="Average ticket evolution %")
    rank: int = Field(description="Rank by total balance")
    trend: str = Field(description="Overall trend (growing/declining/stable)")


class PeriodComparison(FrontendCompatibleModel):
    """
    Period-over-period comparison
    """
    currentPeriod: str = Field(description="Current period")
    previousPeriod: str = Field(description="Previous period")
    periodType: str = Field(description="Comparison type (monthly/quarterly)")
    totalGrowth: float = Field(description="Total portfolio growth %")
    volumeChanges: dict = Field(description="Volume changes by metric")
    qualityChanges: dict = Field(description="Quality changes by metric")
    significantChanges: List[str] = Field(description="Significant changes observed")
    recommendations: List[str] = Field(description="Analysis recommendations")


class CompositionAnalysis(FrontendCompatibleModel):
    """
    Portfolio composition analysis
    """
    diversificationIndex: float = Field(description="Portfolio diversification index")
    concentrationRisk: float = Field(description="Concentration risk score")
    largestPortfolios: List[str] = Field(description="Top 3 largest portfolios")
    emergingPortfolios: List[str] = Field(description="Fastest growing portfolios")
    decliningPortfolios: List[str] = Field(description="Declining portfolios")
    balanceDistribution: dict = Field(description="Balance distribution by range")
    serviceMix: dict = Field(description="Service mix analysis")
    seasonalityFactors: dict = Field(description="Seasonality impact factors")
