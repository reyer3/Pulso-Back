"""
📈 Evolution Analysis Data Models
Pydantic models matching Frontend EvolutionPage types EXACTLY
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from app.models.base import BaseResponse
from app.models.common import FrontendCompatibleModel, ValueType


# =============================================================================
# EVOLUTION DATA MODELS - EXACT FRONTEND MATCH
# =============================================================================

class MetricConfigItem(FrontendCompatibleModel):
    """
    Metric configuration item - EXACT match with Frontend MetricConfigItem interface
    """
    id: str = Field(description="Metric unique identifier")
    name: str = Field(description="Metric display name")
    active: bool = Field(description="Whether metric is active/visible")


class EvolutionDataPoint(FrontendCompatibleModel):
    """
    Single evolution data point - EXACT match with Frontend EvolutionDataPoint interface
    """
    day: int = Field(description="Day number in evolution")
    value: float = Field(description="Metric value for this day")


class EvolutionSeries(FrontendCompatibleModel):
    """
    Evolution data series - EXACT match with Frontend EvolutionSeries interface
    """
    name: str = Field(description="Series name (e.g., cartera name)")
    data: List[EvolutionDataPoint] = Field(description="Data points for this series")


class EvolutionMetric(FrontendCompatibleModel):
    """
    Evolution metric with series - EXACT match with Frontend EvolutionMetric interface
    """
    metric: str = Field(description="Metric name (e.g., 'cobertura', 'contacto')")
    valueType: ValueType = Field(description="Value type (percent, currency, number)")
    series: List[EvolutionSeries] = Field(description="Data series for this metric")


# =============================================================================
# EVOLUTION RESPONSE MODELS - EXACT FRONTEND MATCH
# =============================================================================

# EvolutionData type alias - matches Frontend exactly
EvolutionData = List[EvolutionMetric]

class EvolutionResponse(BaseResponse):
    """
    Evolution analysis API response
    """
    data: EvolutionData = Field(description="Evolution metrics data")
    dateRange: dict = Field(description="Date range analyzed")
    comparisonDimension: str = Field(description="Comparison dimension used")
    queryTime: float = Field(description="Query execution time")


# =============================================================================
# EVOLUTION REQUEST MODELS
# =============================================================================

class EvolutionRequest(BaseModel):
    """
    Request model for evolution analysis
    """
    filters: dict = Field(default_factory=dict, description="Filter criteria")
    fechaInicio: str = Field(description="Start date (YYYY-MM-DD)")
    fechaFin: str = Field(description="End date (YYYY-MM-DD)")
    comparisonDimension: str = Field(default="cartera", description="Dimension for comparison")
    includeMetrics: List[str] = Field(
        default=["cobertura", "contacto", "cierre", "intensidad"],
        description="Metrics to include"
    )


class EvolutionFilters(BaseModel):
    """
    Evolution analysis filters
    """
    cartera: Optional[List[str]] = None
    servicio: Optional[List[str]] = None
    tramo: Optional[List[str]] = None
    includeWeekends: bool = Field(default=False, description="Include weekends in analysis")
    smoothingDays: int = Field(default=1, description="Days for smoothing (1 = no smoothing)")


# =============================================================================
# EVOLUTION SUMMARY MODELS
# =============================================================================

class EvolutionSummary(FrontendCompatibleModel):
    """
    Evolution summary statistics
    """
    totalDays: int = Field(description="Total days analyzed")
    totalSeries: int = Field(description="Total series analyzed")
    bestPerformingCartera: str = Field(description="Best performing portfolio")
    worstPerformingCartera: str = Field(description="Worst performing portfolio")
    averageImprovement: float = Field(description="Average improvement percentage")
    trendDirection: str = Field(description="Overall trend (improving/declining/stable)")
    peakDay: int = Field(description="Day with best overall performance")
    lowestDay: int = Field(description="Day with worst overall performance")


class MetricEvolutionDetail(FrontendCompatibleModel):
    """
    Detailed evolution for a specific metric
    """
    metricName: str = Field(description="Metric name")
    metricType: ValueType = Field(description="Metric value type")
    startValue: float = Field(description="Value at start of period")
    endValue: float = Field(description="Value at end of period")
    peakValue: float = Field(description="Peak value in period")
    lowestValue: float = Field(description="Lowest value in period")
    averageValue: float = Field(description="Average value in period")
    totalChange: float = Field(description="Total change (end - start)")
    percentageChange: float = Field(description="Percentage change")
    volatility: float = Field(description="Metric volatility (std dev)")
    trendLine: List[float] = Field(description="Trend line values")
    inflectionPoints: List[int] = Field(description="Days with significant changes")


class CarteraEvolutionComparison(FrontendCompatibleModel):
    """
    Evolution comparison between carteras
    """
    carteraName: str = Field(description="Portfolio name")
    overallRank: int = Field(description="Overall performance rank")
    metricScores: dict = Field(description="Scores by metric")
    consistencyScore: float = Field(description="Performance consistency score")
    improvementRate: float = Field(description="Rate of improvement")
    relativePerformance: str = Field(description="Relative performance (above/below average)")
    strongestMetric: str = Field(description="Best performing metric")
    weakestMetric: str = Field(description="Worst performing metric")


class DailyEvolutionSnapshot(FrontendCompatibleModel):
    """
    Daily snapshot of all metrics
    """
    day: int = Field(description="Day number")
    date: str = Field(description="Actual date (YYYY-MM-DD)")
    overallScore: float = Field(description="Combined performance score")
    metricValues: dict = Field(description="Values by metric")
    carteraRankings: List[str] = Field(description="Carteras ranked by performance")
    significantChanges: List[str] = Field(description="Significant changes observed")
    dayType: str = Field(description="Day type (weekday/weekend/holiday)")
