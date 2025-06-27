"""
📈 Evolution data models
Pydantic models for evolution/trending API responses
"""

from datetime import date, datetime
from typing import List, Optional, Union

from pydantic import BaseModel, Field, validator

from app.models.base import BaseResponse, CacheInfo, Amount, Count, Percentage


# Request models
class EvolutionFilters(BaseModel):
    """
    Evolution filter parameters
    """
    cartera: Optional[List[str]] = Field(default=None)
    servicio: Optional[List[str]] = Field(default=None)
    tramo: Optional[List[str]] = Field(default=None)
    archivo: Optional[List[str]] = Field(default=None)
    days_back: int = Field(default=30, ge=1, le=90, description="Days to look back")
    comparison_dimension: str = Field(default="cartera", description="Grouping dimension")


# Data models
class EvolutionDataPoint(BaseModel):
    """
    Single data point in evolution series
    """
    dia_gestion: int = Field(ge=0, description="Management day (0-based)")
    fecha_foto: date = Field(description="Snapshot date")
    value: float = Field(description="Metric value")
    
    # Additional context
    cuentas: Optional[Count] = Field(description="Accounts count on this day")
    deuda_actual: Optional[Amount] = Field(description="Current debt on this day")


class EvolutionSeries(BaseModel):
    """
    Evolution data series for a specific dimension
    """
    name: str = Field(description="Series name (e.g., TEMPRANA, MOVIL)")
    data: List[EvolutionDataPoint] = Field(description="Data points over time")
    
    # Series metadata
    total_accounts: Count = Field(description="Total accounts in series")
    start_date: date = Field(description="Series start date")
    end_date: Optional[date] = Field(description="Series end date")
    status: str = Field(description="Series status (ACTIVE, CLOSED, etc.)")
    
    @validator('data')
    def data_must_be_sorted(cls, v):
        """Ensure data points are sorted by management day"""
        if len(v) > 1:
            sorted_data = sorted(v, key=lambda x: x.dia_gestion)
            if v != sorted_data:
                return sorted_data
        return v


class EvolutionMetric(BaseModel):
    """
    Single metric evolution data
    """
    metric: str = Field(description="Metric name (cobertura, contactabilidad, etc.)")
    metric_display_name: str = Field(description="Human-readable metric name")
    value_type: str = Field(description="Value type: percent, currency, number")
    series: List[EvolutionSeries] = Field(description="Data series for this metric")
    
    # Metric metadata
    current_average: float = Field(description="Current period average")
    previous_average: Optional[float] = Field(description="Previous period average")
    trend: str = Field(description="UP, DOWN, STABLE")
    
    @validator('value_type')
    def validate_value_type(cls, v):
        allowed_types = ['percent', 'currency', 'number', 'ratio']
        if v not in allowed_types:
            raise ValueError(f"value_type must be one of {allowed_types}")
        return v


# Comparative analysis models
class ComparisonPoint(BaseModel):
    """
    Comparison between two series at a specific day
    """
    dia_gestion: int
    series_a_value: float
    series_b_value: float
    difference: float
    percentage_difference: float


class SeriesComparison(BaseModel):
    """
    Comparison between two evolution series
    """
    series_a_name: str
    series_b_name: str
    metric: str
    comparison_points: List[ComparisonPoint]
    overall_winner: str  # series_a or series_b
    max_difference: float
    avg_difference: float


# Response models
class EvolutionData(BaseModel):
    """
    Complete evolution data response
    """
    metrics: List[EvolutionMetric] = Field(description="Evolution metrics data")
    
    # Analysis metadata
    comparison_dimension: str = Field(description="Grouping dimension used")
    total_series: int = Field(description="Number of series")
    date_range: dict = Field(description="Date range covered")
    
    # Summary statistics
    best_performing_series: Optional[str] = Field(description="Best performing series")
    worst_performing_series: Optional[str] = Field(description="Worst performing series")
    convergence_analysis: Optional[dict] = Field(description="Series convergence analysis")


class EvolutionResponse(BaseResponse):
    """
    Evolution API response
    """
    data: EvolutionData = Field(description="Evolution data")
    cache_info: Optional[CacheInfo] = Field(description="Cache information")
    query_time: Optional[float] = Field(description="Query execution time")


# Trend analysis models
class TrendAnalysis(BaseModel):
    """
    Trend analysis for a metric series
    """
    series_name: str
    metric: str
    trend_direction: str  # INCREASING, DECREASING, STABLE, VOLATILE
    slope: float  # Trend slope
    r_squared: float  # Correlation coefficient
    volatility: float  # Measure of volatility
    seasonal_pattern: bool  # Whether seasonal pattern detected
    
    # Key insights
    best_day: int  # Day with best performance
    worst_day: int  # Day with worst performance
    average_improvement: float  # Average daily improvement
    total_change: float  # Total change from start to end


class BenchmarkComparison(BaseModel):
    """
    Benchmark comparison analysis
    """
    series_name: str
    metric: str
    benchmark_value: float  # Industry/target benchmark
    current_value: float
    gap_to_benchmark: float
    days_to_reach_benchmark: Optional[int]  # Estimated days to reach benchmark
    probability_of_success: float  # Probability of reaching benchmark


# Performance insights
class PerformanceInsight(BaseModel):
    """
    Automated performance insight
    """
    type: str  # ALERT, OPPORTUNITY, ACHIEVEMENT, WARNING
    priority: str  # HIGH, MEDIUM, LOW
    title: str
    description: str
    affected_series: List[str]
    metric: str
    recommendation: Optional[str] = None
    urgency_days: Optional[int] = None  # Days until action needed


class EvolutionInsights(BaseModel):
    """
    Evolution insights and recommendations
    """
    insights: List[PerformanceInsight]
    trend_analyses: List[TrendAnalysis]
    benchmark_comparisons: List[BenchmarkComparison]
    series_comparisons: List[SeriesComparison]
    
    # Summary
    overall_performance: str  # EXCELLENT, GOOD, FAIR, POOR
    key_recommendations: List[str]
    attention_required: List[str]  # Series requiring immediate attention