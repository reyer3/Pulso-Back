"""
📞 Operation Analysis Data Models
Pydantic models matching Frontend OperationPage types EXACTLY
"""

from typing import List
from pydantic import BaseModel, Field

from app.models.base import BaseResponse
from app.models.common import FrontendCompatibleModel, ChannelType


# =============================================================================
# OPERATION DATA MODELS - EXACT FRONTEND MATCH
# =============================================================================

class OperationDayKPI(FrontendCompatibleModel):
    """
    Daily operation KPI - EXACT match with Frontend OperationDayKPI interface
    """
    label: str = Field(description="KPI label")
    value: str = Field(description="KPI value as string")


class ChannelMetric(FrontendCompatibleModel):
    """
    Channel performance metrics - EXACT match with Frontend ChannelMetric interface
    """
    channel: ChannelType = Field(description="Channel type (Voicebot | Call Center)")
    calls: int = Field(description="Total calls")
    effectiveContacts: int = Field(description="Effective contacts")
    nonEffectiveContacts: int = Field(description="Non-effective contacts")
    pdp: int = Field(description="Promises of payment")
    cierreRate: float = Field(description="Closure rate percentage")


class HourlyPerformance(FrontendCompatibleModel):
    """
    Hourly performance data - EXACT match with Frontend HourlyPerformance interface
    """
    hour: str = Field(description="Hour in format HH:MM (e.g., '09:00')")
    effectiveContacts: int = Field(description="Effective contacts in this hour")
    nonEffectiveContacts: int = Field(description="Non-effective contacts in this hour")
    pdp: int = Field(description="Promises of payment in this hour")


class AttemptEffectiveness(FrontendCompatibleModel):
    """
    Attempt effectiveness data - EXACT match with Frontend AttemptEffectiveness interface
    """
    attempt: int = Field(description="Attempt number")
    cierreRate: float = Field(description="Closure rate percentage for this attempt")


class QueuePerformance(FrontendCompatibleModel):
    """
    Queue performance data - EXACT match with Frontend QueuePerformance interface
    """
    queueName: str = Field(description="Queue name")
    calls: int = Field(description="Total calls")
    effectiveContacts: int = Field(description="Effective contacts")
    pdp: int = Field(description="Promises of payment")
    cierreRate: float = Field(description="Closure rate percentage")


# =============================================================================
# OPERATION RESPONSE MODELS - EXACT FRONTEND MATCH
# =============================================================================

class OperationDayAnalysisData(FrontendCompatibleModel):
    """
    Complete operation day analysis - EXACT match with Frontend OperationDayAnalysisData interface
    """
    kpis: List[OperationDayKPI] = Field(description="Daily KPIs")
    channelPerformance: List[ChannelMetric] = Field(description="Channel performance metrics")
    hourlyPerformance: List[HourlyPerformance] = Field(description="Hourly breakdown")
    attemptEffectiveness: List[AttemptEffectiveness] = Field(description="Attempt effectiveness")
    queuePerformance: List[QueuePerformance] = Field(description="Queue performance")


class OperationDayAnalysisResponse(BaseResponse):
    """
    Operation analysis API response
    """
    data: OperationDayAnalysisData = Field(description="Operation analysis data")
    date: str = Field(description="Analysis date")
    queryTime: float = Field(description="Query execution time")


# =============================================================================
# OPERATION REQUEST MODELS
# =============================================================================

class OperationDayRequest(BaseModel):
    """
    Request model for operation day analysis
    """
    date: str = Field(description="Analysis date (YYYY-MM-DD)")
    includeHourlyBreakdown: bool = Field(default=True, description="Include hourly performance")
    includeQueueDetails: bool = Field(default=True, description="Include queue performance")
    includeAttemptAnalysis: bool = Field(default=True, description="Include attempt effectiveness")


class OperationFilters(BaseModel):
    """
    Operation analysis filters
    """
    channels: List[str] = Field(default=["Voicebot", "Call Center"], description="Channels to include")
    queues: List[str] = Field(default=[], description="Specific queues to analyze")
    hourRange: List[str] = Field(default=["08:00", "20:00"], description="Hour range [start, end]")
    maxAttempts: int = Field(default=5, description="Maximum attempts to analyze")


# =============================================================================
# OPERATION SUMMARY MODELS
# =============================================================================

class OperationSummary(FrontendCompatibleModel):
    """
    Operation summary statistics
    """
    totalCalls: int = Field(description="Total calls")
    totalEffectiveContacts: int = Field(description="Total effective contacts")
    totalPdp: int = Field(description="Total PDPs")
    overallCierreRate: float = Field(description="Overall closure rate")
    peakHour: str = Field(description="Peak hour for effective contacts")
    bestChannel: str = Field(description="Best performing channel")
    averageAttempts: float = Field(description="Average attempts per closure")


class ChannelComparison(FrontendCompatibleModel):
    """
    Channel comparison metrics
    """
    voicebotMetrics: ChannelMetric = Field(description="Voicebot performance")
    callCenterMetrics: ChannelMetric = Field(description="Call center performance")
    performanceDelta: dict = Field(description="Performance differences")
    recommendation: str = Field(description="Performance recommendation")
