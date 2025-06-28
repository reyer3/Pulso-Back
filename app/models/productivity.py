"""
🎯 Productivity Analysis Data Models
Pydantic models matching Frontend ProductivityPage types EXACTLY
"""

from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field

from app.models.base import BaseResponse
from app.models.common import FrontendCompatibleModel, HeatmapMetric


# =============================================================================
# PRODUCTIVITY DATA MODELS - EXACT FRONTEND MATCH  
# =============================================================================

class AgentDailyPerformance(FrontendCompatibleModel):
    """
    Agent daily performance data - EXACT match with Frontend AgentDailyPerformance interface
    """
    gestiones: Optional[int] = Field(description="Number of management actions")
    contactosEfectivos: Optional[int] = Field(description="Effective contacts")
    compromisos: Optional[int] = Field(description="Commitments/promises")


class AgentHeatmapRow(FrontendCompatibleModel):
    """
    Agent heatmap row data - EXACT match with Frontend AgentHeatmapRow interface  
    """
    id: str = Field(description="Unique agent identifier")
    dni: str = Field(description="Agent DNI")
    agentName: str = Field(description="Agent full name")
    dailyPerformance: Dict[int, Optional[AgentDailyPerformance]] = Field(
        description="Daily performance by day number"
    )


class ProductivityTrendPoint(FrontendCompatibleModel):
    """
    Productivity trend data point - EXACT match with Frontend ProductivityTrendPoint interface
    """
    day: Optional[int] = Field(description="Day number (for daily trend)")
    hour: Optional[str] = Field(description="Hour string (for hourly trend)")
    llamadas: int = Field(description="Number of calls")
    compromisos: int = Field(description="Number of commitments")
    recupero: Optional[float] = Field(description="Recovery amount (daily trend only)")


class AgentRankingRow(FrontendCompatibleModel):
    """
    Agent ranking row data - EXACT match with Frontend AgentRankingRow interface
    """
    id: str = Field(description="Unique agent identifier")
    rank: int = Field(description="Agent rank position")
    agentName: str = Field(description="Agent full name")
    calls: int = Field(description="Total calls made")
    directContacts: int = Field(description="Direct contacts achieved")
    commitments: int = Field(description="Commitments obtained")
    amountRecovered: float = Field(description="Amount recovered")
    closingRate: float = Field(description="Closure rate percentage")
    commitmentConversion: float = Field(description="Commitment conversion percentage")
    quartile: int = Field(description="Performance quartile (1-4)", ge=1, le=4)


# =============================================================================
# PRODUCTIVITY RESPONSE MODELS - EXACT FRONTEND MATCH
# =============================================================================

class ProductivityData(FrontendCompatibleModel):
    """
    Complete productivity data - EXACT match with Frontend ProductivityData interface
    """
    dailyTrend: List[ProductivityTrendPoint] = Field(description="Daily trend data")
    hourlyTrend: List[ProductivityTrendPoint] = Field(description="Hourly trend data")
    agentRanking: List[AgentRankingRow] = Field(description="Agent ranking data")
    agentHeatmap: List[AgentHeatmapRow] = Field(description="Agent heatmap data")


class ProductivityResponse(BaseResponse):
    """
    Productivity analysis API response
    """
    data: ProductivityData = Field(description="Productivity analysis data")
    dateRange: Dict[str, str] = Field(description="Analysis date range")
    queryTime: float = Field(description="Query execution time")


# =============================================================================
# PRODUCTIVITY REQUEST MODELS
# =============================================================================

class ProductivityRequest(BaseModel):
    """
    Request model for productivity analysis
    """
    dateFrom: str = Field(description="Start date (YYYY-MM-DD)")
    dateTo: str = Field(description="End date (YYYY-MM-DD)")
    includeHeatmap: bool = Field(default=True, description="Include agent heatmap")
    includeRanking: bool = Field(default=True, description="Include agent ranking")
    includeTrends: bool = Field(default=True, description="Include trend analysis")
    maxAgents: int = Field(default=50, description="Maximum agents to include")


class ProductivityFilters(BaseModel):
    """
    Productivity analysis filters
    """
    agents: List[str] = Field(default=[], description="Specific agent IDs to include")
    teams: List[str] = Field(default=[], description="Team names to include")
    metrics: List[HeatmapMetric] = Field(
        default=[HeatmapMetric.GESTIONES, HeatmapMetric.CONTACTOS_EFECTIVOS, HeatmapMetric.COMPROMISOS],
        description="Metrics to include in analysis"
    )
    quartiles: List[int] = Field(default=[1, 2, 3, 4], description="Quartiles to include")
    minCalls: int = Field(default=10, description="Minimum calls for inclusion")


# =============================================================================
# PRODUCTIVITY SUMMARY MODELS
# =============================================================================

class ProductivitySummary(FrontendCompatibleModel):
    """
    Productivity summary statistics
    """
    totalAgents: int = Field(description="Total agents analyzed")
    activeAgents: int = Field(description="Active agents in period")
    totalCalls: int = Field(description="Total calls made")
    totalCommitments: int = Field(description="Total commitments")
    totalRecovery: float = Field(description="Total amount recovered")
    averageCallsPerAgent: float = Field(description="Average calls per agent")
    averageCommitmentsPerAgent: float = Field(description="Average commitments per agent")
    topPerformerAgent: str = Field(description="Top performing agent name")
    conversionRate: float = Field(description="Overall commitment conversion rate")


class AgentPerformanceDetail(FrontendCompatibleModel):
    """
    Detailed agent performance information
    """
    agentId: str = Field(description="Agent unique identifier")
    agentName: str = Field(description="Agent full name")
    dni: str = Field(description="Agent DNI")
    team: Optional[str] = Field(description="Team name")
    totalCalls: int = Field(description="Total calls")
    effectiveContacts: int = Field(description="Effective contacts")
    commitments: int = Field(description="Commitments obtained")
    recoveryAmount: float = Field(description="Amount recovered")
    contactRate: float = Field(description="Contact rate percentage")
    conversionRate: float = Field(description="Commitment conversion rate")
    closingRate: float = Field(description="Closure rate")
    rank: int = Field(description="Current rank")
    quartile: int = Field(description="Performance quartile")
    trendDirection: str = Field(description="Performance trend (up/down/stable)")
    bestDay: Optional[int] = Field(description="Best performing day")
    bestHour: Optional[str] = Field(description="Best performing hour")


class TeamPerformance(FrontendCompatibleModel):
    """
    Team performance aggregation
    """
    teamName: str = Field(description="Team name")
    totalAgents: int = Field(description="Number of agents in team")
    activeAgents: int = Field(description="Active agents in period")
    totalCalls: int = Field(description="Team total calls")
    totalCommitments: int = Field(description="Team total commitments")
    teamRecovery: float = Field(description="Team recovery amount")
    teamContactRate: float = Field(description="Team contact rate")
    teamConversionRate: float = Field(description="Team conversion rate")
    teamRank: int = Field(description="Team rank")
    topAgent: str = Field(description="Top agent in team")


class HourlyProductivityBreakdown(FrontendCompatibleModel):
    """
    Hourly productivity breakdown
    """
    hour: str = Field(description="Hour (HH:MM)")
    totalCalls: int = Field(description="Total calls in hour")
    totalCommitments: int = Field(description="Total commitments in hour")
    averagePerAgent: float = Field(description="Average calls per agent")
    conversionRate: float = Field(description="Hour conversion rate")
    activeAgents: int = Field(description="Active agents in hour")
    efficiency: float = Field(description="Hour efficiency score")
