"""
⚡ API V1 Endpoints for Productivity Analysis
FastAPI endpoints for agent productivity and performance monitoring
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.logging import LoggerMixin
from app.repositories.data_adapters import DataSourceFactory, DataSourceAdapter
from app.services.dashboard_service_v2 import DashboardServiceV2


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class ProductivityRequest(BaseModel):
    """Request model for productivity analysis"""
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Filter criteria")
    fecha_inicio: Optional[date] = Field(default=None, description="Start date for analysis")
    fecha_fin: Optional[date] = Field(default=None, description="End date for analysis")
    metric_type: Optional[str] = Field(default="gestiones", description="Heatmap metric type")


class AgentDailyPerformance(BaseModel):
    """Daily performance for an agent"""
    gestiones: Optional[int] = Field(default=None)
    contactosEfectivos: Optional[int] = Field(default=None)
    compromisos: Optional[int] = Field(default=None)


class AgentHeatmapRow(BaseModel):
    """Agent heatmap row data"""
    id: str = Field(description="Agent ID")
    dni: str = Field(description="Agent DNI")
    agentName: str = Field(description="Agent name")
    dailyPerformance: Dict[int, Optional[AgentDailyPerformance]] = Field(description="Daily performance by day")


class ProductivityTrendPoint(BaseModel):
    """Productivity trend data point"""
    day: Optional[int] = Field(default=None, description="Day number for daily trend")
    hour: Optional[str] = Field(default=None, description="Hour for hourly trend")
    llamadas: int = Field(description="Number of calls")
    compromisos: int = Field(description="Number of commitments")
    recupero: Optional[float] = Field(default=None, description="Amount recovered (daily only)")


class AgentRankingRow(BaseModel):
    """Agent ranking data"""
    id: str = Field(description="Agent ID")
    rank: int = Field(description="Agent rank")
    agentName: str = Field(description="Agent name")
    calls: int = Field(description="Total calls")
    directContacts: int = Field(description="Direct contacts")
    commitments: int = Field(description="Commitments made")
    amountRecovered: float = Field(description="Amount recovered")
    closingRate: float = Field(description="Closing rate percentage")
    commitmentConversion: float = Field(description="Commitment conversion percentage")
    quartile: int = Field(description="Performance quartile (1-4)")


class ProductivityResponse(BaseModel):
    """Response model for productivity data"""
    dailyTrend: List[ProductivityTrendPoint] = Field(description="Daily productivity trend")
    hourlyTrend: List[ProductivityTrendPoint] = Field(description="Hourly productivity trend")
    agentRanking: List[AgentRankingRow] = Field(description="Agent ranking")
    agentHeatmap: List[AgentHeatmapRow] = Field(description="Agent performance heatmap")
    metadata: Dict[str, Any] = Field(description="Response metadata")


# =============================================================================
# ROUTER SETUP
# =============================================================================

router = APIRouter(prefix="/api/v1", tags=["productivity"])


class ProductivityAPI(LoggerMixin):
    """
    Productivity API endpoints for agent performance analysis
    """
    
    @router.post("/productivity", response_model=ProductivityResponse)
    async def get_productivity_data(
        request: ProductivityRequest,
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> ProductivityResponse:
        """
        Get productivity analysis data
        
        Provides agent productivity metrics including daily trends, hourly patterns,
        agent rankings, and performance heatmaps for call center monitoring.
        """
        try:
            # Set default date range if not provided
            if request.fecha_fin is None:
                request.fecha_fin = date.today()
            if request.fecha_inicio is None:
                request.fecha_inicio = request.fecha_fin - timedelta(days=30)
            
            # Validate date range
            if request.fecha_fin < request.fecha_inicio:
                raise HTTPException(
                    status_code=400,
                    detail="End date must be after start date"
                )
            
            # Generate productivity data using service
            productivity_data = await service.get_productivity_data(
                filters=request.filters,
                fecha_inicio=request.fecha_inicio,
                fecha_fin=request.fecha_fin,
                metric_type=request.metric_type
            )
            
            return ProductivityResponse(**productivity_data)
            
        except HTTPException:
            raise  # Re-raise HTTP exceptions
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate productivity data: {str(e)}"
            )
    
    @router.get("/productivity", response_model=ProductivityResponse)
    async def get_productivity_data_get(
        fecha_inicio: Optional[date] = Query(default=None, description="Start date"),
        fecha_fin: Optional[date] = Query(default=None, description="End date"),
        metric_type: str = Query(default="gestiones", description="Heatmap metric type"),
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> ProductivityResponse:
        """
        Get productivity data with GET method (for simple queries)
        """
        try:
            # Set default date range
            if fecha_fin is None:
                fecha_fin = date.today()
            if fecha_inicio is None:
                fecha_inicio = fecha_fin - timedelta(days=30)
            
            # Generate productivity data
            productivity_data = await service.get_productivity_data(
                filters={},
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                metric_type=metric_type
            )
            
            return ProductivityResponse(**productivity_data)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate productivity data: {str(e)}"
            )
    
    @router.get("/productivity/agents/{agent_id}")
    async def get_agent_detail(
        agent_id: str,
        fecha_inicio: Optional[date] = Query(default=None),
        fecha_fin: Optional[date] = Query(default=None),
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> Dict[str, Any]:
        """
        Get detailed performance data for a specific agent
        """
        try:
            # Set default date range
            if fecha_fin is None:
                fecha_fin = date.today()
            if fecha_inicio is None:
                fecha_inicio = fecha_fin - timedelta(days=30)
            
            agent_detail = await service.get_agent_detail(
                agent_id=agent_id,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            return agent_detail
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get agent detail: {str(e)}"
            )
    
    @router.get("/productivity/metrics")
    async def get_productivity_metrics() -> Dict[str, List[str]]:
        """
        Get available productivity metrics for filtering
        """
        try:
            metrics = {
                "heatmap_metrics": [
                    "gestiones",
                    "contactosEfectivos", 
                    "compromisos"
                ],
                "trend_metrics": [
                    "llamadas",
                    "compromisos",
                    "recupero"
                ],
                "ranking_metrics": [
                    "calls",
                    "directContacts",
                    "commitments",
                    "closingRate",
                    "commitmentConversion"
                ]
            }
            
            return metrics
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get productivity metrics: {str(e)}"
            )


# =============================================================================
# REGISTER API ROUTES
# =============================================================================

# Create API instance to register methods
api = ProductivityAPI()

# The router is already configured with the endpoints above
# This will be imported in the main API router
