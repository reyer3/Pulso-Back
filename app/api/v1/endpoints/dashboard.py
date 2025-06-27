"""
🚀 API V1 Endpoints for Dashboard
FastAPI endpoints that expose dashboard services
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.logging import LoggerMixin
from app.repositories.data_adapters import DataSourceFactory, DataSourceAdapter
from app.services.dashboard_service_v2 import DashboardServiceV2


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class FilterRequest(BaseModel):
    """Request model for filters"""
    cartera: Optional[List[str]] = Field(default=None, description="Portfolio filters")
    servicio: Optional[List[str]] = Field(default=None, description="Service filters (MOVIL/FIJA)")
    periodo: Optional[List[str]] = Field(default=None, description="Period filters")
    fecha_inicio: Optional[date] = Field(default=None, description="Start date filter")
    fecha_fin: Optional[date] = Field(default=None, description="End date filter")


class DashboardRequest(BaseModel):
    """Request model for dashboard data"""
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Filter criteria")
    fecha_corte: Optional[date] = Field(default=None, description="Cut-off date for analysis")
    dimensions: Optional[List[str]] = Field(default=["cartera"], description="Chart dimensions")


class EvolutionRequest(BaseModel):
    """Request model for evolution data"""
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Filter criteria")
    fecha_inicio: date = Field(description="Start date for evolution")
    fecha_fin: date = Field(description="End date for evolution")
    comparison_dimension: Optional[str] = Field(default="cartera", description="Dimension for comparison")


class DashboardResponse(BaseModel):
    """Response model for dashboard data"""
    segmentoData: List[Dict[str, Any]] = Field(description="Segment aggregated data")
    negocioData: List[Dict[str, Any]] = Field(description="Service aggregated data")
    integralChartData: List[Dict[str, Any]] = Field(description="Chart data for KPIs")
    metadata: Dict[str, Any] = Field(description="Response metadata")


class EvolutionResponse(BaseModel):
    """Response model for evolution data"""
    evolutionData: List[Dict[str, Any]] = Field(description="Daily evolution data")
    metadata: Dict[str, Any] = Field(description="Response metadata")


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str = Field(description="Health status")
    timestamp: str = Field(description="Check timestamp")
    data_source: Dict[str, Any] = Field(description="Data source status")
    processor: Dict[str, Any] = Field(description="Processor status")


# =============================================================================
# DEPENDENCY INJECTION
# =============================================================================

async def get_data_adapter() -> DataSourceAdapter:
    """
    Get data source adapter based on configuration
    """
    return DataSourceFactory.create_adapter()


async def get_dashboard_service(
    adapter: DataSourceAdapter = Depends(get_data_adapter)
) -> DashboardServiceV2:
    """
    Get dashboard service with injected adapter
    """
    return DashboardServiceV2(adapter)


# =============================================================================
# ROUTER SETUP
# =============================================================================

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


class DashboardAPI(LoggerMixin):
    """
    Dashboard API endpoints
    """
    
    # =============================================================================
    # DASHBOARD ENDPOINTS
    # =============================================================================
    
    @router.post("/dashboard", response_model=DashboardResponse)
    async def get_dashboard_data(
        request: DashboardRequest,
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> DashboardResponse:
        """
        Get dashboard data with filters
        
        This endpoint provides the main dashboard data matching the React frontend
        structure. Supports filtering by cartera, servicio, and date ranges.
        """
        try:
            # Set default fecha_corte if not provided
            if request.fecha_corte is None:
                request.fecha_corte = date.today()
            
            # Get dashboard data from service
            dashboard_data = await service.get_dashboard_data(
                filters=request.filters,
                fecha_corte=request.fecha_corte
            )
            
            return DashboardResponse(**dashboard_data)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate dashboard data: {str(e)}"
            )
    
    @router.get("/dashboard", response_model=DashboardResponse)
    async def get_dashboard_data_get(
        cartera: Optional[List[str]] = Query(default=None, description="Portfolio filters"),
        servicio: Optional[List[str]] = Query(default=None, description="Service filters"),
        fecha_corte: Optional[date] = Query(default=None, description="Cut-off date"),
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> DashboardResponse:
        """
        Get dashboard data with GET method (for simple queries)
        """
        try:
            # Build filters from query parameters
            filters = {}
            if cartera:
                filters['cartera'] = cartera
            if servicio:
                filters['servicio'] = servicio
            
            # Set default fecha_corte if not provided
            if fecha_corte is None:
                fecha_corte = date.today()
            
            # Get dashboard data from service
            dashboard_data = await service.get_dashboard_data(
                filters=filters,
                fecha_corte=fecha_corte
            )
            
            return DashboardResponse(**dashboard_data)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate dashboard data: {str(e)}"
            )
    
    # =============================================================================
    # EVOLUTION ENDPOINTS
    # =============================================================================
    
    @router.post("/evolution", response_model=EvolutionResponse)
    async def get_evolution_data(
        request: EvolutionRequest,
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> EvolutionResponse:
        """
        Get evolution data for daily tracking
        
        Returns daily snapshots of KPIs for trend analysis.
        Used by the EvolutionPage in the React frontend.
        """
        try:
            # Validate date range
            if request.fecha_fin < request.fecha_inicio:
                raise HTTPException(
                    status_code=400,
                    detail="End date must be after start date"
                )
            
            # Limit date range to avoid performance issues
            max_days = 90
            if (request.fecha_fin - request.fecha_inicio).days > max_days:
                raise HTTPException(
                    status_code=400,
                    detail=f"Date range cannot exceed {max_days} days"
                )
            
            # Get evolution data from service
            evolution_data = await service.get_evolution_data(
                filters=request.filters,
                fecha_inicio=request.fecha_inicio,
                fecha_fin=request.fecha_fin
            )
            
            return EvolutionResponse(**evolution_data)
            
        except HTTPException:
            raise  # Re-raise HTTP exceptions
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate evolution data: {str(e)}"
            )
    
    @router.get("/evolution", response_model=EvolutionResponse)
    async def get_evolution_data_get(
        fecha_inicio: date = Query(description="Start date for evolution"),
        fecha_fin: date = Query(description="End date for evolution"),
        cartera: Optional[List[str]] = Query(default=None, description="Portfolio filters"),
        servicio: Optional[List[str]] = Query(default=None, description="Service filters"),
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> EvolutionResponse:
        """
        Get evolution data with GET method
        """
        try:
            # Build filters from query parameters
            filters = {}
            if cartera:
                filters['cartera'] = cartera
            if servicio:
                filters['servicio'] = servicio
            
            # Validate date range
            if fecha_fin < fecha_inicio:
                raise HTTPException(
                    status_code=400,
                    detail="End date must be after start date"
                )
            
            # Get evolution data from service
            evolution_data = await service.get_evolution_data(
                filters=filters,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            return EvolutionResponse(**evolution_data)
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate evolution data: {str(e)}"
            )
    
    # =============================================================================
    # UTILITY ENDPOINTS
    # =============================================================================
    
    @router.get("/health", response_model=HealthResponse)
    async def health_check(
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> HealthResponse:
        """
        Health check endpoint
        
        Verifies that the API and data sources are working correctly.
        """
        try:
            health_status = await service.health_check()
            return HealthResponse(**health_status)
            
        except Exception as e:
            # Return unhealthy status even if health check fails
            return HealthResponse(
                status="unhealthy",
                timestamp=datetime.now().isoformat(),
                data_source={"type": "unknown", "connected": False, "dataset": "unknown"},
                processor={"initialized": False},
                error=str(e)
            )
    
    @router.get("/filters")
    async def get_filter_options() -> Dict[str, List[Dict[str, str]]]:
        """
        Get available filter options
        
        Returns the available values for each filter type to populate
        dropdown menus in the frontend.
        """
        try:
            # These could be dynamic based on current data
            # For now, return static options that match the business logic
            filter_options = {
                "cartera": [
                    {"value": "TODAS", "label": "Todas las Carteras"},
                    {"value": "TEMPRANA", "label": "Temprana"},
                    {"value": "ALTAS_NUEVAS", "label": "Altas Nuevas"},
                    {"value": "CUOTA_FRACCIONAMIENTO", "label": "Cuota Fraccionamiento"},
                    {"value": "OTRAS", "label": "Otras"}
                ],
                "servicio": [
                    {"value": "TODOS", "label": "Todos los Servicios"},
                    {"value": "MOVIL", "label": "Móvil"},
                    {"value": "FIJA", "label": "Fija"}
                ],
                "periodo": [
                    {"value": "ACTUAL", "label": "Período Actual"},
                    {"value": "ANTERIOR", "label": "Período Anterior"},
                    {"value": "ULTIMO_MES", "label": "Último Mes"},
                    {"value": "ULTIMOS_3_MESES", "label": "Últimos 3 Meses"}
                ]
            }
            
            return filter_options
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get filter options: {str(e)}"
            )
    
    @router.post("/refresh")
    async def refresh_data(
        background_tasks: BackgroundTasks,
        force: bool = Query(default=False, description="Force refresh even if recently updated")
    ) -> Dict[str, str]:
        """
        Trigger data refresh
        
        Initiates a background refresh of cached data.
        Used for the scheduled refresh functionality.
        """
        try:
            # Add background task for data refresh
            background_tasks.add_task(_refresh_data_task, force)
            
            return {
                "status": "refresh_initiated",
                "message": "Data refresh started in background",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to initiate refresh: {str(e)}"
            )
    
    @router.get("/status")
    async def get_api_status() -> Dict[str, Any]:
        """
        Get API status and metadata
        """
        try:
            # Test all data source connections
            connection_status = await DataSourceFactory.test_all_connections()
            
            status = {
                "api_version": "1.0.0",
                "status": "healthy" if any(connection_status.values()) else "degraded",
                "timestamp": datetime.now().isoformat(),
                "data_sources": connection_status,
                "endpoints": {
                    "dashboard": "/api/v1/dashboard",
                    "evolution": "/api/v1/evolution",
                    "health": "/api/v1/health",
                    "filters": "/api/v1/filters",
                    "refresh": "/api/v1/refresh"
                }
            }
            
            return status
            
        except Exception as e:
            return {
                "api_version": "1.0.0",
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }


# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def _refresh_data_task(force: bool = False):
    """
    Background task for data refresh
    """
    try:
        # This would implement cache invalidation or ETL trigger
        # For now, it's a placeholder
        print(f"Data refresh task started (force={force}) at {datetime.now()}")
        
        # TODO: Implement actual refresh logic
        # - Clear Redis cache
        # - Trigger ETL pipeline
        # - Update last refresh timestamp
        
        print(f"Data refresh task completed at {datetime.now()}")
        
    except Exception as e:
        print(f"Data refresh task failed: {str(e)}")


# =============================================================================
# REGISTER API ROUTES
# =============================================================================

# Create API instance to register methods
api = DashboardAPI()

# The router is already configured with the endpoints above
# This will be imported in main.py and included in the FastAPI app
