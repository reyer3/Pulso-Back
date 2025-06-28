"""
🚀 API V1 Endpoints for Dashboard
FastAPI endpoints that expose dashboard services with proper typing
"""

from datetime import date, datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks

from app.core.dependencies import get_dashboard_service, get_cache_service
from app.core.logging import LoggerMixin
from app.models.dashboard import (
    DashboardData,
    DashboardRequest, 
    DashboardFilters,
    DashboardHealthResponse
)
from app.models.base import success_response, error_response
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService


# =============================================================================
# ROUTER SETUP
# =============================================================================

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


class DashboardAPI(LoggerMixin):
    """
    Dashboard API endpoints using centralized dependencies and official models
    """
    
    # =============================================================================
    # DASHBOARD ENDPOINTS
    # =============================================================================
    @staticmethod
    @router.post("/dashboard", response_model=DashboardData)
    async def get_dashboard_data(
        request: DashboardRequest,
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> DashboardData:
        """
        Get dashboard data with filters
        
        Returns DashboardData - EXACT match with Frontend interface.
        Uses FrontendCompatibleModel for automatic camelCase serialization.
        
        This endpoint provides the main dashboard data matching the React frontend
        structure. Supports filtering by cartera, servicio, and date ranges.
        """
        try:
            # Set default fecha_corte if not provided
            if request.fechaCorte is None:
                request.fechaCorte = date.today()
            
            # Get dashboard data from service
            dashboard_data = await service.get_dashboard_data(
                filters=request.filters.model_dump() if request.filters else {},
                fecha_corte=request.fechaCorte
            )
            
            # Service returns DashboardData directly - no wrapper needed
            return dashboard_data
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate dashboard data: {str(e)}"
            )

    @staticmethod
    @router.get("/dashboard", response_model=DashboardData)
    async def get_dashboard_data_get(
        cartera: Optional[List[str]] = Query(default=None, description="Portfolio filters"),
        servicio: Optional[List[str]] = Query(default=None, description="Service filters"),
        fecha_corte: Optional[date] = Query(default=None, description="Cut-off date"),
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> DashboardData:
        """
        Get dashboard data with GET method (for simple queries)
        
        Returns DashboardData directly without metadata wrapper.
        """
        try:
            # Build filters from query parameters
            filters = DashboardFilters()
            if cartera:
                filters.cartera = cartera
            if servicio:
                filters.servicio = servicio
            
            # Set default fecha_corte if not provided
            if fecha_corte is None:
                fecha_corte = date.today()
            
            # Get dashboard data from service
            dashboard_data = await service.get_dashboard_data(
                filters=filters.model_dump(),
                fecha_corte=fecha_corte
            )
            
            return dashboard_data
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate dashboard data: {str(e)}"
            )
    
    # =============================================================================
    # UTILITY ENDPOINTS
    # =============================================================================
    @staticmethod
    @router.get("/health", response_model=DashboardHealthResponse)
    async def health_check(
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> DashboardHealthResponse:
        """
        Health check endpoint

        Verifies that the API and data sources are working correctly.
        """
        try:
            health_status = await service.health_check()
            return DashboardHealthResponse(**health_status)

        except Exception as e:
            # Return unhealthy status even if health check fails
            return DashboardHealthResponse(
                status="unhealthy",
                timestamp=datetime.now().isoformat(),
                dataSource={"type": "unknown", "connected": False, "dataset": "unknown"},
                processor={"initialized": False}
            )

    @staticmethod
    @router.get("/filters")
    async def get_filter_options():
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
            
            return success_response(data=filter_options)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get filter options: {str(e)}"
            )

    @staticmethod
    @router.post("/refresh")
    async def refresh_data(
        background_tasks: BackgroundTasks,
        force: bool = Query(default=False, description="Force refresh even if recently updated"),
        cache_service: CacheService = Depends(get_cache_service)
    ):
        """
        Trigger data refresh
        
        Initiates a background refresh of cached data.
        Used for the scheduled refresh functionality.
        """
        try:
            # Add background task for data refresh
            background_tasks.add_task(_refresh_data_task, cache_service, force)
            
            return success_response(
                message="Data refresh started in background",
                data={
                    "status": "refresh_initiated",
                    "force": force,
                    "timestamp": datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to initiate refresh: {str(e)}"
            )

    @staticmethod
    @router.get("/status")
    async def get_api_status(
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ):
        """
        Get API status and metadata
        """
        try:
            # Get service health status
            health_status = await service.health_check()
            
            status_data = {
                "api_version": "1.0.0",
                "status": "healthy" if health_status.get("status") == "healthy" else "degraded",
                "timestamp": datetime.now().isoformat(),
                "service_health": health_status,
                "endpoints": {
                    "dashboard": "/api/v1/dashboard",
                    "health": "/api/v1/health",
                    "filters": "/api/v1/filters",
                    "refresh": "/api/v1/refresh",
                    "status": "/api/v1/status"
                }
            }
            
            return success_response(data=status_data)
            
        except Exception as e:
            return error_response(
                message="Failed to get API status",
                details={"error": str(e)}
            )


# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def _refresh_data_task(cache_service: CacheService, force: bool = False):
    """
    Background task for data refresh
    """
    try:
        print(f"Data refresh task started (force={force}) at {datetime.now()}")
        
        # Clear cache if force refresh
        if force:
            await cache_service.clear_by_pattern("*")
            print("Cache cleared due to force refresh")
        
        # TODO: Implement actual refresh logic
        # - Trigger ETL pipeline
        # - Update last refresh timestamp
        # - Notify relevant services
        
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
