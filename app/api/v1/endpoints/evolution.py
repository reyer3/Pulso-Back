"""
📈 Evolution API Endpoints
Daily KPI tracking and evolution charts for React frontend
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from app.core.dependencies import get_dashboard_service, get_cache_service
from app.core.logging import LoggerMixin
from app.models.evolution import (
    EvolutionRequest,
    EvolutionResponse,
    EvolutionDataPoint,
    EvolutionMetric,
    EvolutionSeries
)
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService

router = APIRouter(prefix="/evolution", tags=["evolution"])


class EvolutionController(LoggerMixin):
    """Controller for evolution-related endpoints"""
    
    def __init__(self, dashboard_service: DashboardServiceV2, cache_service: CacheService):
        self.dashboard_service = dashboard_service
        self.cache_service = cache_service
    
    async def get_evolution_data(
        self,
        cartera: Optional[str] = None,
        servicio: Optional[str] = None,
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None,
        metrics: List[str] = None
    ) -> EvolutionResponse:
        """
        Generate evolution data for daily KPI tracking
        
        Args:
            cartera: Filter by cartera (TEMPRANA, ALTAS_NUEVAS, etc.)
            servicio: Filter by service (MOVIL, FIJA)
            fecha_inicio: Start date for evolution tracking
            fecha_fin: End date for evolution tracking
            metrics: List of metrics to track
            
        Returns:
            Evolution data with daily snapshots
        """
        # Default date range: last 30 days
        if fecha_fin is None:
            fecha_fin = date.today()
        if fecha_inicio is None:
            fecha_inicio = fecha_fin - timedelta(days=30)
        
        # Default metrics
        if metrics is None:
            metrics = ['cobertura', 'contacto', 'cd', 'ci', 'cierre', 'recupero']
        
        # Build filters
        filters = {}
        if cartera:
            filters['cartera'] = [cartera]
        if servicio:
            filters['servicio'] = [servicio]
        
        self.logger.info(
            f"Generating evolution data from {fecha_inicio} to {fecha_fin} "
            f"for filters: {filters}, metrics: {metrics}"
        )
        
        try:
            # Check cache first
            cache_key = f"evolution:{cartera or 'all'}:{servicio or 'all'}:{fecha_inicio}:{fecha_fin}"
            cached_data = await self.cache_service.get(cache_key)
            
            if cached_data:
                self.logger.info(f"Returning cached evolution data for key: {cache_key}")
                return EvolutionResponse.parse_obj(cached_data)
            
            # Generate evolution data using dashboard service
            evolution_data = await self.dashboard_service.get_evolution_data(
                filters=filters,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            # Transform to frontend format
            response_data = self._transform_evolution_data(
                evolution_data, 
                metrics
            )
            
            # Cache for 1 hour
            await self.cache_service.set(
                cache_key, 
                response_data.dict(), 
                expire_in=3600
            )
            
            self.logger.info(
                f"Generated evolution data with {len(response_data.data)} metrics "
                f"across {len(evolution_data.get('evolutionData', []))} days"
            )
            
            return response_data
            
        except Exception as e:
            self.logger.error(f"Error generating evolution data: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate evolution data: {str(e)}"
            )
    
    def _transform_evolution_data(
        self, 
        raw_evolution: Dict[str, Any], 
        requested_metrics: List[str]
    ) -> EvolutionResponse:
        """
        Transform dashboard evolution data to frontend format
        
        Args:
            raw_evolution: Raw evolution data from dashboard service
            requested_metrics: Metrics to include in response
            
        Returns:
            Formatted evolution response
        """
        evolution_points = raw_evolution.get('evolutionData', [])
        
        if not evolution_points:
            return EvolutionResponse(
                data=[],
                metadata=raw_evolution.get('metadata', {}),
                success=True,
                message="No data available for the specified date range"
            )
        
        # Group by cartera if multiple carteras in data
        cartera_groups = self._group_by_cartera(evolution_points)
        
        # Transform each metric
        metrics_data = []
        
        for metric in requested_metrics:
            if metric == 'recupero':
                # Special handling for recupero (currency)
                metric_data = EvolutionMetric(
                    metric=metric,
                    valueType='currency',
                    series=self._create_metric_series(cartera_groups, metric)
                )
            elif metric in ['cobertura', 'contacto', 'cd', 'ci', 'cierre']:
                # Percentage metrics
                metric_data = EvolutionMetric(
                    metric=metric,
                    valueType='percent',
                    series=self._create_metric_series(cartera_groups, metric)
                )
            else:
                # Number metrics
                metric_data = EvolutionMetric(
                    metric=metric,
                    valueType='number',
                    series=self._create_metric_series(cartera_groups, metric)
                )
            
            metrics_data.append(metric_data)
        
        return EvolutionResponse(
            data=metrics_data,
            metadata={
                **raw_evolution.get('metadata', {}),
                'requestedMetrics': requested_metrics,
                'totalDays': len(evolution_points),
                'carteras': list(cartera_groups.keys())
            },
            success=True,
            message=f"Evolution data generated for {len(evolution_points)} days"
        )
    
    def _group_by_cartera(self, evolution_points: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group evolution points by cartera for series generation
        
        Args:
            evolution_points: List of daily evolution data points
            
        Returns:
            Dictionary grouped by cartera
        """
        # For now, treat all data as one group
        # In future, can split by cartera field if available
        return {"TODAS": evolution_points}
    
    def _create_metric_series(
        self, 
        cartera_groups: Dict[str, List[Dict]], 
        metric: str
    ) -> List[EvolutionSeries]:
        """
        Create evolution series for a specific metric
        
        Args:
            cartera_groups: Evolution data grouped by cartera
            metric: Metric name to create series for
            
        Returns:
            List of evolution series
        """
        series_list = []
        
        for cartera_name, points in cartera_groups.items():
            # Create data points for this series
            data_points = []
            
            for point in points:
                if metric in point:
                    data_point = EvolutionDataPoint(
                        day=point.get('dia_gestion', 1),
                        value=float(point[metric]) if point[metric] is not None else 0.0
                    )
                    data_points.append(data_point)
            
            # Sort by day
            data_points.sort(key=lambda x: x.day)
            
            series = EvolutionSeries(
                name=cartera_name,
                data=data_points
            )
            
            series_list.append(series)
        
        return series_list


# =============================================================================
# FASTAPI ENDPOINTS
# =============================================================================

@router.get("/", response_model=EvolutionResponse)
async def get_evolution_data(
    cartera: Optional[str] = Query(None, description="Filter by cartera type"),
    servicio: Optional[str] = Query(None, description="Filter by service type (MOVIL/FIJA)"),
    fecha_inicio: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    fecha_fin: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    metrics: Optional[str] = Query(
        "cobertura,contacto,cd,ci,cierre,recupero", 
        description="Comma-separated list of metrics"
    ),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
    cache_service: CacheService = Depends(get_cache_service)
):
    """
    Get evolution data for daily KPI tracking
    
    **Usage Examples:**
    
    - Get last 30 days evolution: `/api/v1/evolution/`
    - Filter by cartera: `/api/v1/evolution/?cartera=TEMPRANA`
    - Custom date range: `/api/v1/evolution/?fecha_inicio=2025-06-01&fecha_fin=2025-06-27`
    - Specific metrics: `/api/v1/evolution/?metrics=cobertura,contacto,cierre`
    
    **Metrics available:**
    - `cobertura`: Coverage percentage
    - `contacto`: Contact percentage  
    - `cd`: Direct contact percentage
    - `ci`: Indirect contact percentage
    - `sc`: No contact percentage
    - `cierre`: Closure rate percentage
    - `recupero`: Recovery amount (currency)
    - `intensidad`: Intensity (attempts per account)
    """
    controller = EvolutionController(dashboard_service, cache_service)
    
    # Parse metrics string
    metrics_list = [m.strip() for m in metrics.split(',') if m.strip()] if metrics else None
    
    return await controller.get_evolution_data(
        cartera=cartera,
        servicio=servicio,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        metrics=metrics_list
    )


@router.get("/carteras", response_model=List[str])
async def get_available_carteras(
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Get list of available carteras for filtering
    
    Returns list of cartera names that can be used in evolution filters
    """
    try:
        # Get a sample dashboard data to extract available carteras
        sample_data = await dashboard_service.get_dashboard_data({})
        
        carteras = set()
        for item in sample_data.get('segmentoData', []):
            if 'name' in item and item['name'] != 'Total':
                cartera_name = item['name'].split()[0]  # Extract cartera from "TEMPRANA VTO 1"
                carteras.add(cartera_name)
        
        return sorted(list(carteras))
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get available carteras: {str(e)}"
        )


@router.get("/metrics", response_model=List[Dict[str, str]])
async def get_available_metrics():
    """
    Get list of available metrics for evolution tracking
    
    Returns list of metrics with their descriptions and value types
    """
    metrics = [
        {"metric": "cobertura", "description": "Coverage percentage", "valueType": "percent"},
        {"metric": "contacto", "description": "Contact percentage", "valueType": "percent"},
        {"metric": "cd", "description": "Direct contact percentage", "valueType": "percent"},
        {"metric": "ci", "description": "Indirect contact percentage", "valueType": "percent"},
        {"metric": "sc", "description": "No contact percentage", "valueType": "percent"},
        {"metric": "cierre", "description": "Closure rate percentage", "valueType": "percent"},
        {"metric": "recupero", "description": "Recovery amount", "valueType": "currency"},
        {"metric": "intensidad", "description": "Intensity (attempts per account)", "valueType": "number"},
    ]
    
    return metrics


@router.get("/health")
async def evolution_health_check(
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Health check for evolution endpoints
    """
    try:
        health_status = await dashboard_service.health_check()
        
        return {
            "status": "healthy",
            "service": "evolution",
            "timestamp": datetime.now().isoformat(),
            "dashboard_service": health_status
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy", 
                "service": "evolution",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )
