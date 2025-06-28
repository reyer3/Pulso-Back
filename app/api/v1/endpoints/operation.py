"""
📞 Operation Analysis API Endpoints
Daily operation monitoring for call center performance and GTR analysis
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from app.core.dependencies import get_dashboard_service, get_cache_service
from app.core.logging import LoggerMixin
from app.models.operation import (
    OperationDayRequest,           # ✅ CORRECTO
    OperationDayAnalysisData,      # ✅ CORRECTO
    OperationDayAnalysisResponse,  # ✅ CORRECTO
    OperationDayKPI,
    ChannelMetric,
    HourlyPerformance,
    AttemptEffectiveness,
    QueuePerformance
)
from app.models.base import success_response, error_response
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService

router = APIRouter(prefix="/operation", tags=["operation"])


class OperationController(LoggerMixin):
    """Controller for daily operation analysis endpoints"""
    
    def __init__(self, dashboard_service: DashboardServiceV2, cache_service: CacheService):
        self.dashboard_service = dashboard_service
        self.cache_service = cache_service
    
    async def get_operation_analysis(
        self,
        fecha_analisis: Optional[date] = None,
        include_hourly: bool = True,
        include_attempts: bool = True,
        include_queues: bool = True
    ) -> OperationDayAnalysisData:  # ✅ CORRECTO: Devuelve datos directos
        """
        Generate daily operation analysis for call center performance
        
        Args:
            fecha_analisis: Date for operation analysis
            include_hourly: Include hourly performance breakdown
            include_attempts: Include attempt effectiveness analysis
            include_queues: Include queue performance metrics
            
        Returns:
            OperationDayAnalysisData - Direct data as Frontend expects
        """
        if fecha_analisis is None:
            fecha_analisis = date.today()
        
        self.logger.info(f"Generating operation analysis for {fecha_analisis}")
        
        try:
            # Check cache first
            cache_key = f"operation:{fecha_analisis}:{include_hourly}:{include_attempts}:{include_queues}"
            cached_data = await self.cache_service.get(cache_key)
            
            if cached_data:
                self.logger.info(f"Returning cached operation data for key: {cache_key}")
                return OperationDayAnalysisData.parse_obj(cached_data)
            
            # Get dashboard data for the analysis date
            dashboard_data = await self.dashboard_service.get_dashboard_data(
                filters={},
                fecha_corte=fecha_analisis
            )
            
            # Generate operation analysis components
            analysis_data = await self._generate_operation_analysis(
                dashboard_data=dashboard_data,
                fecha_analisis=fecha_analisis,
                include_hourly=include_hourly,
                include_attempts=include_attempts,
                include_queues=include_queues
            )
            
            # Cache for 30 minutes (operation data is more volatile)
            await self.cache_service.set(
                cache_key,
                analysis_data.dict(),
                expire_in=1800
            )
            
            self.logger.info(
                f"Generated operation analysis for {fecha_analisis} with "
                f"{len(analysis_data.kpis)} KPIs and "
                f"{len(analysis_data.channelPerformance)} channels"
            )
            
            return analysis_data
            
        except Exception as e:
            self.logger.error(f"Error generating operation analysis: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate operation analysis: {str(e)}"
            )
    
    async def _generate_operation_analysis(
        self,
        dashboard_data: Dict[str, Any],
        fecha_analisis: date,
        include_hourly: bool,
        include_attempts: bool,
        include_queues: bool
    ) -> OperationDayAnalysisData:  # ✅ CORRECTO
        """
        Generate complete operation analysis from dashboard data
        
        Args:
            dashboard_data: Dashboard data for the analysis date
            fecha_analisis: Date being analyzed
            include_hourly: Whether to include hourly breakdown
            include_attempts: Whether to include attempt effectiveness
            include_queues: Whether to include queue performance
            
        Returns:
            OperationDayAnalysisData - Direct data object
        """
        # Calculate daily KPIs
        kpis = self._calculate_daily_kpis(dashboard_data)
        
        # Generate channel performance comparison
        channel_performance = self._generate_channel_performance(dashboard_data)
        
        # Optional components based on flags
        hourly_performance = []
        if include_hourly:
            hourly_performance = self._generate_hourly_performance(dashboard_data)
        
        attempt_effectiveness = []
        if include_attempts:
            attempt_effectiveness = self._generate_attempt_effectiveness(dashboard_data)
        
        queue_performance = []
        if include_queues:
            queue_performance = self._generate_queue_performance(dashboard_data)
        
        return OperationDayAnalysisData(
            kpis=kpis,
            channelPerformance=channel_performance,
            hourlyPerformance=hourly_performance,
            attemptEffectiveness=attempt_effectiveness,
            queuePerformance=queue_performance
        )
    
    def _calculate_daily_kpis(self, dashboard_data: Dict[str, Any]) -> List[OperationDayKPI]:
        """
        Calculate key daily operation KPIs
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            List of daily operation KPIs
        """
        kpis = []
        
        # Extract totals from dashboard data
        totals = self._extract_operation_totals(dashboard_data)
        
        # Total Calls KPI
        kpis.append(OperationDayKPI(
            label="Total Llamadas",
            value=f"{totals.get('total_gestiones', 0):,}"
        ))
        
        # Contact Rate KPI
        contact_rate = totals.get('contacto', 0)
        kpis.append(OperationDayKPI(
            label="Tasa de Contacto",
            value=f"{contact_rate:.1f}%"
        ))
        
        # Direct Contact Rate KPI
        cd_rate = totals.get('cd', 0)
        kpis.append(OperationDayKPI(
            label="Contacto Directo",
            value=f"{cd_rate:.1f}%"
        ))
        
        # Closure Rate KPI
        closure_rate = totals.get('cierre', 0)
        kpis.append(OperationDayKPI(
            label="Tasa de Cierre",
            value=f"{closure_rate:.1f}%"
        ))
        
        # Intensity KPI
        intensity = totals.get('inten', 0)
        kpis.append(OperationDayKPI(
            label="Intensidad",
            value=f"{intensity:.1f}"
        ))
        
        # Total Accounts KPI
        kpis.append(OperationDayKPI(
            label="Cuentas Gestionadas",
            value=f"{totals.get('cuentas_gestionadas', 0):,}"
        ))
        
        return kpis
    
    def _extract_operation_totals(self, dashboard_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Extract operational totals from dashboard data
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            Dictionary with operational totals
        """
        totals = {
            'total_gestiones': 0,
            'cuentas_gestionadas': 0,
            'contacto': 0,
            'cd': 0,
            'ci': 0,
            'cierre': 0,
            'inten': 0
        }
        
        # Sum from segmento data (excluding totals row)
        total_cuentas = 0
        weighted_contacto = 0
        weighted_cd = 0
        weighted_ci = 0
        weighted_cierre = 0
        weighted_inten = 0
        
        for item in dashboard_data.get('segmentoData', []):
            if item.get('name') != 'Total':
                cuentas = item.get('cuentas', 0)
                total_cuentas += cuentas
                
                # Weighted averages
                weighted_contacto += item.get('contacto', 0) * cuentas
                weighted_cd += item.get('cd', 0) * cuentas
                weighted_ci += item.get('ci', 0) * cuentas
                weighted_cierre += item.get('cierre', 0) * cuentas
                weighted_inten += item.get('inten', 0) * cuentas
                
                # Sum countable metrics
                totals['total_gestiones'] += (
                    item.get('cdCount', 0) + 
                    item.get('ciCount', 0) + 
                    item.get('scCount', 0)
                )
        
        # Calculate weighted averages
        if total_cuentas > 0:
            totals['contacto'] = weighted_contacto / total_cuentas
            totals['cd'] = weighted_cd / total_cuentas
            totals['ci'] = weighted_ci / total_cuentas
            totals['cierre'] = weighted_cierre / total_cuentas
            totals['inten'] = weighted_inten / total_cuentas
        
        totals['cuentas_gestionadas'] = total_cuentas
        
        return totals
    
    def _generate_channel_performance(
        self, 
        dashboard_data: Dict[str, Any]
    ) -> List[ChannelMetric]:
        """
        Generate channel performance comparison (Bot vs Human)
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            List of channel performance metrics
        """
        # For now, create simulated channel breakdown
        # In future, this would come from gestiones_bot vs gestiones_humano data
        
        totals = self._extract_operation_totals(dashboard_data)
        total_calls = totals.get('total_gestiones', 0)
        total_effective = totals.get('cd', 0) + totals.get('ci', 0)
        
        # Simulate channel split (60% bot, 40% human typical distribution)
        voicebot_calls = int(total_calls * 0.6)
        callcenter_calls = int(total_calls * 0.4)
        
        voicebot_effective = int(total_effective * 0.5)  # Bot less effective per call
        callcenter_effective = int(total_effective * 0.8)  # Human more effective
        
        channels = [
            ChannelMetric(
                channel="Voicebot",
                calls=voicebot_calls,
                effectiveContacts=voicebot_effective,
                nonEffectiveContacts=voicebot_calls - voicebot_effective,
                pdp=int(voicebot_effective * 0.3),  # 30% conversion to PDP
                cierreRate=25.0  # 25% closure rate from effective contacts
            ),
            ChannelMetric(
                channel="Call Center",
                calls=callcenter_calls,
                effectiveContacts=callcenter_effective,
                nonEffectiveContacts=callcenter_calls - callcenter_effective,
                pdp=int(callcenter_effective * 0.4),  # 40% conversion to PDP
                cierreRate=35.0  # 35% closure rate from effective contacts
            )
        ]
        
        return channels
    
    def _generate_hourly_performance(
        self, 
        dashboard_data: Dict[str, Any]
    ) -> List[HourlyPerformance]:
        """
        Generate hourly performance breakdown
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            List of hourly performance metrics
        """
        # Simulate hourly distribution based on typical call center patterns
        hourly_data = []
        
        base_calls_per_hour = 100
        peak_hours = [10, 11, 14, 15, 16]  # Typical peak hours
        
        for hour in range(8, 19):  # 8 AM to 6 PM
            hour_str = f"{hour:02d}:00"
            
            # Adjust calls based on peak hours
            if hour in peak_hours:
                calls = int(base_calls_per_hour * 1.5)
            elif hour in [8, 9, 17, 18]:  # Shoulder hours
                calls = int(base_calls_per_hour * 0.8)
            else:
                calls = base_calls_per_hour
            
            # Calculate performance metrics
            effective_rate = 0.65 if hour in peak_hours else 0.55
            effective_contacts = int(calls * effective_rate)
            non_effective = calls - effective_contacts
            pdp = int(effective_contacts * 0.35)
            
            hourly_data.append(HourlyPerformance(
                hour=hour_str,
                effectiveContacts=effective_contacts,
                nonEffectiveContacts=non_effective,
                pdp=pdp
            ))
        
        return hourly_data
    
    def _generate_attempt_effectiveness(
        self, 
        dashboard_data: Dict[str, Any]
    ) -> List[AttemptEffectiveness]:
        """
        Generate attempt effectiveness analysis
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            List of attempt effectiveness metrics
        """
        # Simulate attempt effectiveness (typically decreases with more attempts)
        attempt_data = []
        
        base_closure_rate = 45.0
        
        for attempt in range(1, 6):  # First 5 attempts
            # Closure rate decreases with each attempt
            closure_rate = base_closure_rate * (0.8 ** (attempt - 1))
            
            attempt_data.append(AttemptEffectiveness(
                attempt=attempt,
                cierreRate=round(closure_rate, 1)
            ))
        
        return attempt_data
    
    def _generate_queue_performance(
        self, 
        dashboard_data: Dict[str, Any]
    ) -> List[QueuePerformance]:
        """
        Generate queue performance metrics
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            List of queue performance metrics
        """
        # Simulate queue performance based on cartera types
        queue_data = []
        
        for item in dashboard_data.get('segmentoData', []):
            if item.get('name') != 'Total':
                queue_name = item.get('name', '')
                cuentas = item.get('cuentas', 0)
                
                # Estimate calls and performance
                calls = int(cuentas * 1.5)  # 1.5 calls per account average
                effective_contacts = int(calls * (item.get('contacto', 0) / 100))
                pdp = int(effective_contacts * 0.3)
                closure_rate = item.get('cierre', 0)
                
                queue_data.append(QueuePerformance(
                    queueName=queue_name,
                    calls=calls,
                    effectiveContacts=effective_contacts,
                    pdp=pdp,
                    cierreRate=closure_rate
                ))
        
        # Sort by calls descending
        queue_data.sort(key=lambda x: x.calls, reverse=True)
        
        return queue_data


# =============================================================================
# FASTAPI ENDPOINTS
# =============================================================================

@router.get("/", response_model=OperationDayAnalysisData)  # ✅ CORRECTO
async def get_operation_analysis(
    fecha_analisis: Optional[date] = Query(None, description="Analysis date (YYYY-MM-DD)"),
    include_hourly: bool = Query(True, description="Include hourly performance breakdown"),
    include_attempts: bool = Query(True, description="Include attempt effectiveness analysis"),
    include_queues: bool = Query(True, description="Include queue performance metrics"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
    cache_service: CacheService = Depends(get_cache_service)
) -> OperationDayAnalysisData:  # ✅ CORRECTO
    """
    Get daily operation analysis for call center performance monitoring
    
    Returns OperationDayAnalysisData - DIRECT data as Frontend expects.
    No wrapper object, no extra metadata field.
    
    **Usage Examples:**
    
    - Today's operation: `/api/v1/operation/`
    - Specific date: `/api/v1/operation/?fecha_analisis=2025-06-27`
    - Minimal analysis: `/api/v1/operation/?include_hourly=false&include_attempts=false`
    
    **Analysis includes:**
    - Daily KPIs (calls, contact rates, closure rates)
    - Channel performance comparison (Bot vs Human)
    - Hourly performance breakdown (optional)
    - Attempt effectiveness analysis (optional)
    - Queue performance by cartera (optional)
    """
    controller = OperationController(dashboard_service, cache_service)
    
    return await controller.get_operation_analysis(
        fecha_analisis=fecha_analisis,
        include_hourly=include_hourly,
        include_attempts=include_attempts,
        include_queues=include_queues
    )


@router.post("/", response_model=OperationDayAnalysisData)  # ✅ NUEVO
async def get_operation_analysis_post(
    request: OperationDayRequest,
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
    cache_service: CacheService = Depends(get_cache_service)
) -> OperationDayAnalysisData:
    """
    Get operation analysis with POST method for complex requests
    
    Returns OperationDayAnalysisData directly - EXACT match with Frontend expectations.
    """
    controller = OperationController(dashboard_service, cache_service)
    
    # Parse date from request
    fecha_analisis = datetime.strptime(request.date, '%Y-%m-%d').date()
    
    return await controller.get_operation_analysis(
        fecha_analisis=fecha_analisis,
        include_hourly=request.includeHourlyBreakdown,
        include_attempts=request.includeAttemptAnalysis,
        include_queues=request.includeQueueDetails
    )


@router.get("/kpis", response_model=List[OperationDayKPI])
async def get_daily_kpis(
    fecha_analisis: Optional[date] = Query(None, description="Analysis date (YYYY-MM-DD)"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Get daily operation KPIs only (lightweight endpoint)
    
    Returns just the key daily metrics without detailed breakdowns
    """
    try:
        if fecha_analisis is None:
            fecha_analisis = date.today()
        
        dashboard_data = await dashboard_service.get_dashboard_data(
            filters={},
            fecha_corte=fecha_analisis
        )
        
        controller = OperationController(dashboard_service, None)
        kpis = controller._calculate_daily_kpis(dashboard_data)
        
        return kpis
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get daily KPIs: {str(e)}"
        )


@router.get("/channels", response_model=List[ChannelMetric])
async def get_channel_performance(
    fecha_analisis: Optional[date] = Query(None, description="Analysis date (YYYY-MM-DD)"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Get channel performance comparison (Bot vs Call Center)
    
    Returns performance metrics for each channel type
    """
    try:
        if fecha_analisis is None:
            fecha_analisis = date.today()
        
        dashboard_data = await dashboard_service.get_dashboard_data(
            filters={},
            fecha_corte=fecha_analisis
        )
        
        controller = OperationController(dashboard_service, None)
        channels = controller._generate_channel_performance(dashboard_data)
        
        return channels
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get channel performance: {str(e)}"
        )


@router.get("/health")
async def operation_health_check(
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Health check for operation endpoints
    """
    try:
        health_status = await dashboard_service.health_check()
        
        return success_response(
            data={
                "status": "healthy",
                "service": "operation",
                "timestamp": datetime.now().isoformat(),
                "dashboard_service": health_status
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content=error_response(
                message="Operation service unhealthy",
                details={
                    "service": "operation",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
            )
        )
