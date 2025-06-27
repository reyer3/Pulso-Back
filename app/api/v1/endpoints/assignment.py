"""
📊 Assignment Analysis API Endpoints  
Portfolio composition analysis and KPI comparison for executive reporting
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from app.core.dependencies import get_dashboard_service, get_cache_service
from app.core.logging import LoggerMixin
from app.models.assignment import (
    AssignmentAnalysisRequest,
    AssignmentAnalysisResponse,
    AssignmentKPI,
    CompositionDataPoint,
    DetailBreakdownRow
)
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService

router = APIRouter(prefix="/assignment", tags=["assignment"])


class AssignmentController(LoggerMixin):
    """Controller for assignment analysis endpoints"""
    
    def __init__(self, dashboard_service: DashboardServiceV2, cache_service: CacheService):
        self.dashboard_service = dashboard_service
        self.cache_service = cache_service
    
    async def get_assignment_analysis(
        self,
        fecha_actual: Optional[date] = None,
        fecha_anterior: Optional[date] = None,
        cartera_filter: Optional[str] = None
    ) -> AssignmentAnalysisResponse:
        """
        Generate assignment composition analysis with period comparison
        
        Args:
            fecha_actual: Current period date for comparison
            fecha_anterior: Previous period date for comparison  
            cartera_filter: Optional filter by specific cartera
            
        Returns:
            Assignment analysis with KPIs and composition breakdown
        """
        # Default dates: current vs previous month
        if fecha_actual is None:
            fecha_actual = date.today()
        if fecha_anterior is None:
            fecha_anterior = fecha_actual - timedelta(days=30)
        
        # Build filters
        filters = {}
        if cartera_filter:
            filters['cartera'] = [cartera_filter]
        
        self.logger.info(
            f"Generating assignment analysis for {fecha_actual} vs {fecha_anterior} "
            f"with filters: {filters}"
        )
        
        try:
            # Check cache first
            cache_key = f"assignment:{cartera_filter or 'all'}:{fecha_actual}:{fecha_anterior}"
            cached_data = await self.cache_service.get(cache_key)
            
            if cached_data:
                self.logger.info(f"Returning cached assignment data for key: {cache_key}")
                return AssignmentAnalysisResponse.parse_obj(cached_data)
            
            # Get dashboard data for both periods
            current_data = await self.dashboard_service.get_dashboard_data(
                filters=filters,
                fecha_corte=fecha_actual
            )
            
            previous_data = await self.dashboard_service.get_dashboard_data(
                filters=filters,
                fecha_corte=fecha_anterior
            )
            
            # Generate assignment analysis
            analysis_response = self._generate_assignment_analysis(
                current_data=current_data,
                previous_data=previous_data,
                fecha_actual=fecha_actual,
                fecha_anterior=fecha_anterior
            )
            
            # Cache for 2 hours
            await self.cache_service.set(
                cache_key,
                analysis_response.dict(),
                expire_in=7200
            )
            
            self.logger.info(
                f"Generated assignment analysis with {len(analysis_response.kpis)} KPIs "
                f"and {len(analysis_response.detailBreakdown)} detail rows"
            )
            
            return analysis_response
            
        except Exception as e:
            self.logger.error(f"Error generating assignment analysis: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate assignment analysis: {str(e)}"
            )
    
    def _generate_assignment_analysis(
        self,
        current_data: Dict[str, Any],
        previous_data: Dict[str, Any],
        fecha_actual: date,
        fecha_anterior: date
    ) -> AssignmentAnalysisResponse:
        """
        Generate assignment analysis from dashboard data comparison
        
        Args:
            current_data: Dashboard data for current period
            previous_data: Dashboard data for previous period
            fecha_actual: Current period date
            fecha_anterior: Previous period date
            
        Returns:
            Complete assignment analysis response
        """
        # Calculate executive KPIs
        kpis = self._calculate_executive_kpis(current_data, previous_data)
        
        # Generate composition data (portfolio distribution)
        composition_data = self._generate_composition_data(current_data)
        
        # Create detailed breakdown by cartera
        detail_breakdown = self._create_detail_breakdown(current_data, previous_data)
        
        return AssignmentAnalysisResponse(
            kpis=kpis,
            compositionData=composition_data,
            detailBreakdown=detail_breakdown,
            metadata={
                "fechaActual": fecha_actual.isoformat(),
                "fechaAnterior": fecha_anterior.isoformat(),
                "totalCarteras": len(detail_breakdown),
                "lastRefresh": datetime.now().isoformat()
            },
            success=True,
            message=f"Assignment analysis generated for {fecha_actual} vs {fecha_anterior}"
        )
    
    def _calculate_executive_kpis(
        self,
        current_data: Dict[str, Any],
        previous_data: Dict[str, Any]
    ) -> List[AssignmentKPI]:
        """
        Calculate executive-level KPIs with period comparison
        
        Args:
            current_data: Current period dashboard data
            previous_data: Previous period dashboard data
            
        Returns:
            List of executive KPIs with variations
        """
        kpis = []
        
        # Extract totals from dashboard data
        current_totals = self._extract_totals(current_data)
        previous_totals = self._extract_totals(previous_data)
        
        # Total Clients KPI
        kpis.append(AssignmentKPI(
            label="Total Clientes",
            valorActual=current_totals.get('clientes', 0),
            valorAnterior=previous_totals.get('clientes', 0),
            variacion=self._calculate_variation(
                current_totals.get('clientes', 0),
                previous_totals.get('clientes', 0)
            ),
            valueType="number"
        ))
        
        # Total Accounts KPI
        kpis.append(AssignmentKPI(
            label="Total Cuentas",
            valorActual=current_totals.get('cuentas', 0),
            valorAnterior=previous_totals.get('cuentas', 0),
            variacion=self._calculate_variation(
                current_totals.get('cuentas', 0),
                previous_totals.get('cuentas', 0)
            ),
            valueType="number"
        ))
        
        # Total Debt KPI
        kpis.append(AssignmentKPI(
            label="Saldo Total Deuda",
            valorActual=current_totals.get('deudaAsig', 0),
            valorAnterior=previous_totals.get('deudaAsig', 0),
            variacion=self._calculate_variation(
                current_totals.get('deudaAsig', 0),
                previous_totals.get('deudaAsig', 0)
            ),
            valueType="currency"
        ))
        
        # Average Ticket KPI
        current_ticket = (
            current_totals.get('deudaAsig', 0) / max(current_totals.get('cuentas', 1), 1)
        )
        previous_ticket = (
            previous_totals.get('deudaAsig', 0) / max(previous_totals.get('cuentas', 1), 1)
        )
        
        kpis.append(AssignmentKPI(
            label="Ticket Promedio",
            valorActual=current_ticket,
            valorAnterior=previous_ticket,
            variacion=self._calculate_variation(current_ticket, previous_ticket),
            valueType="currency"
        ))
        
        return kpis
    
    def _extract_totals(self, dashboard_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Extract total values from dashboard data
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            Dictionary with total values
        """
        totals = {
            'clientes': 0,
            'cuentas': 0,
            'deudaAsig': 0,
            'deudaAct': 0
        }
        
        # Sum from segmento data
        for item in dashboard_data.get('segmentoData', []):
            if item.get('name') != 'Total':  # Skip total row
                totals['cuentas'] += item.get('cuentas', 0)
                totals['deudaAsig'] += item.get('deudaAsig', 0)
                totals['deudaAct'] += item.get('deudaAct', 0)
        
        # Estimate clients (assuming ~1.2 accounts per client)
        totals['clientes'] = int(totals['cuentas'] / 1.2) if totals['cuentas'] > 0 else 0
        
        return totals
    
    def _calculate_variation(self, current: float, previous: float) -> float:
        """
        Calculate percentage variation between periods
        
        Args:
            current: Current period value
            previous: Previous period value
            
        Returns:
            Percentage variation
        """
        if previous == 0:
            return 100.0 if current > 0 else 0.0
        
        return round(((current - previous) / previous) * 100, 1)
    
    def _generate_composition_data(
        self,
        current_data: Dict[str, Any]
    ) -> List[CompositionDataPoint]:
        """
        Generate portfolio composition data points
        
        Args:
            current_data: Current period dashboard data
            
        Returns:
            List of composition data points for charts
        """
        composition_data = []
        
        # Group by cartera from segmento data
        cartera_totals = {}
        
        for item in current_data.get('segmentoData', []):
            if item.get('name') != 'Total':
                # Extract cartera name (first word)
                cartera_name = item.get('name', '').split()[0]
                
                if cartera_name not in cartera_totals:
                    cartera_totals[cartera_name] = 0
                
                cartera_totals[cartera_name] += item.get('deudaAsig', 0)
        
        # Convert to composition data points
        for cartera, saldo in cartera_totals.items():
            composition_data.append(CompositionDataPoint(
                name=cartera,
                value=saldo
            ))
        
        # Sort by value descending
        composition_data.sort(key=lambda x: x.value, reverse=True)
        
        return composition_data
    
    def _create_detail_breakdown(
        self,
        current_data: Dict[str, Any],
        previous_data: Dict[str, Any]
    ) -> List[DetailBreakdownRow]:
        """
        Create detailed breakdown by cartera
        
        Args:
            current_data: Current period dashboard data
            previous_data: Previous period dashboard data
            
        Returns:
            List of detailed breakdown rows
        """
        detail_rows = []
        
        # Create lookup for previous data
        previous_lookup = {}
        for item in previous_data.get('segmentoData', []):
            if item.get('name') != 'Total':
                cartera_name = item.get('name', '').split()[0]
                if cartera_name not in previous_lookup:
                    previous_lookup[cartera_name] = {
                        'cuentas': 0,
                        'deudaAsig': 0
                    }
                previous_lookup[cartera_name]['cuentas'] += item.get('cuentas', 0)
                previous_lookup[cartera_name]['deudaAsig'] += item.get('deudaAsig', 0)
        
        # Group current data by cartera
        current_cartera_data = {}
        for item in current_data.get('segmentoData', []):
            if item.get('name') != 'Total':
                cartera_name = item.get('name', '').split()[0]
                if cartera_name not in current_cartera_data:
                    current_cartera_data[cartera_name] = {
                        'cuentas': 0,
                        'deudaAsig': 0
                    }
                current_cartera_data[cartera_name]['cuentas'] += item.get('cuentas', 0)
                current_cartera_data[cartera_name]['deudaAsig'] += item.get('deudaAsig', 0)
        
        # Create detail rows
        for cartera_name, current_values in current_cartera_data.items():
            previous_values = previous_lookup.get(cartera_name, {'cuentas': 0, 'deudaAsig': 0})
            
            # Calculate values
            current_cuentas = current_values['cuentas']
            previous_cuentas = previous_values['cuentas']
            current_clientes = int(current_cuentas / 1.2) if current_cuentas > 0 else 0
            previous_clientes = int(previous_cuentas / 1.2) if previous_cuentas > 0 else 0
            
            current_saldo = current_values['deudaAsig']
            previous_saldo = previous_values['deudaAsig']
            
            current_ticket = current_saldo / max(current_cuentas, 1)
            previous_ticket = previous_saldo / max(previous_cuentas, 1)
            
            detail_row = DetailBreakdownRow(
                id=cartera_name.lower(),
                name=cartera_name,
                clientesActual=current_clientes,
                clientesAnterior=previous_clientes,
                cuentasActual=current_cuentas,
                cuentasAnterior=previous_cuentas,
                saldoActual=current_saldo,
                saldoAnterior=previous_saldo,
                ticketPromedioActual=current_ticket,
                ticketPromedioAnterior=previous_ticket
            )
            
            detail_rows.append(detail_row)
        
        # Sort by current saldo descending
        detail_rows.sort(key=lambda x: x.saldoActual, reverse=True)
        
        return detail_rows


# =============================================================================
# FASTAPI ENDPOINTS
# =============================================================================

@router.get("/", response_model=AssignmentAnalysisResponse)
async def get_assignment_analysis(
    fecha_actual: Optional[date] = Query(None, description="Current period date (YYYY-MM-DD)"),
    fecha_anterior: Optional[date] = Query(None, description="Previous period date (YYYY-MM-DD)"),
    cartera: Optional[str] = Query(None, description="Filter by specific cartera"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
    cache_service: CacheService = Depends(get_cache_service)
):
    """
    Get assignment composition analysis with period comparison
    
    **Usage Examples:**
    
    - Current vs previous month: `/api/v1/assignment/`
    - Custom date comparison: `/api/v1/assignment/?fecha_actual=2025-06-01&fecha_anterior=2025-05-01`
    - Filter by cartera: `/api/v1/assignment/?cartera=TEMPRANA`
    
    **Response includes:**
    - Executive KPIs with period variations
    - Portfolio composition by cartera
    - Detailed breakdown with comparisons
    """
    controller = AssignmentController(dashboard_service, cache_service)
    
    return await controller.get_assignment_analysis(
        fecha_actual=fecha_actual,
        fecha_anterior=fecha_anterior,
        cartera_filter=cartera
    )


@router.get("/summary", response_model=Dict[str, Any])
async def get_assignment_summary(
    fecha_corte: Optional[date] = Query(None, description="Cut-off date for summary"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Get high-level assignment summary for quick overview
    
    Returns key assignment metrics without detailed breakdown
    """
    try:
        if fecha_corte is None:
            fecha_corte = date.today()
        
        # Get current dashboard data
        dashboard_data = await dashboard_service.get_dashboard_data(
            filters={},
            fecha_corte=fecha_corte
        )
        
        # Extract summary metrics
        totals = AssignmentController(dashboard_service, None)._extract_totals(dashboard_data)
        
        summary = {
            "fechaCorte": fecha_corte.isoformat(),
            "totalCuentas": totals['cuentas'],
            "totalClientes": totals['clientes'],
            "saldoTotal": totals['deudaAsig'],
            "ticketPromedio": totals['deudaAsig'] / max(totals['cuentas'], 1),
            "totalCarteras": len([
                item for item in dashboard_data.get('segmentoData', [])
                if item.get('name') != 'Total'
            ]),
            "timestamp": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get assignment summary: {str(e)}"
        )


@router.get("/health")
async def assignment_health_check(
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service)
):
    """
    Health check for assignment endpoints
    """
    try:
        health_status = await dashboard_service.health_check()
        
        return {
            "status": "healthy",
            "service": "assignment",
            "timestamp": datetime.now().isoformat(),
            "dashboard_service": health_status
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "service": "assignment", 
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )
