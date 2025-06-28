"""
📊 Dashboard Data Models  
Pydantic models matching Frontend types EXACTLY
"""

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict

from app.models.base import BaseResponse, CacheInfo
from app.models.common import (
    IconStatus, 
    FrontendCompatibleModel,
    to_camel_case  # ✅ Import the function instead of field helpers
)


# =============================================================================
# REQUEST MODELS
# =============================================================================

class DashboardFilters(BaseModel):
    """Dashboard filter parameters"""
    cartera: Optional[List[str]] = None
    servicio: Optional[List[str]] = None  
    periodo: Optional[List[str]] = None
    tramo: Optional[List[str]] = None
    vencimiento: Optional[List[int]] = None
    rangoDeuda: Optional[List[str]] = None
    fechaInicio: Optional[date] = None
    fechaFin: Optional[date] = None


# =============================================================================
# DATA MODELS - EXACT FRONTEND MATCH
# =============================================================================

class DataRow(FrontendCompatibleModel):
    """
    Main dashboard data row - EXACT match with Frontend DataRow interface
    """
    id: str = Field(description="Unique identifier")
    name: str = Field(description="Display name")
    status: IconStatus = Field(description="Row status icon")
    
    # Volume metrics
    cuentas: int = Field(description="Total accounts")
    porcentajeCuentas: float = Field(description="Percentage of accounts")
    
    # Debt metrics  
    deudaAsig: float = Field(description="Assigned debt amount")
    porcentajeDeuda: float = Field(description="Percentage of assigned debt")
    porcentajeDeudaStatus: IconStatus = Field(description="Assigned debt status")
    deudaAct: float = Field(description="Current debt amount")
    porcentajeDeudaAct: float = Field(description="Percentage of current debt") 
    porcentajeDeudaActStatus: IconStatus = Field(description="Current debt status")
    
    # Management metrics
    cobertura: float = Field(description="Coverage percentage")
    contacto: float = Field(description="Contact rate")
    contactoStatus: IconStatus = Field(description="Contact status")
    cd: float = Field(description="Direct contact percentage")
    ci: float = Field(description="Indirect contact percentage") 
    sc: float = Field(description="No contact percentage")
    
    # Performance metrics
    cierre: float = Field(description="Closure rate")
    cierreStatus: IconStatus = Field(description="Closure status")
    inten: float = Field(description="Management intensity")
    intenStatus: IconStatus = Field(description="Intensity status")
    
    # Counters
    cdCount: int = Field(description="Direct contact accounts")
    ciCount: int = Field(description="Indirect contact accounts")
    scCount: int = Field(description="No contact accounts")
    sgCount: int = Field(description="No management accounts")
    pdpCount: int = Field(description="PDP accounts")
    fracCount: int = Field(description="Fractionation accounts")
    pdpFracCount: int = Field(description="PDP + Fractionation accounts")
    
    # Optional temporal info
    fechaAsignacion: Optional[str] = Field(description="Assignment date")
    fechaCierre: Optional[str] = Field(description="Closure date")
    diasGestion: Optional[int] = Field(description="Management days")
    diasHabiles: Optional[int] = Field(description="Business days")


class TotalRow(FrontendCompatibleModel):
    """
    Total/summary row - EXACT match with Frontend TotalRow interface
    """
    id: str = "total"
    name: str = "Total"
    status: IconStatus = IconStatus.NONE
    
    # Same fields as DataRow but aggregated
    cuentas: int
    porcentajeCuentas: float = 100.0
    deudaAsig: float
    porcentajeDeuda: float = 100.0
    porcentajeDeudaStatus: IconStatus = IconStatus.NONE
    deudaAct: float
    porcentajeDeudaAct: float = 100.0
    porcentajeDeudaActStatus: IconStatus = IconStatus.NONE
    cobertura: float
    contacto: float
    contactoStatus: IconStatus = IconStatus.NONE
    cd: float
    ci: float
    sc: float
    cierre: float
    cierreStatus: IconStatus = IconStatus.NONE
    inten: float
    intenStatus: IconStatus = IconStatus.NONE
    cdCount: int
    ciCount: int
    scCount: int
    sgCount: int
    pdpCount: int
    fracCount: int
    pdpFracCount: int
    fechaAsignacion: Optional[str] = None
    fechaCierre: Optional[str] = None
    diasGestion: Optional[int] = None
    diasHabiles: Optional[int] = None


class IntegralChartDataPoint(FrontendCompatibleModel):
    """
    Chart data point - EXACT match with Frontend IntegralChartDataPoint interface
    """
    name: str = Field(description="Dimension name")
    cobertura: float = Field(description="Coverage rate")
    contacto: float = Field(description="Contact rate")
    contactoDirecto: float = Field(description="Direct contact rate")
    contactoIndirecto: float = Field(description="Indirect contact rate")
    tasaDeCierre: float = Field(description="Closure rate")
    intensidad: float = Field(description="Management intensity")


# =============================================================================
# RESPONSE MODELS - EXACT FRONTEND MATCH
# =============================================================================

# TableData type alias - matches Frontend exactly
TableData = List[DataRow]

class DashboardData(FrontendCompatibleModel):
    """
    Complete dashboard data - EXACT match with Frontend DashboardData interface
    """
    segmentoData: TableData = Field(description="Segment breakdown data")
    negocioData: TableData = Field(description="Business line breakdown data")
    integralChartData: List[IntegralChartDataPoint] = Field(description="Chart data points")


class DashboardResponse(BaseResponse):
    """
    Dashboard API response with metadata
    """
    data: DashboardData = Field(description="Dashboard data")
    cache_info: Optional[CacheInfo] = None
    query_time: Optional[float] = None
    
    # ✅ V2: Use the consistent camelCase function
    model_config = ConfigDict(
        alias_generator=to_camel_case,
        populate_by_name=True
    )


# =============================================================================
# REQUEST MODELS FOR API ENDPOINTS
# =============================================================================

class DashboardRequest(BaseModel):
    """Dashboard request with filters and dimensions"""
    filters: Optional[DashboardFilters] = Field(default_factory=DashboardFilters)
    fechaCorte: Optional[date] = None
    dimensions: Optional[List[str]] = Field(default=["cartera"])


class DashboardHealthResponse(BaseModel):
    """Health check response for dashboard service"""
    status: str
    timestamp: str
    dataSource: dict
    processor: dict
