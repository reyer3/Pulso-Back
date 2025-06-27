"""
📊 Dashboard data models
Pydantic models for dashboard API responses
"""

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from app.models.base import BaseResponse, CacheInfo, Amount, Count, Percentage


# Request models
class DashboardFilters(BaseModel):
    """
    Dashboard filter parameters
    """
    cartera: Optional[List[str]] = Field(default=None, description="Cartera filter")
    servicio: Optional[List[str]] = Field(default=None, description="Servicio filter (MOVIL, FIJA)")
    periodo: Optional[List[str]] = Field(default=None, description="Periodo filter (YYYY-MM)")
    tramo: Optional[List[str]] = Field(default=None, description="Tramo filter")
    vencimiento: Optional[List[int]] = Field(default=None, description="Vencimiento filter")
    rango_deuda: Optional[List[str]] = Field(default=None, description="Rango deuda filter")
    date_from: Optional[date] = Field(default=None, description="Start date filter")
    date_to: Optional[date] = Field(default=None, description="End date filter")


class ChartDimensions(BaseModel):
    """
    Chart grouping dimensions
    """
    dimensions: List[str] = Field(
        default=["cartera"], 
        description="Grouping dimensions for charts"
    )


# Data models
class DataRow(BaseModel):
    """
    Main dashboard data row
    """
    id: str = Field(description="Unique identifier")
    name: str = Field(description="Display name")
    
    # Volume metrics
    cuentas: Count = Field(description="Total accounts")
    porcentaje_cuentas: Percentage = Field(description="Percentage of accounts")
    clientes: Count = Field(description="Total clients")
    
    # Debt metrics
    deuda_asig: Amount = Field(description="Assigned debt amount")
    porcentaje_deuda_asig: Percentage = Field(description="Percentage of assigned debt")
    deuda_actual: Amount = Field(description="Current debt amount")
    porcentaje_deuda_actual: Percentage = Field(description="Percentage of current debt")
    
    # Management metrics
    cobertura: Percentage = Field(description="Coverage percentage")
    contactabilidad: Percentage = Field(description="Contact rate")
    contacto_directo: Percentage = Field(description="Direct contact percentage")
    contacto_indirecto: Percentage = Field(description="Indirect contact percentage")
    sin_contacto: Percentage = Field(description="No contact percentage")
    
    # Performance metrics
    cierre: Percentage = Field(description="Closure rate")
    intensidad: float = Field(ge=0, description="Management intensity")
    conversion_pdp: Percentage = Field(description="PDP conversion rate")
    
    # Counters
    cuentas_cd: Count = Field(description="Direct contact accounts")
    cuentas_ci: Count = Field(description="Indirect contact accounts")
    cuentas_sc: Count = Field(description="No contact accounts")
    cuentas_sg: Count = Field(description="No management accounts")
    cuentas_pdp: Count = Field(description="PDP accounts")
    cuentas_pagadoras: Count = Field(description="Paying accounts")
    
    # Recovery metrics
    recupero: Amount = Field(description="Recovery amount")
    recupero_con_pdp: Amount = Field(description="Recovery with PDP")
    recupero_sin_pdp: Amount = Field(description="Recovery without PDP")
    
    # Additional info
    fecha_asignacion: Optional[date] = Field(description="Assignment date")
    fecha_cierre: Optional[date] = Field(description="Closure date")
    dias_gestion: Optional[int] = Field(description="Management days")
    estado_cartera: Optional[str] = Field(description="Portfolio status")


class TotalRow(BaseModel):
    """
    Total/summary row for dashboard
    """
    id: str = "total"
    name: str = "Total"
    
    # Same fields as DataRow but aggregated
    cuentas: Count
    porcentaje_cuentas: Percentage = 100.0
    clientes: Count
    deuda_asig: Amount
    porcentaje_deuda_asig: Percentage = 100.0
    deuda_actual: Amount
    porcentaje_deuda_actual: Percentage = 100.0
    cobertura: Percentage
    contactabilidad: Percentage
    contacto_directo: Percentage
    contacto_indirecto: Percentage
    sin_contacto: Percentage
    cierre: Percentage
    intensidad: float
    conversion_pdp: Percentage
    cuentas_cd: Count
    cuentas_ci: Count
    cuentas_sc: Count
    cuentas_sg: Count
    cuentas_pdp: Count
    cuentas_pagadoras: Count
    recupero: Amount
    recupero_con_pdp: Amount
    recupero_sin_pdp: Amount


class IntegralChartDataPoint(BaseModel):
    """
    Data point for integral KPI charts
    """
    name: str = Field(description="Dimension name")
    cobertura: Percentage = Field(description="Coverage rate")
    contactabilidad: Percentage = Field(description="Contact rate")
    contacto_directo: Percentage = Field(description="Direct contact rate")
    contacto_indirecto: Percentage = Field(description="Indirect contact rate")
    cierre: Percentage = Field(description="Closure rate")
    intensidad: float = Field(description="Management intensity")
    efectividad: Percentage = Field(description="Effectiveness rate")


# Response models
class DashboardData(BaseModel):
    """
    Complete dashboard data response
    """
    segmento_data: List[DataRow] = Field(description="Segment breakdown data")
    negocio_data: List[DataRow] = Field(description="Business line breakdown data")
    total_row: TotalRow = Field(description="Total summary row")
    integral_chart_data: List[IntegralChartDataPoint] = Field(description="Chart data points")
    
    # Metadata
    filters_applied: DashboardFilters = Field(description="Applied filters")
    chart_dimensions: List[str] = Field(description="Chart grouping dimensions")
    last_updated: datetime = Field(description="Last data update")
    record_count: int = Field(description="Total records processed")


class DashboardResponse(BaseResponse):
    """
    Dashboard API response
    """
    data: DashboardData = Field(description="Dashboard data")
    cache_info: Optional[CacheInfo] = Field(description="Cache information")
    query_time: Optional[float] = Field(description="Query execution time in seconds")


# Validation and summary models
class DashboardSummary(BaseModel):
    """
    Dashboard summary statistics
    """
    total_portfolios: int
    active_portfolios: int
    total_accounts: int
    total_assigned_debt: Amount
    total_current_debt: Amount
    total_recovery: Amount
    average_coverage: Percentage
    average_contact_rate: Percentage
    average_closure_rate: Percentage


class PortfolioStatus(BaseModel):
    """
    Portfolio status information
    """
    archivo: str
    estado: str  # ABIERTA, CERRADA, PROXIMA_A_CERRAR
    fecha_asignacion: date
    fecha_cierre: Optional[date]
    dias_restantes: Optional[int]
    cuentas: int
    deuda_asignada: Amount
    recupero_actual: Amount
    efectividad: Percentage