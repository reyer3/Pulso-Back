"""
🎯 Modelos de Datos para Análisis de Productividad
Modelos Pydantic que coinciden con los tipos de Frontend ProductivityPage y las necesidades del endpoint.
"""

# Imports estándar
from datetime import date # Necesario para el tipo de fecha_inicio y fecha_fin en ProductivityRequest
from typing import Any, Dict, List, Optional

# Imports de terceros
from pydantic import BaseModel, Field

# Imports internos
from app.models.base import BaseResponse # Usado por el ProductivityResponse unificado
from app.models.common import FrontendCompatibleModel, HeatmapMetric


# =============================================================================
# MODELOS DE SOLICITUD DE PRODUCTIVIDAD (Unificados y basados en el endpoint)
# =============================================================================

class ProductivityRequest(BaseModel):
    """
    Modelo de solicitud para el análisis de productividad.
    Combina campos del modelo inline del endpoint y el modelo del ticket.
    """
    fecha_inicio: Optional[date] = Field(None, description="Fecha de inicio para el análisis (YYYY-MM-DD)")
    fecha_fin: Optional[date] = Field(None, description="Fecha de fin para el análisis (YYYY-MM-DD)")
    agente: Optional[str] = Field(None, description="Identificador o nombre del agente (opcional, del ticket)")
    filtros: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Criterios de filtro (del endpoint inline, tipo Any para flexibilidad)"
    )
    metric_type: Optional[str] = Field(
        default="gestiones",
        description="Tipo de métrica para el heatmap (del endpoint inline)"
    )
    # Campos del modelo ProductivityRequest en app/models/productivity.py (original)
    # que podrían ser útiles o necesitar integración si son distintos:
    # includeHeatmap: bool = Field(default=True, description="Incluir heatmap de agente")
    # includeRanking: bool = Field(default=True, description="Incluir ranking de agente")
    # includeTrends: bool = Field(default=True, description="Incluir análisis de tendencias")
    # maxAgents: int = Field(default=50, description="Máximo de agentes a incluir")

    class Config:
        anystr_strip_whitespace = True


# =============================================================================
# MODELOS DE DATOS DE PRODUCTIVIDAD (Reutilizados/unificados desde el endpoint y models.py)
# =============================================================================

class AgentDailyPerformance(FrontendCompatibleModel):
    """
    Datos de rendimiento diario del agente.
    (Coincide con el modelo inline y el de app/models/productivity.py)
    """
    gestiones: Optional[int] = Field(None, description="Número de gestiones realizadas")
    contactosEfectivos: Optional[int] = Field(None, description="Número de contactos efectivos")
    compromisos: Optional[int] = Field(None, description="Número de compromisos/promesas de pago")


class AgentHeatmapRow(FrontendCompatibleModel):
    """
    Fila de datos para el heatmap de agentes.
    (Coincide con el modelo inline y el de app/models/productivity.py)
    """
    id: str = Field(description="Identificador único del agente")
    dni: str = Field(description="DNI del agente")
    agentName: str = Field(description="Nombre completo del agente")
    dailyPerformance: Dict[int, Optional[AgentDailyPerformance]] = Field(
        description="Rendimiento diario por número de día"
    )


class ProductivityTrendPoint(FrontendCompatibleModel):
    """
    Punto de datos para la tendencia de productividad.
    (Coincide con el modelo inline y el de app/models/productivity.py)
    """
    day: Optional[int] = Field(None, description="Número del día (para tendencia diaria)")
    hour: Optional[str] = Field(None, description="Cadena de hora (para tendencia horaria, ej: '09:00')")
    llamadas: int = Field(description="Número de llamadas")
    compromisos: int = Field(description="Número de compromisos")
    recupero: Optional[float] = Field(None, description="Monto recuperado (solo para tendencia diaria)")


class AgentRankingRow(FrontendCompatibleModel):
    """
    Fila de datos para el ranking de agentes.
    (Coincide con el modelo inline y el de app/models/productivity.py)
    """
    id: str = Field(description="Identificador único del agente")
    rank: int = Field(description="Posición en el ranking del agente")
    agentName: str = Field(description="Nombre completo del agente")
    calls: int = Field(description="Total de llamadas realizadas")
    directContacts: int = Field(description="Contactos directos logrados")
    commitments: int = Field(description="Compromisos obtenidos")
    amountRecovered: float = Field(description="Monto recuperado")
    closingRate: float = Field(description="Porcentaje de tasa de cierre")
    commitmentConversion: float = Field(description="Porcentaje de conversión de compromisos")
    quartile: int = Field(description="Cuartil de rendimiento (1-4)", ge=1, le=4)


class ProductivityDetail(BaseModel): # Modelo del ticket
    """
    Detalle de productividad por agente, según el ticket.
    Este modelo es más simple que AgentRankingRow. Se mantendrá por si es usado en otro contexto
    o si la lógica de negocio lo requiere específicamente.
    """
    agente: str = Field(description="Nombre o identificador del agente")
    gestiones: int = Field(description="Número de gestiones")
    pagos: int = Field(description="Número de pagos conseguidos") # Asumo que "pagos" es diferente a "compromisos"
    efectividad: float = Field(description="Tasa de efectividad (ej. pagos/gestiones)")
    ranking: int = Field(description="Posición en el ranking")


# =============================================================================
# MODELOS DE RESPUESTA DE PRODUCTIVIDAD (Unificados)
# =============================================================================

class ProductivityData(FrontendCompatibleModel):
    """
    Datos completos de productividad.
    (Este es el `data` dentro del `ProductivityResponse` de `app/models/productivity.py`)
    """
    dailyTrend: List[ProductivityTrendPoint] = Field(description="Datos de tendencia diaria")
    hourlyTrend: List[ProductivityTrendPoint] = Field(description="Datos de tendencia horaria")
    agentRanking: List[AgentRankingRow] = Field(description="Datos del ranking de agentes")
    agentHeatmap: List[AgentHeatmapRow] = Field(description="Datos del heatmap de agentes")
    # El modelo inline `ProductivityResponse` no tiene `metadata`, pero el de `app/models` sí.
    # Se omite aquí para que coincida con el `response_model` del endpoint, que es más simple.


class ProductivityResponse(BaseModel): # Modelo de respuesta principal para el endpoint
    """
    Modelo de respuesta para el análisis de productividad, basado en el endpoint inline.
    Este es el que se usará como `response_model` en el endpoint.
    """
    dailyTrend: List[ProductivityTrendPoint] = Field(description="Tendencia de productividad diaria")
    hourlyTrend: List[ProductivityTrendPoint] = Field(description="Tendencia de productividad horaria")
    agentRanking: List[AgentRankingRow] = Field(description="Ranking de agentes")
    agentHeatmap: List[AgentHeatmapRow] = Field(description="Heatmap de rendimiento de agentes")
    metadata: Dict[str, Any] = Field(description="Metadatos de la respuesta")


class ProductivityTicketResponse(BaseModel): # Modelo de respuesta del ticket
    """
    Modelo de respuesta para el análisis de productividad, según el ticket.
    Se mantiene por si es necesario para una lógica de negocio específica o un endpoint diferente.
    """
    total_agentes: int = Field(description="Número total de agentes")
    promedio_efectividad: float = Field(description="Promedio de efectividad general")
    detalles: List[ProductivityDetail] = Field(description="Lista de detalles de productividad por agente")


# ---------------------------------------------------------------------------
# Otros modelos de app/models/productivity.py que no están directamente en los endpoints
# pero podrían ser relevantes para la lógica de servicio o para otros endpoints.
# Se mantienen aquí para consolidación, comentados si no se usan activamente en este refactor.
# ---------------------------------------------------------------------------

# class ProductivityResponseWithBase(BaseResponse): # El que estaba en app/models
#     """
#     Respuesta de API para análisis de productividad, usando BaseResponse.
#     """
#     data: ProductivityData = Field(description="Datos del análisis de productividad")
#     dateRange: Optional[Dict[str, str]] = Field(None, description="Rango de fechas del análisis")
#     queryTime: Optional[float] = Field(None, description="Tiempo de ejecución de la consulta en segundos")


class ProductivityFilters(BaseModel):
    """
    Filtros para el análisis de productividad.
    (Modelo de app/models/productivity.py)
    """
    agents: List[str] = Field(default_factory=list, description="IDs de agentes específicos a incluir")
    teams: List[str] = Field(default_factory=list, description="Nombres de equipos a incluir")
    metrics: List[HeatmapMetric] = Field(
        default_factory=lambda: [HeatmapMetric.GESTIONES, HeatmapMetric.CONTACTOS_EFECTIVOS, HeatmapMetric.COMPROMISOS],
        description="Métricas a incluir en el análisis"
    )
    quartiles: List[int] = Field(default_factory=lambda: [1, 2, 3, 4], description="Cuartiles a incluir")
    minCalls: int = Field(default=10, description="Mínimo de llamadas para inclusión")


class ProductivitySummary(FrontendCompatibleModel):
    """
    Estadísticas de resumen de productividad.
    (Modelo de app/models/productivity.py)
    """
    totalAgents: int = Field(description="Total de agentes analizados")
    activeAgents: int = Field(description="Agentes activos en el período")
    totalCalls: int = Field(description="Total de llamadas realizadas")
    totalCommitments: int = Field(description="Total de compromisos")
    totalRecovery: float = Field(description="Monto total recuperado")
    averageCallsPerAgent: float = Field(description="Promedio de llamadas por agente")
    averageCommitmentsPerAgent: float = Field(description="Promedio de compromisos por agente")
    topPerformerAgent: Optional[str] = Field(None, description="Nombre del agente con mejor rendimiento")
    conversionRate: float = Field(description="Tasa de conversión general de compromisos")


class AgentPerformanceDetail(FrontendCompatibleModel):
    """
    Información detallada del rendimiento del agente.
    (Modelo de app/models/productivity.py)
    """
    agentId: str = Field(description="Identificador único del agente")
    agentName: str = Field(description="Nombre completo del agente")
    dni: str = Field(description="DNI del agente")
    team: Optional[str] = Field(None, description="Nombre del equipo")
    totalCalls: int = Field(description="Total de llamadas")
    effectiveContacts: int = Field(description="Contactos efectivos")
    commitments: int = Field(description="Compromisos obtenidos")
    recoveryAmount: float = Field(description="Monto recuperado")
    contactRate: float = Field(description="Porcentaje de tasa de contacto")
    conversionRate: float = Field(description="Tasa de conversión de compromisos")
    closingRate: float = Field(description="Tasa de cierre")
    rank: int = Field(description="Ranking actual")
    quartile: int = Field(description="Cuartil de rendimiento")
    trendDirection: Optional[str] = Field(None, description="Tendencia de rendimiento (sube/baja/estable)")
    bestDay: Optional[int] = Field(None, description="Día de mejor rendimiento (número del mes)")
    bestHour: Optional[str] = Field(None, description="Hora de mejor rendimiento (HH:MM)")


class TeamPerformance(FrontendCompatibleModel):
    """
    Agregación del rendimiento del equipo.
    (Modelo de app/models/productivity.py)
    """
    teamName: str = Field(description="Nombre del equipo")
    totalAgents: int = Field(description="Número de agentes en el equipo")
    activeAgents: int = Field(description="Agentes activos en el período")
    totalCalls: int = Field(description="Total de llamadas del equipo")
    totalCommitments: int = Field(description="Total de compromisos del equipo")
    teamRecovery: float = Field(description="Monto recuperado por el equipo")
    teamContactRate: float = Field(description="Tasa de contacto del equipo")
    teamConversionRate: float = Field(description="Tasa de conversión del equipo")
    teamRank: Optional[int] = Field(None, description="Ranking del equipo")
    topAgent: Optional[str] = Field(None, description="Agente principal en el equipo")


class HourlyProductivityBreakdown(FrontendCompatibleModel):
    """
    Desglose de productividad por hora.
    (Modelo de app/models/productivity.py)
    """
    hour: str = Field(description="Hora (HH:MM)")
    totalCalls: int = Field(description="Total de llamadas en la hora")
    totalCommitments: int = Field(description="Total de compromisos en la hora")
    averagePerAgent: Optional[float] = Field(None, description="Promedio de llamadas por agente en la hora")
    conversionRate: Optional[float] = Field(None, description="Tasa de conversión de la hora")
    activeAgents: Optional[int] = Field(None, description="Agentes activos en la hora")
    efficiency: Optional[float] = Field(None, description="Puntuación de eficiencia de la hora")
