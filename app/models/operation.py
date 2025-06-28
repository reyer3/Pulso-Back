"""
📞 Modelos de Datos para Análisis de Operación
Modelos Pydantic que coinciden EXACTAMENTE con los tipos de Frontend OperationPage.
"""
# Imports estándar
from typing import List, Dict, Optional # Añadido Dict y Optional por si se usan en el futuro o en modelos no mostrados.

# Imports de terceros
from pydantic import BaseModel, Field

# Imports internos
from app.models.base import BaseResponse # Asumiendo que BaseResponse es relevante
from app.models.common import ChannelType, FrontendCompatibleModel


# =============================================================================
# MODELOS DE DATOS DE OPERACIÓN - COINCIDENCIA EXACTA CON FRONTEND
# =============================================================================

class OperationDayKPI(FrontendCompatibleModel):
    """
    KPI de operación diaria - Coincidencia EXACTA con la interfaz Frontend OperationDayKPI.
    """
    label: str = Field(description="Etiqueta del KPI")
    value: str = Field(description="Valor del KPI como cadena de texto")


class ChannelMetric(FrontendCompatibleModel):
    """
    Métricas de rendimiento del canal - Coincidencia EXACTA con la interfaz Frontend ChannelMetric.
    """
    channel: ChannelType = Field(description="Tipo de canal (Voicebot | Call Center)")
    calls: int = Field(description="Llamadas totales")
    effectiveContacts: int = Field(description="Contactos efectivos")
    nonEffectiveContacts: int = Field(description="Contactos no efectivos")
    pdp: int = Field(description="Promesas de pago")
    cierreRate: float = Field(description="Tasa de cierre porcentual")


class HourlyPerformance(FrontendCompatibleModel):
    """
    Datos de rendimiento por hora - Coincidencia EXACTA con la interfaz Frontend HourlyPerformance.
    """
    hour: str = Field(description="Hora en formato HH:MM (ej., '09:00')")
    effectiveContacts: int = Field(description="Contactos efectivos en esta hora")
    nonEffectiveContacts: int = Field(description="Contactos no efectivos en esta hora")
    pdp: int = Field(description="Promesas de pago en esta hora")


class AttemptEffectiveness(FrontendCompatibleModel):
    """
    Datos de efectividad por intento - Coincidencia EXACTA con la interfaz Frontend AttemptEffectiveness.
    """
    attempt: int = Field(description="Número de intento")
    cierreRate: float = Field(description="Tasa de cierre porcentual para este intento")


class QueuePerformance(FrontendCompatibleModel):
    """
    Datos de rendimiento de cola - Coincidencia EXACTA con la interfaz Frontend QueuePerformance.
    """
    queueName: str = Field(description="Nombre de la cola")
    calls: int = Field(description="Llamadas totales")
    effectiveContacts: int = Field(description="Contactos efectivos")
    pdp: int = Field(description="Promesas de pago")
    cierreRate: float = Field(description="Tasa de cierre porcentual")


# =============================================================================
# MODELOS DE RESPUESTA DE OPERACIÓN - COINCIDENCIA EXACTA CON FRONTEND
# =============================================================================

class OperationDayAnalysisData(FrontendCompatibleModel):
    """
    Análisis completo del día de operación - Coincidencia EXACTA con la interfaz Frontend OperationDayAnalysisData.
    """
    kpis: List[OperationDayKPI] = Field(description="KPIs diarios")
    channelPerformance: List[ChannelMetric] = Field(description="Métricas de rendimiento del canal")
    hourlyPerformance: List[HourlyPerformance] = Field(description="Desglose por hora")
    attemptEffectiveness: List[AttemptEffectiveness] = Field(description="Efectividad por intento")
    queuePerformance: List[QueuePerformance] = Field(description="Rendimiento de la cola")


class OperationDayAnalysisResponse(BaseResponse): # Este modelo existe pero no se usa en los endpoints actuales.
    """
    Respuesta de API para análisis de operación. (Actualmente no usado en endpoints de operation.py)
    """
    data: OperationDayAnalysisData = Field(description="Datos del análisis de operación")
    date: str = Field(description="Fecha del análisis") # Formato YYYY-MM-DD
    queryTime: float = Field(description="Tiempo de ejecución de la consulta en segundos")


# =============================================================================
# MODELOS DE SOLICITUD DE OPERACIÓN
# =============================================================================

class OperationDayRequest(BaseModel):
    """
    Modelo de solicitud para el análisis del día de operación.
    """
    date: str = Field(description="Fecha del análisis (YYYY-MM-DD)")
    includeHourlyBreakdown: bool = Field(
        default=True, description="Incluir rendimiento por hora"
    )
    includeQueueDetails: bool = Field(
        default=True, description="Incluir rendimiento de la cola"
    )
    includeAttemptAnalysis: bool = Field(
        default=True, description="Incluir efectividad por intento"
    )
    # Ejemplo de campo adicional que podría existir, si es necesario.
    # filtros: Optional[Dict[str, str]] = Field(None, description="Filtros adicionales para la consulta")


class OperationFilters(BaseModel): # Este modelo no se usa directamente en los endpoints actuales, pero es bueno tenerlo definido.
    """
    Filtros para el análisis de operación. (Actualmente no usado en endpoints de operation.py)
    """
    channels: List[ChannelType] = Field( # Usar ChannelType para consistencia
        default_factory=list, # Mejor que default=[] para listas mutables
        description="Canales a incluir (ej: ['Voicebot', 'Call Center'])"
    )
    queues: List[str] = Field(
        default_factory=list, description="Colas específicas a analizar"
    )
    hourRange: Optional[List[str]] = Field( # Hacer opcional si puede no estar presente
        default=None, # Ejemplo: default=["08:00", "20:00"] si siempre se espera
        description="Rango horario [inicio, fin] (ej: ['08:00', '20:00'])"
    )
    maxAttempts: Optional[int] = Field( # Hacer opcional
        default=None, # Ejemplo: default=5 si siempre se espera
        description="Número máximo de intentos a analizar"
    )


# =============================================================================
# MODELOS DE RESUMEN DE OPERACIÓN (Ejemplos adicionales, no en uso actual)
# =============================================================================

class OperationSummary(FrontendCompatibleModel):
    """
    Estadísticas de resumen de la operación. (Ejemplo, no en uso actual)
    """
    totalCalls: int = Field(description="Llamadas totales")
    totalEffectiveContacts: int = Field(description="Total de contactos efectivos")
    totalPdp: int = Field(description="Total de PDPs (Promesas de Pago)")
    overallCierreRate: float = Field(description="Tasa de cierre general")
    peakHour: Optional[str] = Field(None, description="Hora pico para contactos efectivos")
    bestChannel: Optional[ChannelType] = Field(None, description="Canal con mejor rendimiento")
    averageAttempts: Optional[float] = Field(None, description="Promedio de intentos por cierre")


class ChannelComparison(FrontendCompatibleModel):
    """
    Métricas de comparación de canales. (Ejemplo, no en uso actual)
    """
    voicebotMetrics: Optional[ChannelMetric] = Field(None, description="Métricas de rendimiento del Voicebot")
    callCenterMetrics: Optional[ChannelMetric] = Field(None, description="Métricas de rendimiento del Call Center")
    performanceDelta: Optional[Dict[str, float]] = Field(None, description="Diferencias de rendimiento (ej: {'cierreRate_diff': 10.5})")
    recommendation: Optional[str] = Field(None, description="Recomendación basada en el rendimiento")

# Asegurar que todos los modelos tengan `alias_generator` si se usa `by_alias=True` en FastAPI y los nombres de campo Python
# difieren de los nombres de campo JSON esperados (camelCase vs snake_case).
# from pydantic.alias_generators import to_camel
# class Config:
#     alias_generator = to_camel
#     allow_population_by_field_name = True
#
# Y luego añadir `Config = Config` a cada modelo Pydantic que lo necesite.
# Por ahora, los nombres de campo parecen ser consistentes con camelCase donde se espera (FrontendCompatibleModel).
