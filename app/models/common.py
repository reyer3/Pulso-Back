"""
🔧 Common Types and Enums
Shared types used across all models - matches Frontend exactly
"""

from enum import Enum
from typing import Dict, List, Union
from pydantic import BaseModel, Field


# =============================================================================
# ENUMS - EXACT MATCH WITH FRONTEND
# =============================================================================

class IconStatus(str, Enum):
    """Icon status types - matches Frontend IconStatus"""
    UP = "up"
    DOWN = "down" 
    NEUTRAL = "neutral"
    WARNING = "warning"
    OK = "ok"
    BAD = "bad"
    NONE = "none"


class ValueType(str, Enum):
    """Value types for metrics - matches Frontend ValueType"""
    PERCENT = "percent"
    CURRENCY = "currency"
    NUMBER = "number"


class TableStatus(str, Enum):
    """Table row status - matches Frontend TableStatus"""
    PAID = "Paid"
    PENDING = "Pending" 
    OVERDUE = "Overdue"


class Page(str, Enum):
    """Page navigation - matches Frontend Page"""
    DASHBOARD = "dashboard"
    EVOLUTIVOS = "evolutivos"
    OPERACION = "operacion"
    COMPOSICION = "composicion"
    PRODUCTIVIDAD = "productividad"


class ChartDimension(str, Enum):
    """Chart dimensions - matches Frontend ChartDimension"""
    CARTERA = "cartera"
    PERIODO = "periodo"
    NEGOCIO = "negocio"
    TIPO_SEGMENTO = "tipoSegmento"
    VENCIMIENTO = "vencimiento"


class HeatmapMetric(str, Enum):
    """Heatmap metrics - matches Frontend HeatmapMetric"""
    GESTIONES = "gestiones"
    CONTACTOS_EFECTIVOS = "contactosEfectivos"
    COMPROMISOS = "compromisos"


class ChannelType(str, Enum):
    """Channel types - matches Frontend ChannelMetric.channel"""
    VOICEBOT = "Voicebot"
    CALL_CENTER = "Call Center"


# =============================================================================
# COMMON MODELS - EXACT MATCH WITH FRONTEND
# =============================================================================

class KPI(BaseModel):
    """A single KPI for top-level cards - matches Frontend KPI exactly"""
    title: str = Field(description="KPI title")
    value: str = Field(description="KPI value as string")
    change: float = Field(description="Change percentage") 
    icon: str = Field(description="Icon name")
    color: str = Field(description="Color theme", regex="^(blue|green|yellow|purple|red|gray)$")


class ChartDataPoint(BaseModel):
    """Single data point for charts - matches Frontend ChartDataPoint exactly"""
    name: str = Field(description="Data point name")
    value: float = Field(description="Data point value")


class TableRow(BaseModel):
    """Single row for data tables - matches Frontend TableRow exactly"""
    id: Union[str, int] = Field(description="Unique identifier")
    customerName: str = Field(description="Customer name")
    cartera: str = Field(description="Portfolio name")
    debtAmount: float = Field(description="Debt amount")
    daysOverdue: int = Field(description="Days overdue")
    status: TableStatus = Field(description="Row status")


class FilterOption(BaseModel):
    """Single filter option - matches Frontend FilterOptions structure"""
    value: str = Field(description="Filter value")
    label: str = Field(description="Display label")


class FilterOptions(BaseModel):
    """Filter options structure - matches Frontend FilterOptions exactly"""
    __root__: Dict[str, List[FilterOption]] = Field(
        description="Filter options by category"
    )


# =============================================================================
# COMMON FIELD VALIDATORS
# =============================================================================

# Type aliases for common validations
Percentage = Field(ge=0, le=100, description="Percentage value (0-100)")
Amount = Field(ge=0, description="Monetary amount") 
Count = Field(ge=0, description="Count value")
PositiveFloat = Field(gt=0, description="Positive float value")
Rate = Field(ge=0, le=100, description="Rate as percentage")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def to_camel_case(string: str) -> str:
    """
    Convert snake_case to camelCase for Frontend compatibility
    """
    components = string.split('_')
    return components[0] + ''.join(word.capitalize() for word in components[1:])


class FrontendCompatibleModel(BaseModel):
    """
    Base model with camelCase aliases for perfect Frontend compatibility
    """
    
    class Config:
        # Auto-generate camelCase aliases for all fields
        alias_generator = to_camel_case
        # Allow using both snake_case and camelCase field names
        allow_population_by_field_name = True
        # Use enum values in serialization
        use_enum_values = True
