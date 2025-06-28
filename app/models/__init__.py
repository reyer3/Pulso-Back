"""
📦 Models Package Initialization
Export all Pydantic models for easy import across the application
"""

# =============================================================================
# BASE MODELS
# =============================================================================
from app.models.base import (
    BaseResponse,
    ErrorResponse,
    PaginatedResponse,
    FilterRequest,
    CacheInfo,
    HealthCheck,
    MetricsInfo,
    success_response,
    error_response
)

# =============================================================================
# COMMON TYPES AND ENUMS
# =============================================================================
from app.models.common import (
    IconStatus,
    ValueType,
    TableStatus,
    Page,
    ChartDimension,
    HeatmapMetric,
    ChannelType,
    KPI,
    ChartDataPoint,
    TableRow,
    FilterOption,
    FilterOptions,
    FrontendCompatibleModel,
    to_camel_case
)

# =============================================================================
# DASHBOARD MODELS
# =============================================================================
from app.models.dashboard import (
    DashboardFilters,
    DataRow,
    TotalRow,
    IntegralChartDataPoint,
    TableData,
    DashboardData,
    DashboardResponse,
    DashboardRequest,
    DashboardHealthResponse
)

# =============================================================================
# EVOLUTION MODELS
# =============================================================================
from app.models.evolution import (
    MetricConfigItem,
    EvolutionDataPoint,
    EvolutionSeries,
    EvolutionMetric,
    EvolutionData,
    EvolutionResponse,
    EvolutionRequest,
    EvolutionFilters,
    EvolutionSummary,
    MetricEvolutionDetail,
    CarteraEvolutionComparison,
    DailyEvolutionSnapshot
)

# =============================================================================
# ASSIGNMENT MODELS
# =============================================================================
from app.models.assignment import (
    AssignmentKPI,
    CompositionDataPoint,
    DetailBreakdownRow,
    AssignmentAnalysisData,
    AssignmentAnalysisResponse,
    AssignmentAnalysisRequest,
    AssignmentFilters,
    AssignmentSummary,
    PortfolioComparison,
    PeriodComparison,
    CompositionAnalysis
)

# =============================================================================
# OPERATION MODELS
# =============================================================================
from app.models.operation import (
    OperationDayKPI,
    ChannelMetric,
    HourlyPerformance,
    AttemptEffectiveness,
    QueuePerformance,
    OperationDayAnalysisData,
    OperationDayAnalysisResponse,
    OperationDayRequest,
    OperationFilters,
    OperationSummary,
    ChannelComparison
)

# =============================================================================
# PRODUCTIVITY MODELS
# =============================================================================
from app.models.productivity import (
    AgentDailyPerformance,
    AgentHeatmapRow,
    ProductivityTrendPoint,
    AgentRankingRow,
    ProductivityData,
    ProductivityResponse,
    ProductivityRequest,
    ProductivityFilters,
    ProductivitySummary,
    AgentPerformanceDetail,
    TeamPerformance,
    HourlyProductivityBreakdown
)

# =============================================================================
# GROUPED EXPORTS FOR EASY ACCESS
# =============================================================================

# All response models (for FastAPI endpoints)
RESPONSE_MODELS = [
    DashboardResponse,
    EvolutionResponse,
    AssignmentAnalysisResponse,
    OperationDayAnalysisResponse,
    ProductivityResponse,
    BaseResponse,
    ErrorResponse,
    PaginatedResponse,
    HealthCheck
]

# All request models (for FastAPI endpoints)
REQUEST_MODELS = [
    DashboardRequest,
    EvolutionRequest,
    AssignmentAnalysisRequest,
    OperationDayRequest,
    ProductivityRequest,
    FilterRequest
]

# All data models (core business entities)
DATA_MODELS = [
    DataRow,
    TotalRow,
    IntegralChartDataPoint,
    DashboardData,
    EvolutionMetric,
    EvolutionSeries,
    EvolutionDataPoint,
    AssignmentKPI,
    CompositionDataPoint,
    DetailBreakdownRow,
    OperationDayKPI,
    ChannelMetric,
    HourlyPerformance,
    AgentDailyPerformance,
    AgentHeatmapRow,
    ProductivityTrendPoint,
    AgentRankingRow
]

# All enum types
ENUM_TYPES = [
    IconStatus,
    ValueType,
    TableStatus,
    Page,
    ChartDimension,
    HeatmapMetric,
    ChannelType
]

# Filter models
FILTER_MODELS = [
    DashboardFilters,
    EvolutionFilters,
    AssignmentFilters,
    OperationFilters,
    ProductivityFilters,
    FilterOptions
]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_all_models():
    """Get all model classes for documentation or validation"""
    return {
        'response_models': RESPONSE_MODELS,
        'request_models': REQUEST_MODELS,
        'data_models': DATA_MODELS,
        'enum_types': ENUM_TYPES,
        'filter_models': FILTER_MODELS
    }

def get_frontend_compatible_models():
    """Get all models that inherit from FrontendCompatibleModel"""
    return [
        model for model in DATA_MODELS 
        if issubclass(model, FrontendCompatibleModel)
    ]

# =============================================================================
# VERSION AND METADATA
# =============================================================================

__version__ = "1.0.0"
__description__ = "Pydantic models with exact Frontend TypeScript compatibility"
__author__ = "Pulso-Back Team"

# Export version info
__all__ = [
    # Version info
    "__version__",
    "__description__",
    "__author__",
    
    # Utility functions
    "get_all_models",
    "get_frontend_compatible_models",
    "to_camel_case",
    "success_response", 
    "error_response",
    
    # Base models
    "BaseResponse",
    "ErrorResponse", 
    "PaginatedResponse",
    "FilterRequest",
    "CacheInfo",
    "HealthCheck",
    "FrontendCompatibleModel",
    
    # Enums
    "IconStatus",
    "ValueType", 
    "TableStatus",
    "Page",
    "ChartDimension",
    "HeatmapMetric",
    "ChannelType",
    
    # Common models
    "KPI",
    "ChartDataPoint",
    "TableRow",
    "FilterOptions",
    
    # Dashboard models  
    "DataRow",
    "TotalRow",
    "IntegralChartDataPoint",
    "DashboardData",
    "DashboardResponse",
    "DashboardRequest",
    
    # Evolution models
    "EvolutionDataPoint",
    "EvolutionSeries", 
    "EvolutionMetric",
    "EvolutionData",
    "EvolutionResponse",
    
    # Assignment models
    "AssignmentKPI",
    "CompositionDataPoint",
    "DetailBreakdownRow", 
    "AssignmentAnalysisData",
    "AssignmentAnalysisResponse",
    
    # Operation models
    "OperationDayKPI",
    "ChannelMetric",
    "HourlyPerformance",
    "AttemptEffectiveness",
    "QueuePerformance",
    "OperationDayAnalysisData",
    "OperationDayAnalysisResponse",
    
    # Productivity models
    "AgentDailyPerformance",
    "AgentHeatmapRow", 
    "ProductivityTrendPoint",
    "AgentRankingRow",
    "ProductivityData",
    "ProductivityResponse",
    
    # Grouped exports
    "RESPONSE_MODELS",
    "REQUEST_MODELS", 
    "DATA_MODELS",
    "ENUM_TYPES",
    "FILTER_MODELS"
]
