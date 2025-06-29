"""
📦 Base Models
Core Pydantic models for common responses and utilities.
"""

from typing import Any, Dict, Generic, List, Optional, TypeVar, Literal
from pydantic import BaseModel, Field
from app.models.common import FrontendCompatibleModel

# =============================================================================
# GENERIC RESPONSES
# =============================================================================

T = TypeVar('T')

class BaseResponse(FrontendCompatibleModel, Generic[T]):
    """Base response model for API endpoints."""
    status: str = Field(..., description="Status of the response (e.g., 'success', 'error')")
    message: str = Field(..., description="A human-readable message about the response")
    data: Optional[T] = Field(None, description="The actual data payload of the response")

class ErrorResponse(BaseResponse):
    """Error response model."""
    status: Literal["error"] = "error"
    code: Optional[int] = Field(None, description="Error code, if applicable")
    details: Optional[Any] = Field(None, description="Additional error details")

class PaginatedResponse(BaseResponse, Generic[T]):
    """Paginated response model."""
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
    total_items: int = Field(..., description="Total number of items across all pages")
    total_pages: int = Field(..., description="Total number of pages")
    data: List[T] = Field(..., description="List of items for the current page")

# =============================================================================
# REQUEST MODELS
# =============================================================================

class FilterRequest(BaseModel):
    """Base model for filtering requests."""
    periodo: Optional[str] = Field(None, description="Filter by period (YYYY-MM)")
    cartera: Optional[List[str]] = Field(None, description="Filter by portfolio(s)")
    servicio: Optional[List[str]] = Field(None, description="Filter by service(s)")
    fecha_inicio: Optional[str] = Field(None, description="Start date for filtering (YYYY-MM-DD)")
    fecha_fin: Optional[str] = Field(None, description="End date for filtering (YYYY-MM-DD)")
    channel: Optional[List[str]] = Field(None, description="Filter by channel(s)")
    agent_id: Optional[List[str]] = Field(None, description="Filter by agent ID(s)")

# =============================================================================
# HEALTH CHECK AND METRICS MODELS
# =============================================================================

class CacheInfo(FrontendCompatibleModel):
    """Information about the cache status."""
    status: str = Field(..., description="Cache status (e.g., 'connected', 'disconnected')")
    last_updated: Optional[str] = Field(None, description="Last update timestamp")
    hits: Optional[int] = Field(None, description="Number of cache hits")
    misses: Optional[int] = Field(None, description="Number of cache misses")

class HealthCheck(FrontendCompatibleModel):
    """Health check response model."""
    status: str = Field(..., description="Overall service status (e.g., 'healthy', 'degraded')")
    database_status: str = Field(..., description="Database connection status")
    cache_status: CacheInfo = Field(..., description="Cache service status")
    message: str = Field(..., description="Detailed health message")
    timestamp: str = Field(..., description="Timestamp of the health check")

class MetricsInfo(FrontendCompatibleModel):
    """General metrics information."""
    total_requests: int = Field(..., description="Total API requests")
    avg_response_time_ms: float = Field(..., description="Average response time in milliseconds")
    error_rate: float = Field(..., description="Percentage of requests resulting in errors")

# =============================================================================
# UTILITY FUNCTIONS FOR RESPONSES
# =============================================================================

def success_response(data: Any, message: str = "Operation successful") -> Dict[str, Any]:
    """Helper to create a success response."""
    return {"status": "success", "message": message, "data": data}

def error_response(message: str = "An error occurred", code: Optional[int] = None, details: Optional[Any] = None) -> Dict[str, Any]:
    """Helper to create an error response."""
    response = {"status": "error", "message": message}
    if code:
        response["code"] = code
    if details:
        response["details"] = details
    return response
