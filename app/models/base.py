"""
🏗️ Base Pydantic models
Shared models and utilities for API responses
"""

import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class BaseResponse(BaseModel):
    """
    Base response model with common fields
    """
    success: bool = True
    message: Optional[str] = None
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    request_id: Optional[str] = None


class ErrorResponse(BaseResponse):
    """
    Error response model
    """
    success: bool = False
    error_code: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class PaginatedResponse(BaseResponse):
    """
    Paginated response model
    """
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=1000)
    total_count: int = Field(ge=0)
    total_pages: int = Field(ge=0)
    has_next: bool
    has_prev: bool
    
    @validator('total_pages', pre=True, always=True)
    def calculate_total_pages(cls, v, values):
        total_count = values.get('total_count', 0)
        page_size = values.get('page_size', 1)
        return (total_count + page_size - 1) // page_size if total_count > 0 else 0
    
    @validator('has_next', pre=True, always=True)
    def calculate_has_next(cls, v, values):
        page = values.get('page', 1)
        total_pages = values.get('total_pages', 0)
        return page < total_pages
    
    @validator('has_prev', pre=True, always=True)
    def calculate_has_prev(cls, v, values):
        page = values.get('page', 1)
        return page > 1


class FilterRequest(BaseModel):
    """
    Base filter request model
    """
    filters: Dict[str, List[str]] = Field(default_factory=dict)
    date_from: Optional[datetime.date] = None
    date_to: Optional[datetime.date] = None
    
    @validator('date_to')
    def date_to_must_be_after_date_from(cls, v, values):
        date_from = values.get('date_from')
        if date_from and v and v < date_from:
            raise ValueError('date_to must be after date_from')
        return v


class CacheInfo(BaseModel):
    """
    Cache information model
    """
    cached: bool
    cache_key: Optional[str] = None
    cache_ttl: Optional[int] = None
    cached_at: Optional[datetime.datetime] = None


class HealthCheck(BaseModel):
    """
    Health check response model
    """
    status: str
    service: str
    version: str
    environment: str
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    uptime: Optional[float] = None
    dependencies: Optional[Dict[str, str]] = None


class MetricsInfo(BaseModel):
    """
    Metrics information model
    """
    total_requests: int
    average_response_time: float
    cache_hit_rate: float
    active_connections: int
    bigquery_queries: int


# Common field types
Percentage = Field(ge=0, le=100, description="Percentage value (0-100)")
Amount = Field(ge=0, description="Monetary amount")
Count = Field(ge=0, description="Count value")
PositiveFloat = Field(gt=0, description="Positive float value")


# Utility functions
def to_camel_case(string: str) -> str:
    """
    Convert snake_case to camelCase
    """
    components = string.split('_')
    return components[0] + ''.join(word.capitalize() for word in components[1:])


class CamelCaseModel(BaseModel):
    """
    Base model with camelCase field aliases
    """
    
    class Config:
        alias_generator = to_camel_case
        allow_population_by_field_name = True


# Response wrapper
def success_response(
    data: Any = None, 
    message: str = None, 
    **kwargs
) -> Dict[str, Any]:
    """
    Create success response
    """
    response = {
        "success": True,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        **kwargs
    }
    
    if data is not None:
        response["data"] = data
    
    if message:
        response["message"] = message
    
    return response


def error_response(
    message: str, 
    error_code: str = None, 
    details: Dict[str, Any] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Create error response
    """
    response = {
        "success": False,
        "message": message,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        **kwargs
    }
    
    if error_code:
        response["error_code"] = error_code
    
    if details:
        response["details"] = details
    
    return response