"""
⚡ Custom middleware for FastAPI
Timing, Prometheus metrics, and request logging
"""

import time
import uuid
from typing import Callable

from fastapi import Request, Response
from prometheus_client import Counter, Histogram, Gauge
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.logging import get_logger, log_request_id

logger = get_logger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"]
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"]
)

ACTIVE_REQUESTS = Gauge(
    "http_requests_active",
    "Active HTTP requests"
)

CACHE_HITS = Counter(
    "cache_hits_total",
    "Total cache hits",
    ["cache_type"]
)

CACHE_MISSES = Counter(
    "cache_misses_total",
    "Total cache misses",
    ["cache_type"]
)

BIGQUERY_QUERIES = Counter(
    "bigquery_queries_total",
    "Total BigQuery queries",
    ["dataset", "view"]
)

BIGQUERY_DURATION = Histogram(
    "bigquery_query_duration_seconds",
    "BigQuery query duration in seconds",
    ["dataset", "view"]
)


class TimingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log request timing and add request ID
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate request ID
        request_id = str(uuid.uuid4())
        log_request_id(request_id)
        
        # Add request ID to headers
        request.state.request_id = request_id
        
        # Log request start
        start_time = time.time()
        logger.info(
            "Request started",
            method=request.method,
            url=str(request.url),
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            request_id=request_id,
        )
        
        # Process request
        try:
            response = await call_next(request)
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Log response
            logger.info(
                "Request completed",
                status_code=response.status_code,
                duration=duration,
                request_id=request_id,
            )
            
            # Add timing header
            response.headers["X-Process-Time"] = str(duration)
            response.headers["X-Request-ID"] = request_id
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                "Request failed",
                error=str(e),
                duration=duration,
                request_id=request_id,
            )
            raise


class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware to collect Prometheus metrics
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get endpoint path template
        endpoint = self._get_endpoint_name(request)
        method = request.method
        
        # Track active requests
        ACTIVE_REQUESTS.inc()
        
        # Start timing
        start_time = time.time()
        
        try:
            response = await call_next(request)
            status_code = response.status_code
            
        except Exception as e:
            status_code = 500
            logger.error(f"Request failed: {e}")
            raise
            
        finally:
            # Track request duration
            duration = time.time() - start_time
            REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)
            
            # Track request count
            REQUEST_COUNT.labels(
                method=method, 
                endpoint=endpoint, 
                status_code=status_code
            ).inc()
            
            # Decrease active requests
            ACTIVE_REQUESTS.dec()
        
        return response
    
    def _get_endpoint_name(self, request: Request) -> str:
        """
        Extract endpoint name from request
        """
        try:
            if hasattr(request, "scope") and "route" in request.scope:
                return request.scope["route"].path
            return request.url.path
        except Exception:
            return "unknown"


class SecurityMiddleware(BaseHTTPMiddleware):
    """
    Security headers middleware
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        return response


def track_cache_hit(cache_type: str) -> None:
    """
    Track cache hit metric
    """
    CACHE_HITS.labels(cache_type=cache_type).inc()


def track_cache_miss(cache_type: str) -> None:
    """
    Track cache miss metric
    """
    CACHE_MISSES.labels(cache_type=cache_type).inc()


def track_bigquery_query(dataset: str, view: str, duration: float) -> None:
    """
    Track BigQuery query metrics
    """
    BIGQUERY_QUERIES.labels(dataset=dataset, view=view).inc()
    BIGQUERY_DURATION.labels(dataset=dataset, view=view).observe(duration)