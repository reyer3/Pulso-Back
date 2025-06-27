"""
🔌 API V1 Router
Main API router that includes all endpoint modules
"""

from fastapi import APIRouter

from app.api.v1.endpoints.dashboard import router as dashboard_router

# Create main API router
api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(dashboard_router, tags=["dashboard"])

# Add health check endpoint at API level
@api_router.get("/health")
async def api_health():
    """
    API-level health check
    """
    return {
        "status": "healthy",
        "api_version": "v1",
        "endpoints": {
            "dashboard": "/dashboard",
            "evolution": "/evolution", 
            "filters": "/filters",
            "refresh": "/refresh"
        }
    }
