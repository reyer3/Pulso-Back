"""
🚀 Pulso-Back FastAPI Application
Main entry point for the API server
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_client import start_http_server

from app.api.v1.api import api_router
from app.core.config import settings
from app.core.database import init_db
from app.core.logging import setup_logging
from app.core.middleware import PrometheusMiddleware, TimingMiddleware

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan events
    """
    # Startup
    logger.info("Starting Pulso-Back API...")
    
    try:
        # Initialize database
        await init_db()
        logger.info("Database initialized")
        
        # Start Prometheus metrics server
        if settings.PROMETHEUS_ENABLED:
            start_http_server(settings.PROMETHEUS_PORT)
            logger.info(f"Prometheus metrics server started on port {settings.PROMETHEUS_PORT}")
        
        logger.info("Pulso-Back API started successfully")
        yield
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise
    finally:
        # Shutdown
        logger.info("Shutting down Pulso-Back API...")


def create_app() -> FastAPI:
    """
    Create FastAPI application with all configurations
    """
    app = FastAPI(
        title=settings.API_TITLE,
        description="API + ETL Backend para Dashboard Cobranzas Telefónica",
        version=settings.API_VERSION,
        openapi_url=f"/api/{settings.API_VERSION}/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(TimingMiddleware)
    
    if settings.PROMETHEUS_ENABLED:
        app.add_middleware(PrometheusMiddleware)

    # Routes
    app.include_router(api_router, prefix=f"/api/{settings.API_VERSION}")

    return app


# Create app instance
app = create_app()


@app.get("/health")
async def health_check():
    """
    Health check endpoint for load balancers
    """
    return {
        "status": "healthy",
        "service": "pulso-back",
        "version": settings.API_VERSION,
        "environment": settings.ENVIRONMENT,
    }


@app.get("/")
async def root():
    """
    Root endpoint
    """
    return {
        "message": "Pulso-Back API",
        "docs": "/docs",
        "health": "/health",
        "version": settings.API_VERSION,
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level=settings.LOG_LEVEL.lower(),
    )