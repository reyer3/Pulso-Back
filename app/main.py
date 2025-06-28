# --- START OF FILE app/main.py ---
"""
🚀 Pulso-Back FastAPI Application
Punto de entrada principal para el servidor de la API, con gestión del ciclo de vida.
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
# from app.core.database import init_db, close_db  # ❌ REMOVIDO: Funciones no existen
from app.core.cache import cache as redis_cache
from app.core.logging import setup_logging
from app.core.middleware import TimingMiddleware, PrometheusMiddleware, SecurityMiddleware

# Configurar el logging tan pronto como sea posible
setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Gestiona los eventos de arranque y parada de la aplicación.
    """
    # --- LÓGICA DE ARRANQUE (STARTUP) ---
    logger.info("Iniciando la API Pulso-Back...")

    # Inicializar el pool de conexiones de Redis
    await redis_cache.init_redis()
    logger.info("Pool de conexiones de Redis inicializado.")

    # ❌ REMOVIDO: init_db ya no existe (refactorizado a asyncpg directo)
    # await init_db()  # Descomentar si necesitas crear tablas al inicio
    # logger.info("Base de datos inicializada.")

    # Iniciar el servidor de métricas de Prometheus
    if settings.PROMETHEUS_ENABLED:
        start_http_server(settings.PROMETHEUS_PORT)
        logger.info(f"Servidor de métricas Prometheus iniciado en el puerto {settings.PROMETHEUS_PORT}")

    logger.info("Arranque de la API Pulso-Back completado con éxito.")

    yield  # La aplicación se ejecuta aquí

    # --- LÓGICA DE PARADA (SHUTDOWN) ---
    logger.info("Deteniendo la API Pulso-Back...")

    # Cerrar el pool de conexiones de Redis
    await redis_cache.close()
    logger.info("Conexiones de Redis cerradas.")

    # ❌ REMOVIDO: close_db ya no existe (refactorizado a asyncpg directo)
    # await close_db()
    # logger.info("Conexiones de base de datos cerradas.")

    logger.info("Parada de la API Pulso-Back completada.")


def create_app() -> FastAPI:
    """
    Crea la instancia de la aplicación FastAPI con toda la configuración.
    """
    app = FastAPI(
        title=settings.API_TITLE,
        description="API + ETL Backend para Dashboard Cobranzas Telefónica",
        version=settings.API_VERSION,
        openapi_url=f"/api/{settings.API_VERSION}/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,  # <-- Aquí se conecta el gestor del ciclo de vida
    )

    # --- Middleware ---
    # El orden es importante: se ejecutan de abajo hacia arriba.
    app.add_middleware(SecurityMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    if settings.PROMETHEUS_ENABLED:
        app.add_middleware(PrometheusMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(TimingMiddleware) # Este middleware debe ser uno de los últimos

    # --- Rutas ---
    app.include_router(api_router, prefix=f"/api/{settings.API_VERSION}")

    return app

# Crear la instancia de la aplicación
app = create_app()

# Endpoints de salud en la raíz para simplicidad
@app.get("/health", tags=["Health"])
async def health_check():
    """Endpoint de salud para balanceadores de carga."""
    return {"status": "healthy", "service": "pulso-back", "version": settings.API_VERSION}

@app.get("/", tags=["Health"])
async def root():
    """Endpoint raíz."""
    return {"message": f"Bienvenido a {settings.API_TITLE}", "docs_url": "/docs"}

# --- Script de ejecución para desarrollo ---
def main():
    """Inicia el servidor Uvicorn para desarrollo."""
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level=settings.LOG_LEVEL.lower(),
    )

if __name__ == "__main__":
    main()
