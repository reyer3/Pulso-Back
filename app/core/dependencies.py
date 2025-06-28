"""
💧 Contenedor de Inyección de Dependencias (DI Container)
Centraliza la creación y provisión de recursos, repositorios y servicios.
"""
from typing import Annotated
from fastapi import Depends
import redis.asyncio as redis

from app.core.config import settings
from app.core.cache import cache as redis_cache_manager
from app.repositories.data_adapters import DataSourceFactory, DataSourceAdapter
from app.repositories.cache_repo import CacheRepository
from app.repositories.bigquery_repo import BigQueryRepository
from app.repositories.postgres_repo import PostgresRepository
from app.services.postgres_service import PostgresService
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService

# -------------------------------------------------------------------
# Recursos básicos y Adapters
# -------------------------------------------------------------------

async def get_redis_client() -> redis.Redis:
    """Devuelve el cliente Redis, inicializándolo si es necesario."""
    if not redis_cache_manager.redis:
        await redis_cache_manager.init_redis()
    return redis_cache_manager.redis

def get_data_adapter() -> DataSourceAdapter:
    """Devuelve el adapter de datos según configuración (BigQuery/Postgres)."""
    return DataSourceFactory.create_adapter()

def get_postgres_service() -> PostgresService:
    """Devuelve el servicio de bajo nivel para PostgreSQL."""
    return PostgresService(settings.POSTGRES_URL)

# -------------------------------------------------------------------
# Repositorios (inyectan servicios/adapters)
# -------------------------------------------------------------------

def get_cache_repo() -> CacheRepository:
    """Repo de cache (Redis)."""
    return CacheRepository()

def get_bigquery_repo() -> BigQueryRepository:
    """Repo especializado para BigQuery."""
    return BigQueryRepository()

def get_postgres_repo(
    pg_service: PostgresService = Depends(get_postgres_service)
) -> PostgresRepository:
    """Repo especializado para Postgres."""
    return PostgresRepository(pg_service)

# -------------------------------------------------------------------
# Servicios de dominio (inyectan repositorios/adapters)
# -------------------------------------------------------------------

def get_cache_service(
    client: redis.Redis = Depends(get_redis_client)
) -> CacheService:
    """Servicio de caché (envuelve Redis)."""
    return CacheService(redis_client=client)

def get_dashboard_service(
    data_adapter: DataSourceAdapter = Depends(get_data_adapter)
) -> DashboardServiceV2:
    """Servicio de dashboard desacoplado de la BD."""
    return DashboardServiceV2(data_adapter)

# -------------------------------------------------------------------
# Aliases y anotaciones para tipado limpio en endpoints
# -------------------------------------------------------------------

RedisClient = Annotated[redis.Redis, Depends(get_redis_client)]
DataAdapter = Annotated[DataSourceAdapter, Depends(get_data_adapter)]
CacheRepo = Annotated[CacheRepository, Depends(get_cache_repo)]
BigQueryRepo = Annotated[BigQueryRepository, Depends(get_bigquery_repo)]
PostgresRepo = Annotated[PostgresRepository, Depends(get_postgres_repo)]
CacheSvc = Annotated[CacheService, Depends(get_cache_service)]
DashboardSvc = Annotated[DashboardServiceV2, Depends(get_dashboard_service)]

# Compatibilidad hacia atrás
get_dashboard_service_v2 = get_dashboard_service