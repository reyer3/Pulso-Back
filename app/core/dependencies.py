"""
💧 Dependency Injection Container
Centralizes the creation and provisioning of resources, repositories, and services.
"""
from typing import Annotated
from fastapi import Depends
import redis.asyncio as redis

from app.core.cache import cache as redis_cache_manager
from app.repositories.bigquery_repo import BigQueryRepository
from app.repositories.postgres_repo import PostgresRepository
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService

# -------------------------------------------------------------------
# Core Resources
# -------------------------------------------------------------------

async def get_redis_client() -> redis.Redis:
    """Provides the Redis client, initializing if necessary."""
    if not redis_cache_manager.redis:
        await redis_cache_manager.init_redis()
    return redis_cache_manager.redis

# -------------------------------------------------------------------
# Repositories
# -------------------------------------------------------------------

def get_postgres_repo() -> PostgresRepository:
    """Provides the singleton instance of the PostgresRepository."""
    # The repository now uses the global DatabaseManager, so no
    # service needs to be injected here.
    return PostgresRepository()

def get_bigquery_repo() -> BigQueryRepository:
    """Provides the singleton instance of the BigQueryRepository."""
    return BigQueryRepository()
# -------------------------------------------------------------------
# Domain Services (inject repositories)
# -------------------------------------------------------------------

def get_cache_service(
    client: redis.Redis = Depends(get_redis_client)
) -> CacheService:
    """Provides the cache service, which wraps the Redis client."""
    return CacheService(redis_client=client)

def get_dashboard_service(
    postgres_repo: PostgresRepository = Depends(get_postgres_repo)
) -> DashboardServiceV2:
    """Provides the main dashboard service, which depends on the PostgresRepository."""
    return DashboardServiceV2(postgres_repo)

# -------------------------------------------------------------------
# Type-annotated Aliases for Clean Endpoints
# -------------------------------------------------------------------

RedisClient = Annotated[redis.Redis, Depends(get_redis_client)]
PostgresRepo = Annotated[PostgresRepository, Depends(get_postgres_repo)]
CacheSvc = Annotated[CacheService, Depends(get_cache_service)]
DashboardSvc = Annotated[DashboardServiceV2, Depends(get_dashboard_service)]
