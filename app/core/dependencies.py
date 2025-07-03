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
from app.repositories.user_repo import UserRepository
from app.repositories.cache_repo import CacheRepository
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService
from app.services.user_service import UserService

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

async def get_user_repo() -> UserRepository:
    """Provides the UserRepository instance with connection."""
    user_repo = UserRepository()
    await user_repo.connect()
    return user_repo

async def get_cache_repo() -> CacheRepository:
    """Provides the CacheRepository instance with connection."""
    cache_repo = CacheRepository()
    await cache_repo.connect()
    return cache_repo

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

async def get_user_service(
    user_repo: UserRepository = Depends(get_user_repo),
    cache_repo: CacheRepository = Depends(get_cache_repo)
) -> UserService:
    """
    Provides the UserService with all required dependencies.
    This is the main factory for user management operations.
    """
    return UserService(user_repo=user_repo, cache_repo=cache_repo)

# -------------------------------------------------------------------
# Type-annotated Aliases for Clean Endpoints
# -------------------------------------------------------------------

RedisClient = Annotated[redis.Redis, Depends(get_redis_client)]
PostgresRepo = Annotated[PostgresRepository, Depends(get_postgres_repo)]
BigQueryRepo = Annotated[BigQueryRepository, Depends(get_bigquery_repo)]
UserRepo = Annotated[UserRepository, Depends(get_user_repo)]
CacheRepo = Annotated[CacheRepository, Depends(get_cache_repo)]

# Service aliases
CacheSvc = Annotated[CacheService, Depends(get_cache_service)]
DashboardSvc = Annotated[DashboardServiceV2, Depends(get_dashboard_service)]
UserSvc = Annotated[UserService, Depends(get_user_service)]
