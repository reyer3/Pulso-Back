"""
🗄️ Redis cache configuration and management
Cache setup with connection pooling
"""

import json
import logging
from typing import Any, Optional, Union

import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from app.core.config import settings

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Redis cache manager with connection pooling
    """
    
    def __init__(self):
        self.pool: Optional[ConnectionPool] = None
        self.redis: Optional[redis.Redis] = None
    
    async def init_redis(self) -> None:
        """
        Initialize Redis connection pool
        """
        try:
            self.pool = ConnectionPool.from_url(
                settings.REDIS_URL,
                max_connections=20,
                retry_on_timeout=True,
                decode_responses=True,
            )
            self.redis = redis.Redis(connection_pool=self.pool)
            
            # Test connection
            await self.redis.ping()
            logger.info("Redis connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            raise
    
    async def close(self) -> None:
        """
        Close Redis connections
        """
        if self.redis:
            await self.redis.close()
        if self.pool:
            await self.pool.disconnect()
        logger.info("Redis connections closed")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache
        """
        try:
            if not self.redis:
                await self.init_redis()
            
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
            
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(
        self, 
        key: str, 
        value: Any, 
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set value in cache with optional TTL
        """
        try:
            if not self.redis:
                await self.init_redis()
            
            json_value = json.dumps(value, default=str)
            if ttl:
                await self.redis.setex(key, ttl, json_value)
            else:
                await self.redis.set(key, json_value)
            
            return True
            
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete key from cache
        """
        try:
            if not self.redis:
                await self.init_redis()
            
            result = await self.redis.delete(key)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching pattern
        """
        try:
            if not self.redis:
                await self.init_redis()
            
            keys = await self.redis.keys(pattern)
            if keys:
                return await self.redis.delete(*keys)
            return 0
            
        except Exception as e:
            logger.error(f"Cache clear pattern error for {pattern}: {e}")
            return 0
    
    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache
        """
        try:
            if not self.redis:
                await self.init_redis()
            
            return bool(await self.redis.exists(key))
            
        except Exception as e:
            logger.error(f"Cache exists error for key {key}: {e}")
            return False


# Global cache instance
cache = RedisCache()


async def get_cache() -> RedisCache:
    """
    Dependency to get cache instance
    """
    if not cache.redis:
        await cache.init_redis()
    return cache


def cache_key(
    prefix: str, 
    **kwargs: Union[str, int, float]
) -> str:
    """
    Generate cache key from prefix and kwargs
    """
    key_parts = [prefix]
    for k, v in sorted(kwargs.items()):
        key_parts.append(f"{k}:{v}")
    return ":".join(key_parts)