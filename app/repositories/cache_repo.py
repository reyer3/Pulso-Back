"""
🗄️ Redis cache repository implementation
Caching layer for frequently accessed data
"""

import json
from typing import Any, Dict, List, Optional, Union

from app.core.cache import cache
from app.core.config import settings
from app.core.middleware import track_cache_hit, track_cache_miss
from app.repositories.base import CacheableRepository


class CacheRepository(CacheableRepository):
    """
    Redis cache repository
    """
    
    def __init__(self):
        super().__init__(cache_prefix="pulso")
        self.cache = cache
    
    async def connect(self) -> None:
        """
        Initialize Redis connection
        """
        await self.cache.init_redis()
        self.is_connected = True
        self.logger.info("Cache repository connected")
    
    async def disconnect(self) -> None:
        """
        Close Redis connection
        """
        await self.cache.close()
        self.is_connected = False
        self.logger.info("Cache repository disconnected")
    
    async def health_check(self) -> bool:
        """
        Check Redis connection health
        """
        try:
            await self.cache.redis.ping()
            return True
        except Exception as e:
            self.logger.error(f"Cache health check failed: {e}")
            return False
    
    async def get_from_cache(
        self, 
        cache_key: str
    ) -> Optional[Any]:
        """
        Get data from cache
        """
        try:
            data = await self.cache.get(cache_key)
            if data is not None:
                track_cache_hit("redis")
                self.logger.debug(f"Cache hit for key: {cache_key}")
                return data
            else:
                track_cache_miss("redis")
                self.logger.debug(f"Cache miss for key: {cache_key}")
                return None
                
        except Exception as e:
            self.logger.error(f"Cache get error for key {cache_key}: {e}")
            track_cache_miss("redis")
            return None
    
    async def set_to_cache(
        self, 
        cache_key: str, 
        data: Any, 
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set data to cache
        """
        try:
            ttl = ttl or self.default_ttl
            success = await self.cache.set(cache_key, data, ttl)
            
            if success:
                self.logger.debug(f"Cache set for key: {cache_key}, TTL: {ttl}")
            else:
                self.logger.warning(f"Cache set failed for key: {cache_key}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Cache set error for key {cache_key}: {e}")
            return False
    
    async def invalidate_cache(
        self, 
        pattern: str
    ) -> int:
        """
        Invalidate cache entries matching pattern
        """
        try:
            count = await self.cache.clear_pattern(pattern)
            self.logger.info(f"Invalidated {count} cache entries for pattern: {pattern}")
            return count
            
        except Exception as e:
            self.logger.error(f"Cache invalidation error for pattern {pattern}: {e}")
            return 0
    
    # Dashboard-specific cache methods
    async def get_dashboard_data(
        self, 
        filters: Dict[str, Union[str, List[str]]] = None,
        dimensions: List[str] = None
    ) -> Optional[Any]:
        """
        Get cached dashboard data
        """
        cache_key = self.get_cache_key(
            "dashboard",
            filters=json.dumps(filters or {}, sort_keys=True),
            dimensions=json.dumps(dimensions or [], sort_keys=True)
        )
        
        return await self.get_from_cache(cache_key)
    
    async def set_dashboard_data(
        self, 
        data: Any,
        filters: Dict[str, Union[str, List[str]]] = None,
        dimensions: List[str] = None
    ) -> bool:
        """
        Cache dashboard data
        """
        cache_key = self.get_cache_key(
            "dashboard",
            filters=json.dumps(filters or {}, sort_keys=True),
            dimensions=json.dumps(dimensions or [], sort_keys=True)
        )
        
        return await self.set_to_cache(
            cache_key, 
            data, 
            settings.CACHE_TTL_DASHBOARD
        )
    
    async def get_evolution_data(
        self, 
        filters: Dict[str, Union[str, List[str]]] = None,
        days_back: int = 30
    ) -> Optional[Any]:
        """
        Get cached evolution data
        """
        cache_key = self.get_cache_key(
            "evolution",
            filters=json.dumps(filters or {}, sort_keys=True),
            days_back=days_back
        )
        
        return await self.get_from_cache(cache_key)
    
    async def set_evolution_data(
        self, 
        data: Any,
        filters: Dict[str, Union[str, List[str]]] = None,
        days_back: int = 30
    ) -> bool:
        """
        Cache evolution data
        """
        cache_key = self.get_cache_key(
            "evolution",
            filters=json.dumps(filters or {}, sort_keys=True),
            days_back=days_back
        )
        
        return await self.set_to_cache(
            cache_key, 
            data, 
            settings.CACHE_TTL_EVOLUTION
        )
    
    async def get_assignment_data(
        self, 
        filters: Dict[str, Union[str, List[str]]] = None
    ) -> Optional[Any]:
        """
        Get cached assignment data
        """
        cache_key = self.get_cache_key(
            "assignment",
            filters=json.dumps(filters or {}, sort_keys=True)
        )
        
        return await self.get_from_cache(cache_key)
    
    async def set_assignment_data(
        self, 
        data: Any,
        filters: Dict[str, Union[str, List[str]]] = None
    ) -> bool:
        """
        Cache assignment data
        """
        cache_key = self.get_cache_key(
            "assignment",
            filters=json.dumps(filters or {}, sort_keys=True)
        )
        
        return await self.set_to_cache(
            cache_key, 
            data, 
            settings.CACHE_TTL_ASSIGNMENT
        )
    
    # Utility methods
    async def invalidate_all_dashboard_cache(self) -> int:
        """
        Invalidate all dashboard cache entries
        """
        return await self.invalidate_cache(f"{self.cache_prefix}:dashboard:*")
    
    async def invalidate_all_evolution_cache(self) -> int:
        """
        Invalidate all evolution cache entries
        """
        return await self.invalidate_cache(f"{self.cache_prefix}:evolution:*")
    
    async def invalidate_all_assignment_cache(self) -> int:
        """
        Invalidate all assignment cache entries
        """
        return await self.invalidate_cache(f"{self.cache_prefix}:assignment:*")
    
    async def invalidate_all_cache(self) -> int:
        """
        Invalidate all cache entries
        """
        return await self.invalidate_cache(f"{self.cache_prefix}:*")
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics
        """
        try:
            if not self.cache.redis:
                await self.connect()
            
            info = await self.cache.redis.info()
            
            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cache stats: {e}")
            return {}
    
    async def get_cache_keys_count(self, pattern: str = None) -> int:
        """
        Get count of cache keys matching pattern
        """
        try:
            if not self.cache.redis:
                await self.connect()
            
            pattern = pattern or f"{self.cache_prefix}:*"
            keys = await self.cache.redis.keys(pattern)
            return len(keys)
            
        except Exception as e:
            self.logger.error(f"Error counting cache keys: {e}")
            return 0