"""
🏗️ Base repository interfaces - segregadas (ISP/LSP friendly)
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

# 1. Solo para repositorios de datos (SQL, BigQuery, etc)
class BaseRepository(ABC):
    def __init__(self):
        self.is_connected: bool = False

    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def disconnect(self) -> None: ...
    @abstractmethod
    async def health_check(self) -> bool: ...
    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]: ...

    async def execute_single(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        results = await self.execute_query(query, params)
        return results[0] if results else None

    async def execute_scalar(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        result = await self.execute_single(query, params)
        return list(result.values())[0] if result else None

# 2. Solo para repositorios de caché
class CacheRepositoryBase(ABC):
    def __init__(self, cache_prefix: str = ""):
        self.cache_prefix = cache_prefix
        self.default_ttl = 3600

    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def disconnect(self) -> None: ...
    @abstractmethod
    async def health_check(self) -> bool: ...
    @abstractmethod
    async def get_from_cache(self, cache_key: str) -> Optional[Any]: ...
    @abstractmethod
    async def set_to_cache(self, cache_key: str, data: Any, ttl: Optional[int] = None) -> bool: ...
    @abstractmethod
    async def invalidate_cache(self, pattern: str) -> int: ...

    def get_cache_key(self, method: str, **kwargs: Union[str, int, float]) -> str:
        key_parts = [self.cache_prefix, method] if self.cache_prefix else [method]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        return ":".join(key_parts)