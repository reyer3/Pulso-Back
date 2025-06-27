"""
🏗️ Base repository interface
Abstract base class for all repository implementations
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from app.core.logging import LoggerMixin


class BaseRepository(ABC, LoggerMixin):
    """
    Abstract base repository with common interface
    """
    
    def __init__(self):
        self.connection = None
        self.is_connected = False
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to data source
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close connection to data source
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if connection is healthy
        """
        pass
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a query and return results
        """
        raise NotImplementedError("Subclasses must implement execute_query")
    
    async def execute_single(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a query and return single result
        """
        results = await self.execute_query(query, params)
        return results[0] if results else None
    
    async def execute_scalar(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Execute a query and return single scalar value
        """
        result = await self.execute_single(query, params)
        if result:
            return list(result.values())[0]
        return None
    
    def build_where_clause(
        self, 
        filters: Dict[str, Union[str, List[str]]], 
        table_alias: str = ""
    ) -> tuple[str, Dict[str, Any]]:
        """
        Build WHERE clause from filters
        Returns (where_clause, params)
        """
        if not filters:
            return "", {}
        
        conditions = []
        params = {}
        param_counter = 0
        
        for field, value in filters.items():
            if value is None:
                continue
            
            # Handle table alias
            field_name = f"{table_alias}.{field}" if table_alias else field
            
            if isinstance(value, list):
                if len(value) == 1:
                    # Single value
                    param_name = f"param_{param_counter}"
                    conditions.append(f"{field_name} = @{param_name}")
                    params[param_name] = value[0]
                    param_counter += 1
                else:
                    # Multiple values - IN clause
                    param_names = []
                    for v in value:
                        param_name = f"param_{param_counter}"
                        param_names.append(f"@{param_name}")
                        params[param_name] = v
                        param_counter += 1
                    conditions.append(f"{field_name} IN ({', '.join(param_names)})")
            else:
                # Single value
                param_name = f"param_{param_counter}"
                conditions.append(f"{field_name} = @{param_name}")
                params[param_name] = value
                param_counter += 1
        
        where_clause = " AND ".join(conditions) if conditions else ""
        return where_clause, params
    
    def build_order_clause(
        self, 
        order_by: Optional[List[str]] = None, 
        table_alias: str = ""
    ) -> str:
        """
        Build ORDER BY clause
        """
        if not order_by:
            return ""
        
        order_parts = []
        for order in order_by:
            if " " in order:
                field, direction = order.split(" ", 1)
                direction = direction.upper()
                if direction not in ["ASC", "DESC"]:
                    direction = "ASC"
            else:
                field = order
                direction = "ASC"
            
            field_name = f"{table_alias}.{field}" if table_alias else field
            order_parts.append(f"{field_name} {direction}")
        
        return f"ORDER BY {', '.join(order_parts)}"
    
    def build_limit_clause(
        self, 
        limit: Optional[int] = None, 
        offset: Optional[int] = None
    ) -> str:
        """
        Build LIMIT/OFFSET clause
        """
        clause_parts = []
        
        if limit is not None:
            clause_parts.append(f"LIMIT {limit}")
        
        if offset is not None and offset > 0:
            clause_parts.append(f"OFFSET {offset}")
        
        return " ".join(clause_parts)
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()
        return False


class CacheableRepository(BaseRepository):
    """
    Repository with caching capabilities
    """
    
    def __init__(self, cache_prefix: str = ""):
        super().__init__()
        self.cache_prefix = cache_prefix
        self.default_ttl = 3600  # 1 hour
    
    def get_cache_key(
        self, 
        method: str, 
        **kwargs: Union[str, int, float]
    ) -> str:
        """
        Generate cache key for method and parameters
        """
        key_parts = [self.cache_prefix, method] if self.cache_prefix else [method]
        
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        
        return ":".join(key_parts)
    
    @abstractmethod
    async def get_from_cache(
        self, 
        cache_key: str
    ) -> Optional[Any]:
        """
        Get data from cache
        """
        pass
    
    @abstractmethod
    async def set_to_cache(
        self, 
        cache_key: str, 
        data: Any, 
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set data to cache
        """
        pass
    
    @abstractmethod
    async def invalidate_cache(
        self, 
        pattern: str
    ) -> int:
        """
        Invalidate cache entries matching pattern
        """
        pass