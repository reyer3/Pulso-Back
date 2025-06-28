"""
🗄️ PostgreSQL repository implementation (production ready)
"""

from typing import Any, Dict, List, Optional

from app.repositories.base import BaseRepository
from app.services.postgres_service import PostgresService

class PostgresRepository(BaseRepository):
    """
    PostgreSQL repository for data access, using PostgresService (asyncpg)
    """
    def __init__(self, dsn: Optional[str] = None):
        super().__init__()
        self.pg_service = PostgresService(dsn)
        self.is_connected = False  # Optionally, you could check at __init__

    async def connect(self) -> None:
        # Optionally test connection here for early failure
        self.is_connected = await self.pg_service.health_check()
        if not self.is_connected:
            raise ConnectionError("Failed to connect to PostgreSQL")

    async def disconnect(self) -> None:
        # PostgresService opens/closes per query; nothing to disconnect globally
        self.is_connected = False

    async def health_check(self) -> bool:
        return await self.pg_service.health_check()

    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return a list of dicts (records)
        """
        # Convert params dict to a positional tuple for asyncpg ($1, $2, ...)
        param_values = tuple(params.values()) if params else tuple()
        records = await self.pg_service.fetch(query, *param_values)
        # asyncpg.Record to dict
        return [dict(r) for r in records]

    async def execute_single(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        param_values = tuple(params.values()) if params else tuple()
        record = await self.pg_service.fetchrow(query, *param_values)
        return dict(record) if record else None

    async def execute_scalar(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        param_values = tuple(params.values()) if params else tuple()
        return await self.pg_service.fetchval(query, *param_values)

    # Puedes agregar aquí helpers de paginación, batch, transacciones, etc. según tu dominio
