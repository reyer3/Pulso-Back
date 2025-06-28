"""
🗄️ PostgreSQL Repository Implementation (Production Ready)
"""
from typing import Any, Dict, List, Optional

from app.repositories.base import BaseRepository
from app.database.connection import DatabaseManager, get_database_manager

class PostgresRepository(BaseRepository):
    """
    PostgreSQL repository for data access, using the efficient,
    connection-pooled DatabaseManager with asyncpg.
    """
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        super().__init__()
        self._db_manager = db_manager
        self.is_connected = False

    async def _get_db_manager(self) -> DatabaseManager:
        if self._db_manager is None:
            self._db_manager = await get_database_manager()
        return self._db_manager

    async def connect(self) -> None:
        """
        Ensures the database manager is initialized and the connection
        pool is ready.
        """
        db_manager = await self._get_db_manager()
        self.is_connected = await db_manager.health_check()
        if not self.is_connected:
            raise ConnectionError("Failed to connect to PostgreSQL via DatabaseManager.")

    async def disconnect(self) -> None:
        """
        The DatabaseManager handles connection pooling globally,
        so a repository-level disconnect is not needed.
        """
        self.is_connected = False

    async def health_check(self) -> bool:
        db_manager = await self._get_db_manager()
        return await db_manager.health_check()

    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes a query and returns a list of records as dictionaries.
        """
        db_manager = await self._get_db_manager()
        # asyncpg uses positional parameters ($1, $2), so we pass the values.
        param_values = tuple(params.values()) if params else tuple()
        records = await db_manager.execute_query(query, *param_values, fetch="all")
        return [dict(r) for r in records]

    async def execute_single(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Executes a query and returns a single record as a dictionary or None.
        """
        db_manager = await self._get_db_manager()
        param_values = tuple(params.values()) if params else tuple()
        record = await db_manager.execute_query(query, *param_values, fetch="one")
        return dict(record) if record else None

    async def execute_scalar(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Executes a query and returns a single value from the first record.
        """
        db_manager = await self._get_db_manager()
        param_values = tuple(params.values()) if params else tuple()
        return await db_manager.execute_query(query, *param_values, fetch="val")