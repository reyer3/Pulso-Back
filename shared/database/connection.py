"""
🗄️ Pure asyncpg Database Connection Management
Clean, efficient PostgreSQL connections without SQLAlchemy overhead

Features:
- Connection pooling with asyncpg
- Automatic connection management
- Health checks and monitoring
- Transaction support
"""

import logging
from typing import Optional, Dict, Any, List

import asyncpg

from shared.core.config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Pure asyncpg database manager
    Handles connection pooling and database operations
    """
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._pool: Optional[asyncpg.Pool] = None
        self.logger = logging.getLogger(__name__)
    
    async def initialize(self) -> None:
        """Initialize the connection pool"""
        if self._pool is None:
            try:
                self._pool = await asyncpg.create_pool(
                    self.connection_string,
                    min_size=2,
                    max_size=10,
                    command_timeout=60,
                    server_settings={
                        'jit': 'off'  # Disable JIT for faster connection times
                    }
                )
                self.logger.info("✅ PostgreSQL connection pool initialized")
                
                # Test connection
                await self.health_check()
                
            except Exception as e:
                self.logger.error(f"❌ Failed to initialize database pool: {e}")
                raise
    
    async def get_pool(self) -> asyncpg.Pool:
        """Get the connection pool, initializing if necessary"""
        if self._pool is None:
            await self.initialize()
        return self._pool
    
    async def close(self) -> None:
        """Close the connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.logger.info("🔒 PostgreSQL connection pool closed")
    
    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            self.logger.error(f"❌ Database health check failed: {e}")
            return False
    
    async def execute_query(
        self, 
        query: str, 
        *args, 
        fetch: str = "none"
    ) -> Any:
        """
        Execute a query with automatic connection management
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            fetch: 'none', 'one', 'all', 'val'
            
        Returns:
            Query result based on fetch type
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            if fetch == "none":
                return await conn.execute(query, *args)
            elif fetch == "one":
                return await conn.fetchrow(query, *args)
            elif fetch == "all":
                return await conn.fetch(query, *args)
            elif fetch == "val":
                return await conn.fetchval(query, *args)
            else:
                raise ValueError(f"Invalid fetch type: {fetch}")
    
    async def execute_transaction(self, queries: List[tuple]) -> None:
        """
        Execute multiple queries in a transaction
        
        Args:
            queries: List of (query, args) tuples
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                for query, args in queries:
                    await conn.execute(query, *args)
    
    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table"""
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = $1
        ORDER BY ordinal_position
        """
        
        rows = await self.execute_query(query, table_name, fetch="all")
        return {
            "table_name": table_name,
            "columns": [dict(row) for row in rows],
            "column_count": len(rows)
        }
    
    async def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = $1
        )
        """
        return await self.execute_query(query, table_name, fetch="val")


# 🌍 Global database manager instance
_db_manager: Optional[DatabaseManager] = None


async def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance"""
    global _db_manager
    
    if _db_manager is None:
        _db_manager = DatabaseManager(settings.POSTGRES_URL)
        await _db_manager.initialize()
    
    return _db_manager


async def close_database_connections() -> None:
    """Close all database connections"""
    global _db_manager
    
    if _db_manager:
        await _db_manager.close()
        _db_manager = None


# 🚀 Convenience functions for common operations
async def execute_query(query: str, *args, fetch: str = "none") -> Any:
    """Execute a query using the global database manager"""
    db = await get_database_manager()
    return await db.execute_query(query, *args, fetch=fetch)


async def execute_transaction(queries: List[tuple]) -> None:
    """Execute a transaction using the global database manager"""
    db = await get_database_manager()
    return await db.execute_transaction(queries)


async def database_health_check() -> Dict[str, Any]:
    """Comprehensive database health check"""
    try:
        db = await get_database_manager()
        is_healthy = await db.health_check()
        
        if is_healthy:
            # Get some basic stats
            stats = await execute_query(
                """
                SELECT 
                    current_database() as database_name,
                    current_user as current_user,
                    version() as postgres_version,
                    now() as server_time
                """,
                fetch="one"
            )
            
            return {
                "status": "healthy",
                "database_name": stats["database_name"],
                "current_user": stats["current_user"],
                "postgres_version": stats["postgres_version"].split(",")[0],  # Just version number
                "server_time": stats["server_time"].isoformat(),
                "connection_pool": {
                    "size": db._pool.get_size() if db._pool else 0,
                    "idle": db._pool.get_idle_size() if db._pool else 0,
                }
            }
        else:
            return {
                "status": "unhealthy",
                "error": "Connection test failed"
            }
            
    except Exception as e:
        logger.error(f"Database health check error: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


# 🔧 Migration support functions
async def run_migrations() -> None:
    """
    Run database migrations using yoyo-migrations
    This is a convenience function - migrations should typically be run via CLI
    """
    try:
        import subprocess
        import os
        
        # Set database URL from settings
        env = os.environ.copy()
        env['DATABASE_URL'] = settings.POSTGRES_URL
        
        # Run yoyo migrations
        result = subprocess.run(
            ['yoyo', 'apply', '--database', settings.POSTGRES_URL],
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode == 0:
            logger.info("✅ Database migrations applied successfully")
        else:
            logger.error(f"❌ Migration failed: {result.stderr}")
            raise Exception(f"Migration failed: {result.stderr}")
            
    except ImportError:
        logger.warning("⚠️ yoyo-migrations not available for programmatic migration")
    except Exception as e:
        logger.error(f"❌ Migration error: {e}")
        raise


# 🎯 FastAPI integration
async def database_startup():
    """FastAPI startup handler for database"""
    logger.info("🚀 Initializing database connections...")
    await get_database_manager()
    logger.info("✅ Database connections ready")


async def database_shutdown():
    """FastAPI shutdown handler for database"""
    logger.info("🔒 Closing database connections...")
    await close_database_connections()
    logger.info("✅ Database connections closed")
