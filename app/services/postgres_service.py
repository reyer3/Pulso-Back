import asyncpg
import asyncio
import logging
from typing import Optional, List, Dict, Any

from app.core.config import settings

logger = logging.getLogger(__name__)

class PostgresService:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or settings.POSTGRES_URL
        if not self.dsn:
            raise ValueError("PostgreSQL DSN must be configured via POSTGRES_URL environment variable.")
        # Ensure the DSN is compatible with asyncpg (postgresql://...)
        if self.dsn.startswith("postgresql+asyncpg://"):
            logger.warning("DSN starts with postgresql+asyncpg://, converting to postgresql:// for asyncpg direct use.")
            self.dsn = self.dsn.replace("postgresql+asyncpg://", "postgresql://")

    async def _get_connection(self):
        try:
            conn = await asyncpg.connect(dsn=self.dsn)
            logger.info(f"Successfully connected to PostgreSQL: {self.dsn.split('@')[-1]}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    async def fetch(self, query: str, *params: Any) -> List[asyncpg.Record]:
        """
        Executes a query and returns a list of records.
        """
        conn = None
        try:
            conn = await self._get_connection()
            return await conn.fetch(query, *params)
        except Exception as e:
            logger.error(f"Error executing fetch query: {e}. Query: {query[:200]}", exc_info=True)
            raise
        finally:
            if conn:
                await conn.close()

    async def fetchrow(self, query: str, *params: Any) -> Optional[asyncpg.Record]:
        """
        Executes a query and returns a single record or None.
        """
        conn = None
        try:
            conn = await self._get_connection()
            return await conn.fetchrow(query, *params)
        except Exception as e:
            logger.error(f"Error executing fetchrow query: {e}. Query: {query[:200]}", exc_info=True)
            raise
        finally:
            if conn:
                await conn.close()

    async def fetchval(self, query: str, *params: Any) -> Any:
        """
        Executes a query and returns a single value from the first record.
        """
        conn = None
        try:
            conn = await self._get_connection()
            return await conn.fetchval(query, *params)
        except Exception as e:
            logger.error(f"Error executing fetchval query: {e}. Query: {query[:200]}", exc_info=True)
            raise
        finally:
            if conn:
                await conn.close()

    async def execute(self, query: str, *params: Any) -> str:
        """
        Executes a query (e.g., INSERT, UPDATE, DELETE) and returns the status.
        """
        conn = None
        try:
            conn = await self._get_connection()
            return await conn.execute(query, *params)
        except Exception as e:
            logger.error(f"Error executing execute query: {e}. Query: {query[:200]}", exc_info=True)
            raise
        finally:
            if conn:
                await conn.close()

    async def health_check(self) -> bool:
        """
        Performs a simple health check on the PostgreSQL connection.
        Returns True if successful, False otherwise.
        """
        try:
            val = await self.fetchval("SELECT 1")
            return val == 1
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False

# Example usage (can be removed or moved to tests)
async def example_get_user_by_id(user_id: int):
    pg_service = PostgresService()
    # Ensure DSN is correctly configured in your environment for this example to run
    if not pg_service.dsn:
        logger.warning("Skipping example_get_user_by_id: POSTGRES_URL not configured.")
        return None

    logger.info(f"Attempting to fetch user with ID: {user_id} using DSN: {pg_service.dsn}")
    try:
        # Example: Fetching a user by ID
        # Replace 'users' and 'id' with your actual table and column names
        # Ensure you have a 'users' table with an 'id' column for this example
        # user_record = await pg_service.fetchrow("SELECT * FROM users WHERE id = $1", user_id)

        # For testing without a real users table, let's try a simple query
        time_record = await pg_service.fetchrow("SELECT NOW() as current_time;")
        if time_record:
            logger.info(f"PostgreSQL current time: {time_record['current_time']}")
            return time_record
        else:
            logger.warning(f"No record found for example query.")
            return None

    except Exception as e:
        logger.error(f"Error in example_get_user_by_id: {e}")
        return None

if __name__ == "__main__":
    # This is for standalone testing of the service
    # You'll need to have your .env file or environment variables set up
    # e.g., POSTGRES_URL=postgresql://user:pass@host/db

    async def main():
        # Check if DSN is available
        if not settings.POSTGRES_URL:
            print("POSTGRES_URL is not set. Skipping PostgreSQL service example.")
            return

        print(f"Running PostgresService example with DSN: {settings.POSTGRES_URL}")

        # Example: Fetch current time (or a user if you have a users table)
        record = await example_get_user_by_id(1)
        if record:
            print("Example query successful. Record:", dict(record))
        else:
            print("Example query failed or returned no record.")

        # Test health check
        pg_service = PostgresService()
        is_healthy = await pg_service.health_check()
        print(f"PostgreSQL health check: {'OK' if is_healthy else 'Failed'}")

    # Setup basic logging for the example
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
