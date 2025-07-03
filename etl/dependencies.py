"""
🔧 ETL Dependencies - SIMPLIFIED VERSION

Backward compatibility wrapper for existing imports.
The new architecture uses etl/simple_incremental_etl.py directly.

For new code, use:
- etl.extractors.bigquery_extractor.BigQueryExtractor directly
- etl.loaders.postgres_loader.PostgresLoader directly 
- shared.database.connection.get_database_manager() directly
"""

# Backward compatibility imports
from etl.extractors.bigquery_extractor import BigQueryExtractor
from etl.loaders.postgres_loader import PostgresLoader
from shared.database.connection import get_database_manager


class SimplifiedETLDependencies:
    """
    Simplified dependencies container for backward compatibility only.
    
    New code should use etl/simple_incremental_etl.py instead.
    """
    
    def __init__(self):
        self._bigquery_extractor = None
        self._postgres_loader = None
        self._db_manager = None
    
    def bigquery_extractor(self) -> BigQueryExtractor:
        """Get BigQuery extractor instance"""
        if self._bigquery_extractor is None:
            self._bigquery_extractor = BigQueryExtractor()
        return self._bigquery_extractor
    
    async def postgres_loader(self) -> PostgresLoader:
        """Get PostgreSQL loader instance"""
        if self._postgres_loader is None:
            if self._db_manager is None:
                self._db_manager = await get_database_manager()
            self._postgres_loader = PostgresLoader(self._db_manager)
        return self._postgres_loader
    
    async def init_resources(self) -> None:
        """Initialize resources"""
        self._db_manager = await get_database_manager()
    
    async def shutdown_resources(self) -> None:
        """Shutdown resources"""
        if self._db_manager:
            await self._db_manager.close()


# Singleton for backward compatibility
etl_dependencies = SimplifiedETLDependencies()
