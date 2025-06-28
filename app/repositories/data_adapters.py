"""
🔌 Data Source Adapters y Factory - producción
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd

from app.core.config import get_settings
from app.core.logging import LoggerMixin

# --- INTERFAZ ABSTRACTA ---

class DataSourceAdapter(ABC):
    @abstractmethod
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def execute_query_df(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    async def test_connection(self) -> bool:
        pass

    @property
    @abstractmethod
    def dataset(self) -> str:
        pass

# --- IMPLEMENTACIÓN BIGQUERY ---

from google.cloud import bigquery
import asyncio
from datetime import date, datetime

class BigQueryAdapter(DataSourceAdapter, LoggerMixin):
    def __init__(self, project_id: str, dataset_name: str):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.client = bigquery.Client(project=project_id)

    @property
    def dataset(self) -> str:
        return f"{self.project_id}.{self.dataset_name}"

    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        try:
            loop = asyncio.get_event_loop()
            job = await loop.run_in_executor(None, self.client.query, query)
            results = await loop.run_in_executor(None, job.result)
            rows = []
            for row in results:
                row_dict = {k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in row.items()}
                rows.append(row_dict)
            self.logger.info(f"BigQuery query returned {len(rows)} rows")
            return rows
        except Exception as e:
            self.logger.error(f"BigQuery query failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            raise

    async def execute_query_df(self, query: str) -> pd.DataFrame:
        try:
            loop = asyncio.get_event_loop()
            job = await loop.run_in_executor(None, self.client.query, query)
            df = await loop.run_in_executor(None, job.to_dataframe)
            self.logger.info(f"BigQuery query returned DataFrame with {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"BigQuery query failed: {str(e)}")
            raise

    async def test_connection(self) -> bool:
        try:
            test_query = "SELECT 1 as test_value"
            result = await self.execute_query(test_query)
            return len(result) == 1 and result[0]['test_value'] == 1
        except Exception as e:
            self.logger.error(f"BigQuery connection test failed: {str(e)}")
            return False

# --- IMPLEMENTACIÓN POSTGRESQL ---

import asyncpg

class PostgreSQLAdapter(DataSourceAdapter, LoggerMixin):
    def __init__(self, connection_string: str, schema_name: str):
        self.connection_string = connection_string
        self.schema_name = schema_name
        self._pool: Optional[asyncpg.Pool] = None

    @property
    def dataset(self) -> str:
        return self.schema_name

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
        return self._pool

    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                result = []
                for row in rows:
                    row_dict = {k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in row.items()}
                    result.append(row_dict)
                self.logger.info(f"PostgreSQL query returned {len(result)} rows")
                return result
        except Exception as e:
            self.logger.error(f"PostgreSQL query failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            raise

    async def execute_query_df(self, query: str) -> pd.DataFrame:
        rows = await self.execute_query(query)
        df = pd.DataFrame(rows)
        self.logger.info(f"PostgreSQL query returned DataFrame with {len(df)} rows")
        return df

    async def test_connection(self) -> bool:
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            self.logger.error(f"PostgreSQL connection test failed: {str(e)}")
            return False

    async def close(self):
        if self._pool:
            await self._pool.close()

# --- FACTORY DE ADAPTERS ---

class DataSourceFactory:
    """
    Factory para crear adapters de datos según configuración
    """
    @staticmethod
    def create_adapter(source_type: Optional[str] = None) -> DataSourceAdapter:
        settings = get_settings()
        if source_type is None:
            source_type = settings.DATA_SOURCE_TYPE
        st = source_type.lower()
        if st == 'bigquery':
            return BigQueryAdapter(
                project_id=settings.BIGQUERY_PROJECT_ID,
                dataset_name=settings.BIGQUERY_DATASET
            )
        elif st == 'postgresql':
            return PostgreSQLAdapter(
                connection_string=settings.POSTGRES_URL,
                schema_name=settings.POSTGRES_SCHEMA
            )
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")

    @staticmethod
    async def test_all_connections() -> Dict[str, bool]:
        results = {}
        try:
            bigquery_adapter = DataSourceFactory.create_adapter('bigquery')
            results['bigquery'] = await bigquery_adapter.test_connection()
        except Exception:
            results['bigquery'] = False
        try:
            postgres_adapter = DataSourceFactory.create_adapter('postgresql')
            results['postgresql'] = await postgres_adapter.test_connection()
            if hasattr(postgres_adapter, 'close'):
                await postgres_adapter.close()
        except Exception:
            results['postgresql'] = False
        return results