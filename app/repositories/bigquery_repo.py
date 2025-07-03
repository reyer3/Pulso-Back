# app/repositories/bigquery_repo.py
"""
🗄️ Enhanced Google BigQuery Repository Implementation
Async-first, production-ready BigQuery client with advanced features
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Union
from functools import wraps

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.oauth2 import service_account
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.repositories.base import BaseRepository
from shared.core.config import settings


def async_retry(func):
    """Async retry decorator for BigQuery operations"""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,)),
    )
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await func(*args, **kwargs)

    return wrapper


class BigQueryRepository(BaseRepository):
    """
    Enhanced BigQuery repository with async support, retry logic, and advanced features.

    Features:
    - True async operations using ThreadPoolExecutor
    - Automatic retry on failures
    - Query result caching
    - Pagination support
    - Data type conversion utilities
    - Performance metrics
    - Structured logging
    """

    def __init__(self, max_workers: int = 4):
        super().__init__()
        self.client: Optional[bigquery.Client] = None
        self.project_id = settings.BIGQUERY_PROJECT_ID
        self.dataset_id = settings.BIGQUERY_DATASET
        self.location = settings.BIGQUERY_LOCATION
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._query_cache: Dict[str, Any] = {}
        self._connection_attempts = 0
        self._max_connection_attempts = 3

        # Initialize connection
        asyncio.create_task(self.connect())

    async def connect(self) -> None:
        """Establishes async connection to BigQuery"""
        if self.is_connected and self.client:
            return

        try:
            self._connection_attempts += 1

            # Run connection in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            self.client = await loop.run_in_executor(
                self.executor, self._create_client
            )

            # Test connection
            await self._test_connection()

            self.is_connected = True
            self._connection_attempts = 0
            self.logger.info(
                f"✅ BigQuery connected successfully",
                extra={
                    "project_id": self.project_id,
                    "dataset": self.dataset_id,
                    "location": self.location
                }
            )

        except Exception as e:
            self.is_connected = False
            self.logger.error(
                f"❌ BigQuery connection failed (attempt {self._connection_attempts})",
                extra={"error": str(e), "project_id": self.project_id},
                exc_info=True
            )

            if self._connection_attempts < self._max_connection_attempts:
                await asyncio.sleep(2 ** self._connection_attempts)
                await self.connect()
            else:
                raise ConnectionError(f"Failed to connect to BigQuery after {self._max_connection_attempts} attempts")

    def _create_client(self) -> bigquery.Client:
        """Creates BigQuery client with proper authentication"""
        try:
            # Use service account key if provided
            if hasattr(settings, 'GCP_SERVICE_ACCOUNT_KEY_JSON') and settings.GCP_SERVICE_ACCOUNT_KEY_JSON:
                if isinstance(settings.GCP_SERVICE_ACCOUNT_KEY_JSON, str):
                    key_info = json.loads(settings.GCP_SERVICE_ACCOUNT_KEY_JSON)
                else:
                    key_info = settings.GCP_SERVICE_ACCOUNT_KEY_JSON

                credentials = service_account.Credentials.from_service_account_info(key_info)
                return bigquery.Client(credentials=credentials, project=self.project_id, location=self.location)

            # Use application default credentials
            elif settings.GOOGLE_APPLICATION_CREDENTIALS:
                credentials = service_account.Credentials.from_service_account_file(
                    settings.GOOGLE_APPLICATION_CREDENTIALS
                )
                return bigquery.Client(credentials=credentials, project=self.project_id, location=self.location)

            # Fallback to default credentials
            else:
                return bigquery.Client(project=self.project_id, location=self.location)

        except Exception as e:
            self.logger.error(f"Failed to create BigQuery client: {e}")
            raise

    async def _test_connection(self) -> None:
        """Tests the BigQuery connection with a simple query"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.executor,
            lambda: list(self.client.query("SELECT 1 as test").result())
        )

    async def disconnect(self) -> None:
        """Closes BigQuery client and thread pool"""
        if self.client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self.executor, self.client.close)
            self.client = None

        if self.executor:
            self.executor.shutdown(wait=True)

        self.is_connected = False
        self.logger.info("🔌 BigQuery client disconnected")

    async def health_check(self) -> bool:
        """Performs comprehensive health check"""
        if not self.client or not self.is_connected:
            return False

        try:
            await self.execute_query("SELECT 1 as health_check")
            return True
        except Exception as e:
            self.logger.error(f"BigQuery health check failed: {e}")
            return False

    @async_retry
    async def execute_query(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None,
            use_cache: bool = False,
            cache_ttl: int = 3600,
            max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes a BigQuery query with advanced features

        Args:
            query: SQL query string
            params: Query parameters
            use_cache: Whether to cache results
            cache_ttl: Cache TTL in seconds
            max_results: Maximum number of results to return

        Returns:
            List of dictionaries with query results
        """
        if not self.client:
            await self.connect()

        # Check cache first
        cache_key = self._get_cache_key(query, params) if use_cache else None
        if cache_key and cache_key in self._query_cache:
            self.logger.debug(f"📋 Cache hit for query: {query[:100]}...")
            return self._query_cache[cache_key]

        start_time = datetime.now(timezone.utc)

        try:
            # Prepare job config
            job_config = QueryJobConfig()
            if params:
                job_config.query_parameters = self._build_query_parameters(params)

            if max_results:
                job_config.maximum_bytes_billed = 100 * 1024 * 1024 * 1024  # 100 GB limit

            # Execute query in thread pool
            loop = asyncio.get_event_loop()
            query_job = await loop.run_in_executor(
                self.executor,
                lambda: self.client.query(query, job_config=job_config)
            )

            # Get results
            results = await loop.run_in_executor(
                self.executor,
                lambda: list(query_job.result(max_results=max_results))
            )

            # Convert to dict format
            data = [dict(row) for row in results]

            # Cache results if requested
            if cache_key:
                self._query_cache[cache_key] = data
                # Simple TTL - in production use Redis with proper TTL
                asyncio.create_task(self._expire_cache_key(cache_key, cache_ttl))

            # Log metrics
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(
                f"🔍 BigQuery query executed successfully",
                extra={
                    "query_hash": hash(query) % 10000,
                    "execution_time": execution_time,
                    "rows_returned": len(data),
                    "bytes_processed": getattr(query_job, 'total_bytes_processed', 0),
                    "cached": bool(cache_key),
                }
            )

            return data

        except Exception as e:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.error(
                f"❌ BigQuery query failed",
                extra={
                    "query_hash": hash(query) % 10000,
                    "execution_time": execution_time,
                    "error": str(e),
                    "params": params,
                },
                exc_info=True
            )
            raise

    async def execute_query_to_dataframe(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None,
            **kwargs
    ) -> pd.DataFrame:
        """Execute query and return pandas DataFrame"""
        results = await self.execute_query(query, params, **kwargs)
        return pd.DataFrame(results)

    async def execute_paginated_query(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None,
            page_size: int = 1000,
            max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query with automatic pagination

        Args:
            query: Base SQL query
            params: Query parameters
            page_size: Number of records per page
            max_pages: Maximum number of pages to fetch

        Returns:
            Combined results from all pages
        """
        all_results = []
        page = 0

        while max_pages is None or page < max_pages:
            offset = page * page_size
            paginated_query = f"""
            {query}
            LIMIT {page_size}
            OFFSET {offset}
            """

            page_results = await self.execute_query(paginated_query, params)

            if not page_results:
                break

            all_results.extend(page_results)
            page += 1

            self.logger.debug(f"📄 Fetched page {page}, records: {len(page_results)}")

            # If we got less than page_size, we've reached the end
            if len(page_results) < page_size:
                break

        self.logger.info(f"📚 Paginated query completed: {len(all_results)} total records, {page} pages")
        return all_results

    async def get_table_schema(self, table_name: str, dataset: Optional[str] = None) -> Dict[str, Any]:
        """Get table schema information"""
        dataset = dataset or self.dataset_id
        table_ref = f"{self.project_id}.{dataset}.{table_name}"

        loop = asyncio.get_event_loop()
        table = await loop.run_in_executor(
            self.executor,
            lambda: self.client.get_table(table_ref)
        )

        return {
            "table_id": table.table_id,
            "dataset_id": table.dataset_id,
            "project_id": table.project,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created.isoformat() if table.created else None,
            "modified": table.modified.isoformat() if table.modified else None,
            "schema": [
                {
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description,
                }
                for field in table.schema
            ]
        }

    async def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        """List all tables in a dataset"""
        dataset = dataset or self.dataset_id
        dataset_ref = f"{self.project_id}.{dataset}"

        loop = asyncio.get_event_loop()
        tables = await loop.run_in_executor(
            self.executor,
            lambda: list(self.client.list_tables(dataset_ref))
        )

        return [table.table_id for table in tables]

    def _build_query_parameters(self, params: Dict[str, Any]) -> List[bigquery.ScalarQueryParameter]:
        """Build BigQuery query parameters from dict"""
        query_params = []

        for name, value in params.items():
            param_type = self._get_bigquery_type(value)

            if param_type.startswith("ARRAY"):
                # Handle arrays
                element_type = param_type.replace("ARRAY<", "").replace(">", "")
                query_params.append(bigquery.ArrayQueryParameter(name, element_type, value))
            else:
                # Handle scalars
                query_params.append(bigquery.ScalarQueryParameter(name, param_type, value))

        return query_params

    def _get_bigquery_type(self, value: Any) -> str:
        """Convert Python types to BigQuery types"""
        if value is None:
            return "STRING"
        elif isinstance(value, bool):
            return "BOOL"
        elif isinstance(value, int):
            return "INT64"
        elif isinstance(value, float):
            return "FLOAT64"
        elif isinstance(value, str):
            return "STRING"
        elif isinstance(value, datetime):
            return "DATETIME"
        elif isinstance(value, date):
            return "DATE"
        elif isinstance(value, list):
            if not value:
                return "ARRAY<STRING>"
            element_type = self._get_bigquery_type(value[0])
            return f"ARRAY<{element_type}>"
        else:
            return "STRING"

    def _get_cache_key(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate cache key for query and parameters"""
        import hashlib

        key_data = {"query": query, "params": params or {}}
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_string.encode()).hexdigest()

    async def _expire_cache_key(self, cache_key: str, ttl: int) -> None:
        """Remove cache key after TTL expires"""
        await asyncio.sleep(ttl)
        self._query_cache.pop(cache_key, None)

    def clear_cache(self) -> int:
        """Clear all cached queries"""
        count = len(self._query_cache)
        self._query_cache.clear()
        return count

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()

    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'executor') and self.executor:
            self.executor.shutdown(wait=False)


# Convenience functions for common queries
class BigQueryQueries:
    """Common BigQuery queries for the Pulso dashboard"""

    @staticmethod
    def get_dashboard_metrics(date_from: str, date_to: str) -> str:
        """Query for dashboard metrics"""
        return f"""
        SELECT 
            DATE(fecha) as fecha,
            COUNT(*) as total_calls,
            SUM(CASE WHEN resultado = 'PROMESA' THEN 1 ELSE 0 END) as promesas,
            SUM(CASE WHEN resultado = 'CONTACTO' THEN 1 ELSE 0 END) as contactos,
            AVG(duracion_segundos) as avg_duration
        FROM `{settings.BIGQUERY_PROJECT_ID}.{settings.BIGQUERY_DATASET}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE DATE(fecha) BETWEEN @date_from AND @date_to
        GROUP BY DATE(fecha)
        ORDER BY fecha DESC
        """

    @staticmethod
    def get_evolution_data(campaign_id: str, days: int = 30) -> str:
        """Query for evolution analysis"""
        return f"""
        SELECT 
            DATE(fecha) as fecha,
            campaign_id,
            SUM(attempts) as total_attempts,
            SUM(successful_contacts) as successful_contacts,
            SUM(promises) as promises,
            SUM(amount_promised) as amount_promised
        FROM `{settings.BIGQUERY_PROJECT_ID}.{settings.BIGQUERY_DATASET}.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
        WHERE campaign_id = @campaign_id 
            AND DATE(fecha) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        GROUP BY DATE(fecha), campaign_id
        ORDER BY fecha DESC
        """


# Factory function for dependency injection
async def get_bigquery_repository() -> BigQueryRepository:
    """Factory function to get BigQuery repository instance"""
    repo = BigQueryRepository()
    await repo.connect()
    return repo