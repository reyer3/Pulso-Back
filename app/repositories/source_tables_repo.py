"""
📊 BigQuery repository implementation
Data access layer for BigQuery operations
"""

import time
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.core.middleware import track_bigquery_query
from app.repositories.base import BaseRepository

class BigQueryRepository(BaseRepository):
    """
    BigQuery repository for data access
    """

    def __init__(self):
        super().__init__()
        self.client: Optional[bigquery.Client] = None
        self.project_id = settings.BIGQUERY_PROJECT_ID
        self.dataset_id = settings.BIGQUERY_DATASET
        self.location = settings.BIGQUERY_LOCATION

    async def _ensure_connection(self) -> None:
        """Ensure the BigQuery client is connected and healthy."""
        if not self.client or not self.is_connected:
            await self.connect()
        elif not await self.health_check():
            await self.connect()

    async def connect(self) -> None:
        """Initialize BigQuery client."""
        try:
            self.client = bigquery.Client(
                project=self.project_id,
                location=self.location
            )
            await self.health_check()
            self.is_connected = True
            self.logger.info("BigQuery connection established")
        except Exception as e:
            self.logger.error(f"Failed to connect to BigQuery: {e}")
            raise

    async def disconnect(self) -> None:
        """Close BigQuery client."""
        if self.client:
            self.client.close()
            self.is_connected = False
            self.logger.info("BigQuery connection closed")

    async def health_check(self) -> bool:
        """Check BigQuery connection health."""
        try:
            if not self.client:
                return False
            query = "SELECT 1 as health_check"
            job = self.client.query(query)
            results = list(job.result())
            return len(results) == 1 and results[0].health_check == 1
        except Exception as e:
            self.logger.error(f"BigQuery health check failed: {e}")
            return False

    def _build_job_config(self, params: Optional[Dict[str, Any]] = None, job_config: Optional[bigquery.QueryJobConfig] = None) -> bigquery.QueryJobConfig:
        """Create or extend a QueryJobConfig with parameters."""
        config = job_config or bigquery.QueryJobConfig()
        if params:
            config.query_parameters = [
                bigquery.ScalarQueryParameter(k, self._get_bigquery_type(v), v)
                for k, v in params.items()
            ]
        return config

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        job_config: Optional[bigquery.QueryJobConfig] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute BigQuery SQL query
        """
        await self._ensure_connection()
        start_time = time.time()

        try:
            actual_job_config = self._build_job_config(params, job_config)
            self.logger.info(f"Executing BigQuery query", query_length=len(query))
            job = self.client.query(query, job_config=actual_job_config)
            results = job.result()
            rows = [dict(row) for row in results]
            duration = time.time() - start_time
            track_bigquery_query(self.dataset_id, "query", duration)
            self.logger.info(
                f"BigQuery query completed",
                rows_returned=len(rows),
                duration=duration,
                bytes_processed=job.total_bytes_processed,
                bytes_billed=job.total_bytes_billed
            )
            return rows
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(
                f"BigQuery query failed",
                error=str(e),
                duration=duration,
                query=query[:200] + "..." if len(query) > 200 else query
            )
            raise

    @staticmethod
    def _get_bigquery_type(value: Any) -> str:
        """
        Get BigQuery parameter type from Python value
        """
        if isinstance(value, bool):
            return "BOOL"
        elif isinstance(value, int):
            return "INT64"
        elif isinstance(value, float):
            return "FLOAT64"
        elif isinstance(value, str):
            return "STRING"
        else:
            return "STRING"  # Default to string

    def get_full_table_name(self, table_name: str) -> str:
        """Get fully qualified table name"""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"