"""
📊 BigQuery repository (production ready)
"""
import time
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.config import settings
from app.repositories.base import BaseRepository

class BigQueryRepository(BaseRepository):
    def __init__(self):
        super().__init__()
        self.client: Optional[bigquery.Client] = None
        self.project_id = settings.BIGQUERY_PROJECT_ID
        self.dataset_id = settings.BIGQUERY_DATASET
        self.location = settings.BIGQUERY_LOCATION

    async def connect(self) -> None:
        try:
            self.client = bigquery.Client(
                project=self.project_id,
                location=self.location
            )
            await self.health_check()
            self.is_connected = True
        except Exception as e:
            raise RuntimeError(f"Failed to connect to BigQuery: {e}")

    async def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.is_connected = False

    async def health_check(self) -> bool:
        try:
            if not self.client:
                return False
            job = self.client.query("SELECT 1 as health_check")
            results = list(job.result())
            return len(results) == 1 and results[0].health_check == 1
        except Exception:
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.client or not self.is_connected:
            await self.connect()
        start_time = time.time()
        try:
            job_config = bigquery.QueryJobConfig()
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(k, self._get_bigquery_type(v), v)
                    for k, v in params.items()
                ]
            job = self.client.query(query, job_config=job_config)
            results = job.result()
            rows = [dict(row) for row in results]
            return rows
        except Exception as e:
            raise RuntimeError(f"BigQuery query failed: {e}")

    @staticmethod
    def _get_bigquery_type(value: Any) -> str:
        if isinstance(value, bool):
            return "BOOL"
        elif isinstance(value, int):
            return "INT64"
        elif isinstance(value, float):
            return "FLOAT64"
        elif isinstance(value, str):
            return "STRING"
        else:
            return "STRING"