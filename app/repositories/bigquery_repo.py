# app/repositories/bigquery_repo.py
"""
🗄️ Google BigQuery Repository Implementation
"""
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

from app.core.config import settings
from app.core.logging import LoggerMixin
from app.repositories.base import BaseRepository


class BigQueryRepository(BaseRepository, LoggerMixin):
    """
    BigQuery repository for data access.
    Handles client authentication and query execution.
    """

    def __init__(self):
        super().__init__()
        self.client: Optional[bigquery.Client] = None
        self.project_id = settings.BIGQUERY_PROJECT_ID
        self._connect_on_init()

    def _connect_on_init(self):
        """Initializes the connection upon instantiation."""
        try:
            self.logger.info("Initializing BigQuery client...")
            if settings.GCP_SERVICE_ACCOUNT_KEY_JSON:
                credentials = service_account.Credentials.from_service_account_info(
                    settings.GCP_SERVICE_ACCOUNT_KEY_JSON
                )
                self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            else:
                # Fallback to default credentials if no specific key is provided
                self.client = bigquery.Client(project=self.project_id)

            self.is_connected = True
            self.logger.info(f"BigQuery client initialized successfully for project '{self.project_id}'.")
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}", exc_info=True)
            self.is_connected = False
            self.client = None

    async def connect(self) -> None:
        """Ensures the BigQuery client is available."""
        if not self.client:
            self._connect_on_init()
        if not self.is_connected:
            raise ConnectionError("Failed to connect to BigQuery.")

    async def disconnect(self) -> None:
        """Closes the BigQuery client."""
        if self.client:
            self.client.close()
            self.is_connected = False
            self.logger.info("BigQuery client closed.")

    async def health_check(self) -> bool:
        """Performs a simple query to check the connection."""
        if not self.client or not self.is_connected:
            return False
        try:
            query_job = self.client.query("SELECT 1")
            await self.execute_query("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"BigQuery health check failed: {e}")
            return False

    async def execute_query(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes a query on BigQuery with named parameters.

        Args:
            query: The SQL query string with @param_name placeholders.
            params: A dictionary of parameters.

        Returns:
            A list of records as dictionaries.
        """
        if not self.client:
            raise ConnectionError("BigQuery client is not initialized.")

        self.logger.debug(f"Executing BigQuery query: {query}", params=params)

        job_config = bigquery.QueryJobConfig()
        if params:
            query_params = [
                bigquery.ScalarQueryParameter(name, self._get_param_type(value), value)
                for name, value in params.items()
            ]
            job_config.query_parameters = query_params

        try:
            query_job = self.client.query(query, job_config=job_config)
            # Wait for the job to complete and convert the result to a list of dicts
            results = [dict(row) for row in query_job.result()]
            self.logger.info(f"BigQuery query executed successfully, returned {len(results)} rows.")
            return results
        except Exception as e:
            self.logger.error(f"BigQuery query failed: {e}", exc_info=True)
            raise

    @staticmethod
    def _get_param_type(value: Any) -> str:
        """Determines BigQuery type from Python type for query parameters."""
        if isinstance(value, str):
            return "STRING"
        if isinstance(value, int):
            return "INT64"
        if isinstance(value, float):
            return "FLOAT64"
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, (date, datetime)):
            return "DATE" if isinstance(value, date) else "DATETIME"
        if isinstance(value, list):
            # BigQuery requires the type of elements in the array
            if not value:
                raise ValueError("Cannot determine array type from empty list.")
            # For simplicity, we assume homogeneous lists
            element_type = BigQueryRepository._get_param_type(value[0])
            return f"ARRAY<{element_type}>"
        return "STRING"  # Default fallback