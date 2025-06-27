"""
📊 BigQuery repository implementation
Data access layer for BigQuery operations
"""

import time
from typing import Any, Dict, List, Optional, Union

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
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
    
    async def connect(self) -> None:
        """
        Initialize BigQuery client
        """
        try:
            self.client = bigquery.Client(
                project=self.project_id,
                location=self.location
            )
            
            # Test connection
            await self.health_check()
            self.is_connected = True
            self.logger.info("BigQuery connection established")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to BigQuery: {e}")
            raise
    
    async def disconnect(self) -> None:
        """
        Close BigQuery client
        """
        if self.client:
            self.client.close()
            self.is_connected = False
            self.logger.info("BigQuery connection closed")
    
    async def health_check(self) -> bool:
        """
        Check BigQuery connection health
        """
        try:
            if not self.client:
                return False
            
            # Simple query to test connection
            query = "SELECT 1 as health_check"
            job = self.client.query(query)
            results = list(job.result())
            
            return len(results) == 1 and results[0].health_check == 1
            
        except Exception as e:
            self.logger.error(f"BigQuery health check failed: {e}")
            return False
    
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
        if not self.client or not self.is_connected:
            await self.connect()
        
        start_time = time.time()
        
        try:
            # Configure job
            if job_config is None:
                job_config = bigquery.QueryJobConfig()
            
            # Set query parameters if provided
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(k, self._get_bigquery_type(v), v)
                    for k, v in params.items()
                ]
            
            # Execute query
            self.logger.info(f"Executing BigQuery query", query_length=len(query))
            job = self.client.query(query, job_config=job_config)
            
            # Get results
            results = job.result()
            rows = [dict(row) for row in results]
            
            # Track metrics
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
    
    async def get_dashboard_data(
        self, 
        filters: Dict[str, Union[str, List[str]]] = None,
        dimensions: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get dashboard data from BigQuery view
        """
        view_name = f"{self.project_id}.{self.dataset_id}.bi_P3fV4dWNeMkN5RJMhV8e_vw_kpis_ejecutivos"
        
        # Build WHERE clause
        where_clause, params = self.build_where_clause(filters or {})
        
        # Build query
        query = f"""
        SELECT *
        FROM `{view_name}`
        {f'WHERE {where_clause}' if where_clause else ''}
        ORDER BY PERIODO_DATE DESC, CARTERA, SERVICIO
        LIMIT 10000
        """
        
        return await self.execute_query(query, params)
    
    async def get_evolution_data(
        self, 
        filters: Dict[str, Union[str, List[str]]] = None,
        days_back: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get evolution data from BigQuery view
        """
        view_name = f"{self.project_id}.{self.dataset_id}.bi_P3fV4dWNeMkN5RJMhV8e_vw_evolutivos_diarios"
        
        # Build WHERE clause
        where_conditions = []
        params = {}
        
        # Add date filter
        where_conditions.append("FECHA_FOTO >= DATE_SUB(CURRENT_DATE(), INTERVAL @days_back DAY)")
        params["days_back"] = days_back
        
        # Add other filters
        if filters:
            filter_where, filter_params = self.build_where_clause(filters)
            if filter_where:
                where_conditions.append(filter_where)
                params.update(filter_params)
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT *
        FROM `{view_name}`
        WHERE {where_clause}
        ORDER BY CARTERA, SERVICIO, DIA_GESTION
        LIMIT 50000
        """
        
        return await self.execute_query(query, params)
    
    async def get_assignment_data(
        self, 
        filters: Dict[str, Union[str, List[str]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get assignment data from BigQuery view
        """
        view_name = f"{self.project_id}.{self.dataset_id}.bi_P3fV4dWNeMkN5RJMhV8e_vw_resumen_asignacion"
        
        # Build WHERE clause
        where_clause, params = self.build_where_clause(filters or {})
        
        query = f"""
        SELECT *
        FROM `{view_name}`
        {f'WHERE {where_clause}' if where_clause else ''}
        ORDER BY FECHA_ASIGNACION DESC, CARTERA, SERVICIO
        LIMIT 5000
        """
        
        return await self.execute_query(query, params)
    
    async def get_view_info(self, view_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a BigQuery view
        """
        try:
            table_ref = self.client.dataset(self.dataset_id).table(view_name)
            table = self.client.get_table(table_ref)
            
            return {
                "name": table.table_id,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "description": table.description,
                "schema": [
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": field.description
                    }
                    for field in table.schema
                ]
            }
            
        except NotFound:
            return None
        except Exception as e:
            self.logger.error(f"Error getting view info: {e}")
            return None
    
    async def list_views(self) -> List[str]:
        """
        List all views in the dataset
        """
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            tables = self.client.list_tables(dataset_ref)
            
            view_names = [
                table.table_id 
                for table in tables 
                if table.table_type == "VIEW"
            ]
            
            return sorted(view_names)
            
        except Exception as e:
            self.logger.error(f"Error listing views: {e}")
            return []
    
    async def test_view_query(self, view_name: str) -> bool:
        """
        Test if a view query executes successfully
        """
        try:
            query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset_id}.{view_name}`
            LIMIT 1
            """
            
            results = await self.execute_query(query)
            return len(results) == 1
            
        except Exception as e:
            self.logger.error(f"View query test failed for {view_name}: {e}")
            return False
    
    def _get_bigquery_type(self, value: Any) -> str:
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
        """
        Get fully qualified table name
        """
        return f"{self.project_id}.{self.dataset_id}.{table_name}"