"""
🔌 Data Source Adapters
Abstract data access to facilitate BigQuery -> Postgres migration
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import asyncio
from datetime import date, datetime

import pandas as pd
from google.cloud import bigquery
import asyncpg

from app.core.config import get_settings
from app.core.logging import LoggerMixin


class DataSourceAdapter(ABC):
    """
    Abstract data source adapter
    Allows switching between BigQuery and Postgres seamlessly
    """
    
    @abstractmethod
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries"""
        pass
    
    @abstractmethod
    async def execute_query_df(self, query: str) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame"""
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """Test if connection is working"""
        pass
    
    @property
    @abstractmethod
    def dataset(self) -> str:
        """Get dataset/schema name"""
        pass


class BigQueryAdapter(DataSourceAdapter, LoggerMixin):
    """
    BigQuery data source adapter
    Current production data source
    """
    
    def __init__(self, project_id: str, dataset_name: str):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.client = bigquery.Client(project=project_id)
        
    @property
    def dataset(self) -> str:
        return f"{self.project_id}.{self.dataset_name}"
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute BigQuery query and return results
        """
        try:
            # Run query in thread pool to make it async
            loop = asyncio.get_event_loop()
            job = await loop.run_in_executor(
                None, 
                self.client.query, 
                query
            )
            
            # Get results
            results = await loop.run_in_executor(None, job.result)
            
            # Convert to list of dicts
            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    # Handle BigQuery data types
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                    elif isinstance(value, date):
                        row_dict[key] = value.isoformat()
                    else:
                        row_dict[key] = value
                rows.append(row_dict)
            
            self.logger.info(f"BigQuery query returned {len(rows)} rows")
            return rows
            
        except Exception as e:
            self.logger.error(f"BigQuery query failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            raise
    
    async def execute_query_df(self, query: str) -> pd.DataFrame:
        """
        Execute BigQuery query and return DataFrame
        """
        try:
            loop = asyncio.get_event_loop()
            job = await loop.run_in_executor(
                None, 
                self.client.query, 
                query
            )
            
            # Convert to DataFrame
            df = await loop.run_in_executor(
                None, 
                job.to_dataframe
            )
            
            self.logger.info(f"BigQuery query returned DataFrame with {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"BigQuery query failed: {str(e)}")
            raise
    
    async def test_connection(self) -> bool:
        """
        Test BigQuery connection
        """
        try:
            test_query = f"SELECT 1 as test_value"
            result = await self.execute_query(test_query)
            return len(result) == 1 and result[0]['test_value'] == 1
            
        except Exception as e:
            self.logger.error(f"BigQuery connection test failed: {str(e)}")
            return False


class PostgreSQLAdapter(DataSourceAdapter, LoggerMixin):
    """
    PostgreSQL data source adapter  
    Future migration target
    """
    
    def __init__(self, connection_string: str, schema_name: str):
        self.connection_string = connection_string
        self.schema_name = schema_name
        self._pool: Optional[asyncpg.Pool] = None
    
    @property 
    def dataset(self) -> str:
        return self.schema_name
    
    async def _get_pool(self) -> asyncpg.Pool:
        """
        Get or create connection pool
        """
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
        return self._pool
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute PostgreSQL query and return results
        """
        pool = await self._get_pool()
        
        try:
            async with pool.acquire() as conn:
                # Convert BigQuery SQL to PostgreSQL SQL
                pg_query = self._convert_query_to_postgres(query)
                
                # Execute query
                rows = await conn.fetch(pg_query)
                
                # Convert to list of dicts
                result = []
                for row in rows:
                    row_dict = {}
                    for key, value in row.items():
                        # Handle PostgreSQL data types
                        if isinstance(value, (datetime, date)):
                            row_dict[key] = value.isoformat()
                        else:
                            row_dict[key] = value
                    result.append(row_dict)
                
                self.logger.info(f"PostgreSQL query returned {len(result)} rows")
                return result
                
        except Exception as e:
            self.logger.error(f"PostgreSQL query failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            raise
    
    async def execute_query_df(self, query: str) -> pd.DataFrame:
        """
        Execute PostgreSQL query and return DataFrame
        """
        rows = await self.execute_query(query)
        df = pd.DataFrame(rows)
        
        self.logger.info(f"PostgreSQL query returned DataFrame with {len(df)} rows")
        return df
    
    async def test_connection(self) -> bool:
        """
        Test PostgreSQL connection
        """
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
                
        except Exception as e:
            self.logger.error(f"PostgreSQL connection test failed: {str(e)}")
            return False
    
    def _convert_query_to_postgres(self, bigquery_sql: str) -> str:
        """
        Convert BigQuery SQL to PostgreSQL SQL
        Handle common differences between dialects
        """
        # Basic conversions for common BigQuery -> PostgreSQL differences
        postgres_sql = bigquery_sql
        
        # Replace BigQuery dataset references
        postgres_sql = postgres_sql.replace(
            f"`{self.dataset}.", 
            f"{self.schema_name}."
        )
        postgres_sql = postgres_sql.replace("`", '"')
        
        # Replace DATE() function with PostgreSQL equivalent
        postgres_sql = postgres_sql.replace("DATE(", "DATE(")
        
        # Replace CAST(x AS INT64) with CAST(x AS BIGINT)
        postgres_sql = postgres_sql.replace("INT64", "BIGINT")
        
        # Replace SAFE_CAST with regular CAST (handle errors in Python)
        postgres_sql = postgres_sql.replace("SAFE_CAST", "CAST")
        
        # More conversions can be added as needed
        
        return postgres_sql
    
    async def close(self):
        """
        Close connection pool
        """
        if self._pool:
            await self._pool.close()


class DataSourceFactory:
    """
    Factory for creating data source adapters
    Simplifies switching between BigQuery and Postgres
    """
    
    @staticmethod
    def create_adapter(source_type: str = None) -> DataSourceAdapter:
        """
        Create appropriate data source adapter based on configuration
        """
        settings = get_settings()
        
        if source_type is None:
            source_type = settings.DATA_SOURCE_TYPE
        
        if source_type.lower() == 'bigquery':
            return BigQueryAdapter(
                project_id=settings.BIGQUERY_PROJECT_ID,
                dataset_name=settings.BIGQUERY_DATASET
            )
        
        elif source_type.lower() == 'postgresql':
            return PostgreSQLAdapter(
                connection_string=settings.POSTGRES_URL,
                schema_name=settings.POSTGRES_SCHEMA
            )
        
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")
    
    @staticmethod
    async def test_all_connections() -> Dict[str, bool]:
        """
        Test all configured data sources
        """
        results = {}
        
        try:
            bigquery_adapter = DataSourceFactory.create_adapter('bigquery')
            results['bigquery'] = await bigquery_adapter.test_connection()
        except Exception as e:
            results['bigquery'] = False
            
        try:
            postgres_adapter = DataSourceFactory.create_adapter('postgresql')
            results['postgresql'] = await postgres_adapter.test_connection()
            if hasattr(postgres_adapter, 'close'):
                await postgres_adapter.close()
        except Exception as e:
            results['postgresql'] = False
        
        return results


# =============================================================================
# QUERY BUILDER FOR COMMON PATTERNS
# =============================================================================

class QueryBuilder:
    """
    Helper for building database-agnostic queries
    Reduces the need for query conversion
    """
    
    def __init__(self, adapter: DataSourceAdapter):
        self.adapter = adapter
        
    def select_asignaciones(
        self, 
        fecha_inicio: date, 
        fecha_fin: date,
        filters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Build asignaciones query
        """
        base_query = f"""
            SELECT 
                archivo, cod_luna, cuenta, min_vto, negocio,
                telefono, tramo_gestion, decil_contacto, decil_pago,
                creado_el, nro_documento
            FROM {self._get_table_ref('batch_P3fV4dWNeMkN5RJMhV8e_asignacion')}
            WHERE {self._date_filter('creado_el', fecha_inicio, fecha_fin)}
        """
        
        if filters:
            filter_conditions = self._build_filter_conditions(filters, 'asignaciones')
            if filter_conditions:
                base_query += f" AND {filter_conditions}"
                
        return base_query
    
    def select_tran_deuda(
        self, 
        fecha_inicio: date, 
        fecha_fin: date
    ) -> str:
        """
        Build tran_deuda query
        """
        return f"""
            SELECT 
                cod_cuenta, nro_documento, fecha_vencimiento,
                monto_exigible, creado_el
            FROM {self._get_table_ref('batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda')}
            WHERE {self._date_filter('creado_el', fecha_inicio, fecha_fin)}
        """
    
    def select_gestiones_bot(
        self, 
        fecha_inicio: date, 
        fecha_fin: date
    ) -> str:
        """
        Build gestiones bot query
        """
        return f"""
            SELECT 
                {self._cast_to_int('document')} as cod_luna,
                date as fecha_gestion,
                management, sub_management, compromiso
            FROM {self._get_table_ref('voicebot_P3fV4dWNeMkN5RJMhV8e')}
            WHERE {self._date_filter('date', fecha_inicio, fecha_fin)}
        """
    
    def select_gestiones_humano(
        self, 
        fecha_inicio: date, 
        fecha_fin: date
    ) -> str:
        """
        Build gestiones humano query
        """
        return f"""
            SELECT 
                {self._cast_to_int('document')} as cod_luna,
                date as fecha_gestion,
                management, sub_management, n3 as compromiso
            FROM {self._get_table_ref('mibotair_P3fV4dWNeMkN5RJMhV8e')}
            WHERE {self._date_filter('date', fecha_inicio, fecha_fin)}
        """
    
    def select_pagos(
        self, 
        fecha_inicio: date, 
        fecha_fin: date
    ) -> str:
        """
        Build pagos query
        """
        return f"""
            SELECT 
                nro_documento, fecha_pago, monto_cancelado, creado_el
            FROM {self._get_table_ref('batch_P3fV4dWNeMkN5RJMhV8e_pagos')}
            WHERE {self._date_filter('fecha_pago', fecha_inicio, fecha_fin)}
        """
    
    def _get_table_ref(self, table_name: str) -> str:
        """
        Get table reference for current adapter
        """
        if isinstance(self.adapter, BigQueryAdapter):
            return f"`{self.adapter.dataset}.{table_name}`"
        else:
            return f"{self.adapter.dataset}.{table_name}"
    
    def _date_filter(self, date_column: str, fecha_inicio: date, fecha_fin: date) -> str:
        """
        Build date filter for current adapter
        """
        if isinstance(self.adapter, BigQueryAdapter):
            return f"DATE({date_column}) >= '{fecha_inicio}' AND DATE({date_column}) <= '{fecha_fin}'"
        else:
            return f"DATE({date_column}) >= '{fecha_inicio}' AND DATE({date_column}) <= '{fecha_fin}'"
    
    def _cast_to_int(self, column: str) -> str:
        """
        Cast column to integer for current adapter
        """
        if isinstance(self.adapter, BigQueryAdapter):
            return f"CAST({column} AS INT64)"
        else:
            return f"CAST({column} AS BIGINT)"
    
    def _build_filter_conditions(self, filters: Dict[str, Any], table_type: str) -> str:
        """
        Build filter conditions for query
        """
        # This can be enhanced based on specific filter requirements
        conditions = []
        
        if table_type == 'asignaciones':
            if 'archivo' in filters and filters['archivo']:
                archivo_list = "', '".join(filters['archivo'])
                conditions.append(f"archivo IN ('{archivo_list}')")
                
            if 'servicio' in filters and filters['servicio']:
                servicio_list = "', '".join(filters['servicio'])
                conditions.append(f"negocio IN ('{servicio_list}')")
        
        return " AND ".join(conditions)
