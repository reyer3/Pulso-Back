"""
🔌 Data Source Adapters y Factory - producción
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd
from datetime import date, datetime

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

# --- QUERY BUILDER PARA COMPATIBILIDAD CROSS-DATABASE ---

class QueryBuilder(LoggerMixin):
    """
    Database-agnostic query builder
    Builds SQL queries that work across BigQuery and PostgreSQL
    """
    
    def __init__(self, data_adapter: DataSourceAdapter):
        self.data_adapter = data_adapter
        self.dataset = data_adapter.dataset
        
    def select_asignaciones(
        self, 
        fecha_inicio: date, 
        fecha_fin: date, 
        filters: Dict[str, Any]
    ) -> str:
        """Build query for asignaciones data"""
        
        # Base query structure
        query = f"""
        SELECT 
            archivo,
            fecha_asignacion as fecha_asignacion_real,
            cartera,
            negocio as servicio,
            cod_luna,
            cuenta,
            deuda_inicial,
            deuda_inicial as monto_exigible_diario,
            CASE 
                WHEN deuda_inicial >= 1 THEN 'GESTIONABLE'
                ELSE 'NO_GESTIONABLE' 
            END as estado_deudor
        FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_asignaciones_clean`
        WHERE fecha_asignacion >= '{fecha_inicio.isoformat()}'
          AND fecha_asignacion <= '{fecha_fin.isoformat()}'
        """
        
        # Add filters
        if filters.get('archivo'):
            query += f" AND archivo IN {self._format_list(filters['archivo'])}"
        if filters.get('cartera'):
            query += f" AND cartera IN {self._format_list(filters['cartera'])}"
        if filters.get('servicio'):
            query += f" AND negocio IN {self._format_list(filters['servicio'])}"
            
        return query
    
    def select_tran_deuda(self, fecha_inicio: date, fecha_fin: date) -> str:
        """Build query for trandeuda data"""
        
        return f"""
        SELECT 
            nro_documento as cuenta,
            fecha_proceso,
            deuda_total as monto_exigible_diario
        FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE fecha_proceso >= '{fecha_inicio.isoformat()}'
          AND fecha_proceso <= '{fecha_fin.isoformat()}'
          AND deuda_total > 0
        """
    
    def select_gestiones_bot(self, fecha_inicio: date, fecha_fin: date) -> str:
        """Build query for bot gestiones"""
        
        return f"""
        SELECT 
            codLuna as cod_luna,
            DATE(fecha_creacion) as fecha_gestion,
            campaign_name,
            'BOT' as canal_origen,
            flujo_final,
            CASE 
                WHEN flujo_final IN ('CD', 'CI') THEN TRUE 
                ELSE FALSE 
            END as es_contacto_efectivo,
            CASE 
                WHEN flujo_final IN ('CD') THEN TRUE 
                ELSE FALSE 
            END as es_contacto_directo,
            CASE 
                WHEN flujo_final IN ('PDP') THEN TRUE 
                ELSE FALSE 
            END as es_compromiso
        FROM `{self.dataset}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE DATE(fecha_creacion) >= '{fecha_inicio.isoformat()}'
          AND DATE(fecha_creacion) <= '{fecha_fin.isoformat()}'
        """
    
    def select_gestiones_humano(self, fecha_inicio: date, fecha_fin: date) -> str:
        """Build query for human gestiones"""
        
        return f"""
        SELECT 
            cod_luna,
            DATE(fecha_inicio_gestion) as fecha_gestion,
            campana as campaign_name,
            'HUMANO' as canal_origen,
            tipo_resultado as flujo_final,
            correo_agente,
            nombre_agente,
            CASE 
                WHEN tipo_resultado IN ('CONTACTO_EFECTIVO', 'CD', 'CI') THEN TRUE 
                ELSE FALSE 
            END as es_contacto_efectivo,
            CASE 
                WHEN tipo_resultado IN ('CONTACTO_EFECTIVO', 'CD') THEN TRUE 
                ELSE FALSE 
            END as es_contacto_directo,
            CASE 
                WHEN tipo_resultado IN ('PDP', 'COMPROMISO') THEN TRUE 
                ELSE FALSE 
            END as es_compromiso
        FROM `{self.dataset}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE DATE(fecha_inicio_gestion) >= '{fecha_inicio.isoformat()}'
          AND DATE(fecha_inicio_gestion) <= '{fecha_fin.isoformat()}'
        """
    
    def select_pagos(self, fecha_inicio: date, fecha_fin: date) -> str:
        """Build query for pagos data"""
        
        return f"""
        SELECT DISTINCT
            nro_documento as cuenta,
            fecha_pago,
            monto_cancelado as monto_recuperado
        FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE fecha_pago >= '{fecha_inicio.isoformat()}'
          AND fecha_pago <= '{fecha_fin.isoformat()}'
          AND monto_cancelado > 0
        """
    
    def _format_list(self, values: List[str]) -> str:
        """Format list of values for SQL IN clause"""
        if isinstance(values, str):
            values = [values]
        formatted_values = [f"'{v}'" for v in values]
        return f"({', '.join(formatted_values)})"

# --- IMPLEMENTACIÓN BIGQUERY ---

from google.cloud import bigquery
import asyncio

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
