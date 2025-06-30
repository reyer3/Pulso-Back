"""
🎯 Mart Base Transformer
Sigue arquitectura existente: BQ -> RAW -> AUX -> MART
SRP: Una responsabilidad - orquestación base de transformaciones mart
"""

from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from shared.core.logging import LoggerMixin
from etl.config import ETLConfig


class MartTransformerBase(LoggerMixin, ABC):
    """
    Base transformer siguiendo patrones existentes del codebase
    KISS: Orchestración simple, SQL en archivos separados, lógica Python para cálculos complejos
    """
    
    def __init__(self, project_uid: str = None):
        super().__init__()
        self.project_uid = project_uid or ETLConfig.PROJECT_UID
        self.sql_path = Path(__file__).parent.parent / "sql" / "mart"
        self.schemas = {
            'raw_schema': f"raw_{self.project_uid}",
            'aux_schema': f"aux_{self.project_uid}",
            'mart_schema': f"mart_{self.project_uid}"
        }
    
    @abstractmethod
    def get_sql_filename(self) -> str:
        """Devuelve el nombre del archivo SQL correspondiente"""
        pass
    
    @abstractmethod
    def transform_with_pandas(self, df: pd.DataFrame, fecha_proceso: date, **kwargs) -> pd.DataFrame:
        """
        Aplica transformaciones complejas con pandas
        AQUÍ es donde Python brilla vs SQL puro
        """
        pass
    
    @abstractmethod
    def get_mart_table_name(self) -> str:
        """Devuelve nombre de la tabla mart destino"""
        pass
    
    def execute_transformation(self, engine: Engine, fecha_proceso: date, archivo: str = None, **kwargs) -> Dict[str, Any]:
        """
        Ejecuta transformación completa siguiendo patrón: SQL -> pandas -> load
        """
        try:
            table_name = self.get_mart_table_name()
            self.logger.info(f"🚀 Starting {table_name} transformation for {fecha_proceso}")
            
            # 1. Extract: Cargar datos usando SQL
            df_source = self._load_source_data(engine, fecha_proceso, archivo, **kwargs)
            
            if df_source.empty:
                self.logger.warning(f"No source data for {fecha_proceso}")
                return {"status": "no_data", "records": 0}
            
            # 2. Transform: Aplicar lógica Python compleja
            df_transformed = self.transform_with_pandas(df_source, fecha_proceso, **kwargs)
            
            if df_transformed.empty:
                self.logger.warning(f"No data after transformation")
                return {"status": "no_data_after_transform", "records": 0}
            
            # 3. Load: Cargar a tabla mart
            records_loaded = self._load_to_mart(engine, df_transformed, fecha_proceso, archivo)
            
            self.logger.info(f"✅ {table_name} transformation completed: {records_loaded} records")
            
            return {
                "status": "success",
                "records": records_loaded,
                "source_records": len(df_source),
                "transformed_records": len(df_transformed)
            }
            
        except Exception as e:
            self.logger.error(f"❌ {table_name} transformation failed: {str(e)}")
            raise
    
    def _load_source_data(self, engine: Engine, fecha_proceso: date, archivo: str = None, **kwargs) -> pd.DataFrame:
        """
        Carga datos usando SQL file siguiendo patrón existente
        """
        sql_file = self.sql_path / self.get_sql_filename()
        
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            query = f.read()
        
        # Format schemas siguiendo patrón de mart_build_pipeline.py
        query = query.format(**self.schemas)
        
        # Replace parameters
        query = self._replace_sql_parameters(query, fecha_proceso, archivo, **kwargs)
        
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    
    def _replace_sql_parameters(self, query: str, fecha_proceso: date, archivo: str = None, **kwargs) -> str:
        """
        Reemplaza parámetros en query SQL
        """
        # Handle archivo filter (NULL si no se especifica)
        archivo_value = f"'{archivo}'" if archivo else "NULL"
        
        replacements = {
            "{fecha_proceso}": f"'{fecha_proceso}'",
            "{archivo}": archivo_value
        }
        
        for key, value in replacements.items():
            query = query.replace(key, value)
        
        return query
    
    def _load_to_mart(self, engine: Engine, df: pd.DataFrame, fecha_proceso: date, archivo: str = None) -> int:
        """
        Carga datos transformados a tabla mart con patrón de limpieza idempotente
        """
        if df.empty:
            return 0
        
        table_name = self.get_mart_table_name()
        full_table = f"{self.schemas['mart_schema']}.{table_name}"
        
        with engine.connect() as conn:
            # Delete existing data for idempotency (siguiendo patrón de mart_build_pipeline)
            if archivo:
                delete_query = text(f"""
                    DELETE FROM {full_table} 
                    WHERE fecha_foto = :fecha AND archivo = :archivo
                """)
                conn.execute(delete_query, {"fecha": fecha_proceso, "archivo": archivo})
            else:
                delete_query = text(f"""
                    DELETE FROM {full_table} 
                    WHERE fecha_foto = :fecha
                """)
                conn.execute(delete_query, {"fecha": fecha_proceso})
            
            # Insert new data using pandas to_sql
            df.to_sql(
                name=table_name,
                con=conn,
                schema=self.schemas['mart_schema'],
                if_exists='append',
                index=False,
                method='multi'
            )
            
            conn.commit()
        
        return len(df)
    
    @staticmethod
    def _safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
        """Safe division siguiendo patrón de raw_data_transformer"""
        if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
            return default
        return float(numerator / denominator)
    
    @staticmethod
    def _calculate_percentage(part: float, total: float, default: float = 0.0) -> float:
        """Calculate percentage with safe division"""
        result = MartTransformerBase._safe_divide(part, total, default) * 100
        return round(result, 2)
