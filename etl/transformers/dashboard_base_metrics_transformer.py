"""
📊 Dashboard Base Metrics Transformer (BigQuery POC)
Conecta directamente a BigQuery para POC de 2 horas
Genera métricas base granulares para cálculos dinámicos en frontend
"""

from datetime import date, datetime, timezone
from typing import Dict, Any
import pandas as pd
from google.cloud import bigquery

from etl.transformers.mart_transformer_base import MartTransformerBase


class DashboardBaseMetricsTransformer(MartTransformerBase):
    """
    POC Transformer que conecta directamente a BigQuery
    Genera métricas base para cálculos dinámicos de KPIs en frontend
    """
    
    def __init__(self, project_uid: str = None):
        super().__init__(project_uid)
        # For POC: Direct BigQuery connection
        self.bq_client = bigquery.Client(project="mibot-222814")
    
    def get_sql_filename(self) -> str:
        return "build_dashboard_base_metrics_bq_poc.sql"
    
    def get_mart_table_name(self) -> str:
        return "dashboard_base_metrics"
    
    def _load_source_data(self, engine, fecha_proceso: date, archivo: str = None, **kwargs) -> pd.DataFrame:
        """
        Override to use direct BigQuery connection for POC
        """
        sql_file = self.sql_path / self.get_sql_filename()
        
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            query = f.read()
        
        # Replace parameters for BigQuery
        archivo_value = f"'{archivo}'" if archivo else "NULL"
        query = query.replace("{fecha_proceso}", f"'{fecha_proceso}'")
        query = query.replace("{archivo}", archivo_value)
        
        self.logger.info(f"🔍 Executing BigQuery POC query for fecha_proceso={fecha_proceso}, archivo={archivo}")
        
        # Execute on BigQuery
        try:
            df = self.bq_client.query(query).to_dataframe()
            self.logger.info(f"📊 BigQuery returned {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"❌ BigQuery query failed: {str(e)}")
            raise
    
    def transform_with_pandas(self, df: pd.DataFrame, fecha_proceso: date, **kwargs) -> pd.DataFrame:
        """
        Transform BigQuery results to final mart format
        NO pre-calculated KPIs - only base metrics for dynamic calculation
        """
        if df.empty:
            return df
        
        self.logger.info(f"📊 Processing {len(df)} metric rows for base dashboard data")
        
        # Add metadata columns
        df = self._add_metadata_columns(df, fecha_proceso)
        
        # Apply minimal validations (no KPI calculations)
        df = self._apply_base_validations(df)
        
        # Ensure proper data types
        df = self._ensure_data_types(df)
        
        return df
    
    def _add_metadata_columns(self, df: pd.DataFrame, fecha_proceso: date) -> pd.DataFrame:
        """Add metadata columns"""
        current_time = datetime.now(timezone.utc)
        
        df['fecha_procesamiento'] = current_time
        df['created_at'] = current_time
        df['updated_at'] = current_time
        
        return df
    
    def _apply_base_validations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply basic validations to base metrics"""
        initial_count = len(df)
        
        # Remove negative counts
        count_cols = [
            'cuentas_asignadas', 'clientes_asignados', 'cuentas_gestionables',
            'cuentas_gestionadas', 'total_gestiones', 'contactos_efectivos_total',
            'cuentas_con_contacto_directo', 'cuentas_con_compromiso', 'cuentas_pagadoras'
        ]
        
        for col in count_cols:
            if col in df.columns:
                df = df[df[col] >= 0]
        
        # Remove negative financial values
        financial_cols = ['deuda_asignada', 'deuda_actual', 'recupero_total']
        for col in financial_cols:
            if col in df.columns:
                df = df[df[col] >= 0]
        
        # Business logic validation: gestionadas <= asignadas
        if 'cuentas_gestionadas' in df.columns and 'cuentas_asignadas' in df.columns:
            df = df[df['cuentas_gestionadas'] <= df['cuentas_asignadas']]
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            self.logger.warning(f"📊 Filtered {filtered_count} invalid records from base metrics")
        
        return df
    
    def _ensure_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure proper data types for mart table"""
        
        # String columns
        string_cols = ['archivo', 'cartera', 'servicio', 'periodo', 'tipo_segmento', 'negocio', 'rango_vencimiento', 'zona']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        # Integer columns
        int_cols = [
            'cuentas_asignadas', 'clientes_asignados', 'cuentas_gestionables',
            'cuentas_gestionadas', 'total_gestiones', 'contactos_efectivos_total',
            'contactos_no_efectivos_total', 'cuentas_con_contacto_directo',
            'cuentas_con_contacto_indirecto', 'cuentas_con_contacto_total',
            'cuentas_con_compromiso', 'total_compromisos', 'gestiones_bot_total',
            'gestiones_humano_total', 'cuentas_pagadoras', 'pagos_totales'
        ]
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        # Float columns  
        float_cols = ['deuda_asignada', 'deuda_actual', 'peso_bot_total', 'peso_humano_total', 'recupero_total']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.0).astype(float)
        
        # Date columns
        if 'fecha_foto' in df.columns:
            df['fecha_foto'] = pd.to_datetime(df['fecha_foto']).dt.date
        
        return df
    
    def execute_transformation(self, engine, fecha_proceso: date, archivo: str = None, **kwargs) -> Dict[str, Any]:
        """
        Override to bypass PostgreSQL engine and use BigQuery directly
        """
        try:
            table_name = self.get_mart_table_name()
            self.logger.info(f"🚀 Starting BigQuery POC {table_name} transformation for {fecha_proceso}")
            
            # 1. Extract from BigQuery (bypassing engine)
            df_source = self._load_source_data(None, fecha_proceso, archivo, **kwargs)
            
            if df_source.empty:
                self.logger.warning(f"No source data for {fecha_proceso}")
                return {"status": "no_data", "records": 0}
            
            # 2. Transform with pandas
            df_transformed = self.transform_with_pandas(df_source, fecha_proceso, **kwargs)
            
            if df_transformed.empty:
                self.logger.warning(f"No data after transformation")
                return {"status": "no_data_after_transform", "records": 0}
            
            # 3. For POC: Just log results (no PostgreSQL load)
            self.logger.info(f"✅ POC transformation completed: {len(df_transformed)} records")
            self.logger.info(f"📊 Sample dimensions: {df_transformed['cartera'].unique()[:5].tolist()}")
            self.logger.info(f"🎯 Sample metrics: cuentas_asignadas={df_transformed['cuentas_asignadas'].sum()}")
            
            # Store results for API access (in production this would go to PostgreSQL)
            self._store_poc_results(df_transformed, fecha_proceso, archivo)
            
            return {
                "status": "success",
                "records": len(df_transformed),
                "source_records": len(df_source),
                "transformed_records": len(df_transformed),
                "sample_data": df_transformed.head(3).to_dict('records')
            }
            
        except Exception as e:
            self.logger.error(f"❌ BigQuery POC transformation failed: {str(e)}")
            raise
    
    def _store_poc_results(self, df: pd.DataFrame, fecha_proceso: date, archivo: str = None):
        """
        Store POC results for API access
        In production this would be replaced by PostgreSQL load
        """
        # For POC: Could store in memory, file, or return directly to API
        # This is where we'd implement the storage strategy for the 2-hour POC
        self.logger.info(f"📦 POC results stored: {len(df)} records for {fecha_proceso}")
        
        # Example: Save to CSV for quick inspection
        # output_file = f"/tmp/dashboard_base_metrics_{fecha_proceso}_{archivo or 'all'}.csv"
        # df.to_csv(output_file, index=False)
        # self.logger.info(f"💾 POC data saved to {output_file}")
