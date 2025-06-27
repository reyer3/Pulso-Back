"""
📊 Dashboard Service V2 - Python-First Approach
Database-agnostic dashboard service using raw tables + Python processing
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from app.core.logging import LoggerMixin
from app.services.data_processor import DataProcessor


class DashboardServiceV2(LoggerMixin):
    """
    Dashboard service using raw tables + Python processing
    Designed for easy BigQuery -> Postgres migration
    """
    
    def __init__(self, data_source_adapter):
        """
        Initialize with data source adapter (BigQuery or Postgres)
        """
        self.data_source = data_source_adapter
        self.processor = DataProcessor()
        
    # =============================================================================
    # MAIN DASHBOARD DATA GENERATION
    # =============================================================================
    
    async def get_dashboard_data(
        self,
        filters: Dict[str, Any],
        fecha_corte: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Generate dashboard data using Python processing
        
        Args:
            filters: Dictionary with filter criteria
            fecha_corte: Cut-off date for analysis
            
        Returns:
            Dashboard data structure matching React frontend needs
        """
        if fecha_corte is None:
            fecha_corte = date.today()
            
        self.logger.info(f"Generating dashboard data for {fecha_corte} with filters: {filters}")
        
        # 1. Extract raw data from tables
        raw_data = await self._extract_raw_data(filters, fecha_corte)
        
        # 2. Process each data source
        processed_data = self._process_raw_data(raw_data)
        
        # 3. Calculate account-level metrics
        account_metrics = self._calculate_account_metrics(processed_data, fecha_corte)
        
        # 4. Aggregate for dashboard views
        dashboard_data = self._aggregate_dashboard_metrics(account_metrics, filters)
        
        self.logger.info(f"Generated dashboard data with {len(account_metrics)} accounts")
        
        return dashboard_data
    
    # =============================================================================
    # RAW DATA EXTRACTION (Simple queries)
    # =============================================================================
    
    async def _extract_raw_data(
        self, 
        filters: Dict[str, Any], 
        fecha_corte: date
    ) -> Dict[str, List[Dict]]:
        """
        Extract raw data with simple queries
        Processing logic stays in Python
        """
        # Date range for data extraction
        fecha_inicio = fecha_corte - timedelta(days=90)  # 3 months back
        
        # Simple queries - complexity handled in Python
        queries = {
            'asignaciones': f"""
                SELECT 
                    archivo, cod_luna, cuenta, min_vto, negocio,
                    telefono, tramo_gestion, decil_contacto, decil_pago,
                    creado_el, nro_documento
                FROM `{self.data_source.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
                WHERE DATE(creado_el) >= '{fecha_inicio}'
                  AND DATE(creado_el) <= '{fecha_corte}'
            """,
            
            'tran_deuda': f"""
                SELECT 
                    cod_cuenta, nro_documento, fecha_vencimiento,
                    monto_exigible, creado_el
                FROM `{self.data_source.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
                WHERE DATE(creado_el) >= '{fecha_inicio}'
                  AND DATE(creado_el) <= '{fecha_corte}'
            """,
            
            'gestiones_bot': f"""
                SELECT 
                    CAST(document AS INT64) as cod_luna,
                    date as fecha_gestion,
                    management, sub_management, compromiso
                FROM `{self.data_source.dataset}.voicebot_P3fV4dWNeMkN5RJMhV8e`
                WHERE DATE(date) >= '{fecha_inicio}'
                  AND DATE(date) <= '{fecha_corte}'
            """,
            
            'gestiones_humano': f"""
                SELECT 
                    CAST(document AS INT64) as cod_luna,
                    date as fecha_gestion,
                    management, sub_management, n3 as compromiso
                FROM `{self.data_source.dataset}.mibotair_P3fV4dWNeMkN5RJMhV8e`
                WHERE DATE(date) >= '{fecha_inicio}'
                  AND DATE(date) <= '{fecha_corte}'
            """,
            
            'pagos': f"""
                SELECT 
                    nro_documento, fecha_pago, monto_cancelado, creado_el
                FROM `{self.data_source.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
                WHERE DATE(fecha_pago) >= '{fecha_inicio}'
                  AND DATE(fecha_pago) <= '{fecha_corte}'
            """
        }
        
        # Execute queries and return raw data
        raw_data = {}
        for table_name, query in queries.items():
            # Apply filters to query if needed
            filtered_query = self._apply_filters_to_query(query, filters, table_name)
            raw_data[table_name] = await self.data_source.execute_query(filtered_query)
            
        return raw_data
    
    def _apply_filters_to_query(
        self, 
        base_query: str, 
        filters: Dict[str, Any], 
        table_name: str
    ) -> str:
        """
        Apply filters to base query
        Keep queries simple, filters are basic WHERE clauses
        """
        # Start with base query
        query = base_query
        
        # Add filters based on table type
        if table_name == 'asignaciones' and filters:
            filter_conditions = []
            
            if 'cartera' in filters and filters['cartera']:
                cartera_conditions = []
                for cartera in filters['cartera']:
                    if cartera == 'TEMPRANA':
                        cartera_conditions.append("UPPER(archivo) LIKE '%TEMPRANA%'")
                    elif cartera == 'ALTAS_NUEVAS':
                        cartera_conditions.append("(UPPER(archivo) LIKE '%AN%' OR UPPER(archivo) LIKE '%ALTAS%')")
                    elif cartera == 'CUOTA_FRACCIONAMIENTO':
                        cartera_conditions.append("UPPER(archivo) LIKE '%CF_ANN%'")
                
                if cartera_conditions:
                    filter_conditions.append(f"({' OR '.join(cartera_conditions)})")
            
            if 'servicio' in filters and filters['servicio']:
                if 'MOVIL' in filters['servicio'] and 'FIJA' not in filters['servicio']:
                    filter_conditions.append("negocio = 'MOVIL'")
                elif 'FIJA' in filters['servicio'] and 'MOVIL' not in filters['servicio']:
                    filter_conditions.append("negocio != 'MOVIL'")
            
            # Add filter conditions to query
            if filter_conditions:
                query += " AND " + " AND ".join(filter_conditions)
        
        return query
    
    # =============================================================================
    # DATA PROCESSING (Using DataProcessor)
    # =============================================================================
    
    def _process_raw_data(self, raw_data: Dict[str, List[Dict]]) -> Dict[str, pd.DataFrame]:
        """
        Process raw data using DataProcessor
        """
        processed = {}
        
        # Process each data source
        processed['asignaciones'] = self.processor.process_asignaciones(
            raw_data.get('asignaciones', [])
        )
        
        processed['tran_deuda'] = self.processor.process_tran_deuda(
            raw_data.get('tran_deuda', [])
        )
        
        processed['gestiones'] = self.processor.process_gestiones(
            raw_data.get('gestiones_bot', []),
            raw_data.get('gestiones_humano', [])
        )
        
        processed['pagos'] = self.processor.process_pagos(
            raw_data.get('pagos', [])
        )
        
        return processed
    
    def _calculate_account_metrics(
        self, 
        processed_data: Dict[str, pd.DataFrame], 
        fecha_corte: date
    ) -> pd.DataFrame:
        """
        Calculate account-level metrics using DataProcessor
        """
        return self.processor.calculate_account_metrics(
            asignaciones_df=processed_data.get('asignaciones', pd.DataFrame()),
            gestiones_df=processed_data.get('gestiones', pd.DataFrame()),
            pagos_df=processed_data.get('pagos', pd.DataFrame()),
            deuda_df=processed_data.get('tran_deuda', pd.DataFrame()),
            fecha_corte=fecha_corte
        )
    
    # =============================================================================
    # DASHBOARD AGGREGATIONS (Business Logic)
    # =============================================================================
    
    def _aggregate_dashboard_metrics(
        self, 
        account_metrics: pd.DataFrame, 
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate account metrics for dashboard views
        Matches the React frontend data structure
        """
        if account_metrics.empty:
            return self._empty_dashboard_response()
        
        # Generate different aggregation levels
        dashboard_data = {
            'segmentoData': self._aggregate_by_segments(account_metrics),
            'negocioData': self._aggregate_by_service(account_metrics),
            'integralChartData': self._generate_integral_chart_data(account_metrics),
            'metadata': {
                'lastRefresh': datetime.now().isoformat(),
                'totalRecords': len(account_metrics),
                'filters': filters
            }
        }
        
        return dashboard_data
    
    def _aggregate_by_segments(self, df: pd.DataFrame) -> List[Dict]:
        """
        Aggregate metrics by cartera + vencimiento segments
        """
        if df.empty:
            return []
        
        # Group by cartera and vencimiento_grupo
        segments = df.groupby(['cartera', 'vencimiento_grupo']).agg({
            'cuenta': 'count',
            'deuda_inicial': 'sum',
            'deuda_actual': 'sum',
            'gestionado': 'sum',
            'contactado': 'sum',
            'contacto_directo': 'sum',
            'contacto_indirecto': 'sum',
            'sin_contacto': 'sum',
            'compromisos': 'sum',
            'pagador': 'sum',
            'recupero_total': 'sum',
            'total_gestiones': 'sum'
        }).reset_index()
        
        # Calculate percentages and KPIs
        result = []
        for _, row in segments.iterrows():
            segment_data = self._calculate_segment_kpis(row)
            segment_data['id'] = f"{row['cartera']}_{row['vencimiento_grupo']}"
            segment_data['name'] = f"{row['cartera']} {row['vencimiento_grupo']}"
            result.append(segment_data)
        
        return result
    
    def _aggregate_by_service(self, df: pd.DataFrame) -> List[Dict]:
        """
        Aggregate metrics by service type (MOVIL/FIJA)
        """
        if df.empty:
            return []
        
        # Group by servicio
        services = df.groupby('servicio').agg({
            'cuenta': 'count',
            'deuda_inicial': 'sum',
            'deuda_actual': 'sum',
            'gestionado': 'sum',
            'contactado': 'sum',
            'contacto_directo': 'sum',
            'contacto_indirecto': 'sum',
            'sin_contacto': 'sum',
            'compromisos': 'sum',
            'pagador': 'sum',
            'recupero_total': 'sum',
            'total_gestiones': 'sum'
        }).reset_index()
        
        # Calculate percentages and KPIs
        result = []
        for _, row in services.iterrows():
            service_data = self._calculate_segment_kpis(row)
            service_data['id'] = row['servicio']
            service_data['name'] = row['servicio']
            result.append(service_data)
        
        return result
    
    def _calculate_segment_kpis(self, row: pd.Series) -> Dict[str, Any]:
        """
        Calculate KPIs for a segment (shared logic)
        """
        cuentas = row['cuenta'] if row['cuenta'] > 0 else 1  # Avoid division by zero
        
        return {
            'cuentas': int(row['cuenta']),
            'porcentajeCuentas': 0,  # Will be calculated at higher level
            'deudaAsig': float(row['deuda_inicial']),
            'porcentajeDeuda': 0,  # Will be calculated at higher level  
            'deudaAct': float(row['deuda_actual']),
            'porcentajeDeudaAct': 0,  # Will be calculated at higher level
            'cobertura': round((row['gestionado'] / cuentas) * 100, 1),
            'contacto': round((row['contactado'] / cuentas) * 100, 1),
            'cd': round((row['contacto_directo'] / cuentas) * 100, 1),
            'ci': round((row['contacto_indirecto'] / cuentas) * 100, 1),
            'sc': round((row['sin_contacto'] / cuentas) * 100, 1),
            'cierre': round((row['pagador'] / cuentas) * 100, 1),
            'inten': round(row['total_gestiones'] / max(row['gestionado'], 1), 1),
            'cdCount': int(row['contacto_directo']),
            'ciCount': int(row['contacto_indirecto']),
            'scCount': int(row['sin_contacto']),
            'sgCount': int(cuentas - row['gestionado']),
            'pdpCount': int(row['compromisos']),
            'fracCount': 0,  # TODO: Add fraccionamiento logic
            'pdpFracCount': int(row['compromisos']),
            'status': 'ok'  # TODO: Add status logic based on targets
        }
    
    def _generate_integral_chart_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Generate data for integral KPI charts
        """
        if df.empty:
            return []
        
        # This could be by different dimensions based on filters
        # For now, aggregate by cartera
        chart_data = df.groupby('cartera').agg({
            'cuenta': 'count',
            'gestionado': 'sum',
            'contactado': 'sum',
            'contacto_directo': 'sum',
            'contacto_indirecto': 'sum',
            'pagador': 'sum',
            'total_gestiones': 'sum'
        }).reset_index()
        
        result = []
        for _, row in chart_data.iterrows():
            cuentas = row['cuenta'] if row['cuenta'] > 0 else 1
            result.append({
                'name': row['cartera'],
                'cobertura': round((row['gestionado'] / cuentas) * 100, 1),
                'contacto': round((row['contactado'] / cuentas) * 100, 1),
                'contactoDirecto': round((row['contacto_directo'] / cuentas) * 100, 1),
                'contactoIndirecto': round((row['contacto_indirecto'] / cuentas) * 100, 1),
                'tasaDeCierre': round((row['pagador'] / cuentas) * 100, 1),
                'intensidad': round(row['total_gestiones'] / max(row['gestionado'], 1), 1)
            })
        
        return result
    
    def _empty_dashboard_response(self) -> Dict[str, Any]:
        """
        Return empty dashboard structure
        """
        return {
            'segmentoData': [],
            'negocioData': [],
            'integralChartData': [],
            'metadata': {
                'lastRefresh': datetime.now().isoformat(),
                'totalRecords': 0,
                'filters': {}
            }
        }
