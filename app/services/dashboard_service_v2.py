"""
📊 Dashboard Service V2 - Python-First Approach
Database-agnostic dashboard service using raw tables + Python processing
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from app.core.logging import LoggerMixin
from app.services.data_processor import DataProcessor
from app.repositories.data_adapters import DataSourceAdapter, QueryBuilder


class DashboardServiceV2(LoggerMixin):
    """
    Dashboard service using raw tables + Python processing
    Designed for easy BigQuery -> Postgres migration
    """
    
    def __init__(self, data_adapter: DataSourceAdapter):
        """
        Initialize with data source adapter (BigQuery or Postgres)
        """
        self.data_adapter = data_adapter
        self.query_builder = QueryBuilder(data_adapter)
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
        
        # 1. Extract raw data from tables using adapters
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
    # RAW DATA EXTRACTION (Using QueryBuilder)
    # =============================================================================
    
    async def _extract_raw_data(
        self, 
        filters: Dict[str, Any], 
        fecha_corte: date
    ) -> Dict[str, List[Dict]]:
        """
        Extract raw data using QueryBuilder for database-agnostic queries
        """
        # Date range for data extraction
        fecha_inicio = fecha_corte - timedelta(days=90)  # 3 months back
        
        self.logger.info(f"Extracting data from {fecha_inicio} to {fecha_corte}")
        
        # Build queries using QueryBuilder
        queries = {
            'asignaciones': self.query_builder.select_asignaciones(
                fecha_inicio, fecha_corte, filters
            ),
            'tran_deuda': self.query_builder.select_tran_deuda(
                fecha_inicio, fecha_corte
            ),
            'gestiones_bot': self.query_builder.select_gestiones_bot(
                fecha_inicio, fecha_corte
            ),
            'gestiones_humano': self.query_builder.select_gestiones_humano(
                fecha_inicio, fecha_corte
            ),
            'pagos': self.query_builder.select_pagos(
                fecha_inicio, fecha_corte
            )
        }
        
        # Execute queries using adapter
        raw_data = {}
        for table_name, query in queries.items():
            try:
                self.logger.debug(f"Executing query for {table_name}")
                raw_data[table_name] = await self.data_adapter.execute_query(query)
                self.logger.info(f"Extracted {len(raw_data[table_name])} records from {table_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to extract {table_name}: {str(e)}")
                raw_data[table_name] = []  # Continue with empty data
                
        return raw_data
    
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
        
        # Add totals row for percentage calculations
        account_metrics_with_totals = self._add_totals_row(account_metrics)
        
        # Generate different aggregation levels
        segmento_data = self._aggregate_by_segments(account_metrics_with_totals)
        negocio_data = self._aggregate_by_service(account_metrics_with_totals)
        
        # Calculate percentages relative to totals
        segmento_data = self._calculate_percentages(segmento_data, account_metrics_with_totals)
        negocio_data = self._calculate_percentages(negocio_data, account_metrics_with_totals)
        
        dashboard_data = {
            'segmentoData': segmento_data,
            'negocioData': negocio_data,
            'integralChartData': self._generate_integral_chart_data(account_metrics),
            'metadata': {
                'lastRefresh': datetime.now().isoformat(),
                'totalRecords': len(account_metrics),
                'filters': filters,
                'fechaCorte': account_metrics['fecha_asignacion'].max().isoformat() if not account_metrics.empty else None
            }
        }
        
        return dashboard_data
    
    def _add_totals_row(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add totals row for percentage calculations
        """
        if df.empty:
            return df
        
        # Calculate totals
        totals = {
            'id': 'total',
            'name': 'Total',
            'cartera': 'TOTAL',
            'servicio': 'TOTAL',
            'vencimiento_grupo': 'TOTAL',
            'cuenta': df['cuenta'].nunique() if 'cuenta' in df.columns else len(df),
            'deuda_inicial': df.get('deuda_inicial', 0).sum(),
            'deuda_actual': df.get('deuda_actual', 0).sum(),
            'gestionado': df.get('gestionado', False).sum(),
            'contactado': df.get('contactado', False).sum(),
            'contacto_directo': df.get('contacto_directo', 0).sum(),
            'contacto_indirecto': df.get('contacto_indirecto', 0).sum(),
            'sin_contacto': df.get('sin_contacto', 0).sum(),
            'compromisos': df.get('compromisos', 0).sum(),
            'pagador': df.get('pagador', False).sum(),
            'recupero_total': df.get('recupero_total', 0).sum(),
            'total_gestiones': df.get('total_gestiones', 0).sum(),
            'status': 'none'
        }
        
        # Create totals DataFrame
        totals_df = pd.DataFrame([totals])
        
        # Append to original DataFrame
        result = pd.concat([df, totals_df], ignore_index=True)
        
        return result
    
    def _aggregate_by_segments(self, df: pd.DataFrame) -> List[Dict]:
        """
        Aggregate metrics by cartera + vencimiento segments
        """
        if df.empty:
            return []
        
        # Filter out totals row for aggregation
        df_no_totals = df[df['cartera'] != 'TOTAL']
        
        if df_no_totals.empty:
            return []
        
        # Group by cartera and vencimiento_grupo
        segments = df_no_totals.groupby(['cartera', 'vencimiento_grupo']).agg({
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
        
        # Calculate KPIs for each segment
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
        
        # Filter out totals row for aggregation
        df_no_totals = df[df['servicio'] != 'TOTAL']
        
        if df_no_totals.empty:
            return []
        
        # Group by servicio
        services = df_no_totals.groupby('servicio').agg({
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
        
        # Calculate KPIs for each service
        result = []
        for _, row in services.iterrows():
            service_data = self._calculate_segment_kpis(row)
            service_data['id'] = row['servicio']
            service_data['name'] = row['servicio']
            result.append(service_data)
        
        return result
    
    def _calculate_percentages(
        self, 
        aggregated_data: List[Dict], 
        df_with_totals: pd.DataFrame
    ) -> List[Dict]:
        """
        Calculate percentages relative to totals
        """
        if not aggregated_data or df_with_totals.empty:
            return aggregated_data
        
        # Get totals row
        totals_row = df_with_totals[df_with_totals['cartera'] == 'TOTAL']
        
        if totals_row.empty:
            return aggregated_data
        
        total_cuentas = totals_row.iloc[0]['cuenta']
        total_deuda_inicial = totals_row.iloc[0]['deuda_inicial']
        total_deuda_actual = totals_row.iloc[0]['deuda_actual']
        
        # Calculate percentages for each item
        for item in aggregated_data:
            if total_cuentas > 0:
                item['porcentajeCuentas'] = round((item['cuentas'] / total_cuentas) * 100, 1)
            else:
                item['porcentajeCuentas'] = 0
                
            if total_deuda_inicial > 0:
                item['porcentajeDeuda'] = round((item['deudaAsig'] / total_deuda_inicial) * 100, 1)
            else:
                item['porcentajeDeuda'] = 0
                
            if total_deuda_actual > 0:
                item['porcentajeDeudaAct'] = round((item['deudaAct'] / total_deuda_actual) * 100, 1)
            else:
                item['porcentajeDeudaAct'] = 0
        
        return aggregated_data
    
    def _calculate_segment_kpis(self, row: pd.Series) -> Dict[str, Any]:
        """
        Calculate KPIs for a segment (shared logic)
        """
        cuentas = row['cuenta'] if row['cuenta'] > 0 else 1  # Avoid division by zero
        
        return {
            'cuentas': int(row['cuenta']),
            'porcentajeCuentas': 0,  # Will be calculated in _calculate_percentages
            'deudaAsig': float(row['deuda_inicial']),
            'porcentajeDeuda': 0,  # Will be calculated in _calculate_percentages  
            'porcentajeDeudaStatus': 'ok',
            'deudaAct': float(row['deuda_actual']),
            'porcentajeDeudaAct': 0,  # Will be calculated in _calculate_percentages
            'porcentajeDeudaActStatus': 'ok',
            'cobertura': round((row['gestionado'] / cuentas) * 100, 1),
            'contacto': round((row['contactado'] / cuentas) * 100, 1),
            'contactoStatus': 'ok',
            'cd': round((row['contacto_directo'] / cuentas) * 100, 1),
            'ci': round((row['contacto_indirecto'] / cuentas) * 100, 1),
            'sc': round((row['sin_contacto'] / cuentas) * 100, 1),
            'cierre': round((row['pagador'] / cuentas) * 100, 1),
            'cierreStatus': 'ok',
            'inten': round(row['total_gestiones'] / max(row['gestionado'], 1), 1),
            'intenStatus': 'ok',
            'cdCount': int(row['contacto_directo']),
            'ciCount': int(row['contacto_indirecto']),
            'scCount': int(row['sin_contacto']),
            'sgCount': int(cuentas - row['gestionado']),
            'pdpCount': int(row['compromisos']),
            'fracCount': 0,  # TODO: Add fraccionamiento logic when available
            'pdpFracCount': int(row['compromisos']),
            'status': 'ok'  # TODO: Add status logic based on targets
        }
    
    def _generate_integral_chart_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Generate data for integral KPI charts
        """
        if df.empty:
            return []
        
        # Aggregate by cartera for chart
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
                'filters': {},
                'fechaCorte': None
            }
        }
    
    # =============================================================================
    # EVOLUTION DATA (For EvolutionPage)
    # =============================================================================
    
    async def get_evolution_data(
        self,
        filters: Dict[str, Any],
        fecha_inicio: date,
        fecha_fin: date
    ) -> Dict[str, Any]:
        """
        Generate evolution data for daily tracking
        """
        self.logger.info(f"Generating evolution data from {fecha_inicio} to {fecha_fin}")
        
        # For evolution, we need daily snapshots
        evolution_data = []
        
        current_date = fecha_inicio
        while current_date <= fecha_fin:
            daily_data = await self.get_dashboard_data(filters, current_date)
            
            # Extract key metrics for evolution
            if daily_data['metadata']['totalRecords'] > 0:
                evolution_point = {
                    'fecha': current_date.isoformat(),
                    'dia_gestion': (current_date - fecha_inicio).days + 1,
                    'cobertura': self._extract_total_metric(daily_data, 'cobertura'),
                    'contacto': self._extract_total_metric(daily_data, 'contacto'),
                    'cd': self._extract_total_metric(daily_data, 'cd'),
                    'cierre': self._extract_total_metric(daily_data, 'cierre'),
                    'recupero': self._extract_total_metric(daily_data, 'recupero_total')
                }
                evolution_data.append(evolution_point)
            
            current_date += timedelta(days=1)
        
        return {
            'evolutionData': evolution_data,
            'metadata': {
                'fechaInicio': fecha_inicio.isoformat(),
                'fechaFin': fecha_fin.isoformat(),
                'totalDays': len(evolution_data),
                'filters': filters
            }
        }
    
    def _extract_total_metric(self, dashboard_data: Dict, metric: str) -> float:
        """
        Extract total metric value from dashboard data
        """
        # Sum across all segments for total
        total = 0
        for item in dashboard_data.get('segmentoData', []):
            if metric in item:
                total += item[metric]
        
        return total if total > 0 else 0
    
    # =============================================================================
    # HEALTH CHECK
    # =============================================================================
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check health of data sources and service
        """
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'data_source': {
                'type': type(self.data_adapter).__name__,
                'connected': False,
                'dataset': self.data_adapter.dataset
            },
            'processor': {
                'initialized': self.processor is not None
            }
        }
        
        try:
            # Test data source connection
            health_status['data_source']['connected'] = await self.data_adapter.test_connection()
            
            if not health_status['data_source']['connected']:
                health_status['status'] = 'unhealthy'
                
        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['error'] = str(e)
        
        return health_status
