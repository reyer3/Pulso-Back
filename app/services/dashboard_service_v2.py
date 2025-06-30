# app/services/dashboard_service_v2.py
"""
📊 Dashboard Service V2 - "Última Foto" POC Approach (Dynamic Dimensions & KPI Calculation)
Service layer that queries BigQuery for base metrics and calculates all KPIs in-app.
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from app.core.logging import LoggerMixin  # Asegúrate que el import sea correcto
from app.repositories.bigquery_repo import BigQueryRepository
from app.models.dashboard import DashboardData, DataRow, IntegralChartDataPoint, IconStatus


class DashboardServiceV2(LoggerMixin):
    """
    Dashboard service that queries a BigQuery table with base metrics
    and calculates all KPIs before returning the response.
    """

    def __init__(self, bigquery_repo: BigQueryRepository):
        self.repo = bigquery_repo
        self.base_table = "`BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_tbldashboard_metricas_base`"

        self.api_to_db_map = {
            "cartera": "TIPO_CARTERA",
            "servicio": "SERVICIO",
            "periodo": "PERIODO"
        }

    async def get_dashboard_data(
            self,
            filters: Dict[str, Any],
            dimensions: List[str]
    ) -> DashboardData:
        """
        Generates dashboard data by querying base metrics and calculating KPIs.
        """
        self.logger.info(
            f"Generating dashboard data from BigQuery 'última foto' with filters: {filters} and dimensions: {dimensions}"
        )

        query_params = {}
        where_clauses = ["1=1"]

        for api_filter, values in filters.items():
            if values and api_filter in self.api_to_db_map:
                db_column = self.api_to_db_map[api_filter]
                param_name = f"filter_{api_filter}"
                where_clauses.append(f"{db_column} IN UNNEST(@{param_name})")
                query_params[param_name] = values

        where_sql = " AND ".join(where_clauses)

        # La tabla solo contiene contadores y sumas, no ratios.
        query = f"SELECT * FROM {self.base_table} WHERE {where_sql};"

        records = await self.repo.execute_query(query, query_params)
        if not records:
            return DashboardData(segmentoData=[], negocioData=[], integralChartData=[])

        df = pd.DataFrame(records)

        processed_tables = {}
        processed_dfs = {}

        for api_dim in dimensions:
            db_column = self.api_to_db_map.get(api_dim)

            if not db_column or db_column not in df.columns:
                self.logger.warning(f"Dimensión API '{api_dim}' o su columna '{db_column}' no es válida. Saltando.")
                continue

            # Agrupar y sumar los contadores base
            grouped_df = df.groupby(db_column).sum(numeric_only=True).reset_index()

            # --- MODIFICACIÓN CLAVE: Calcular KPIs después de agrupar ---
            self._calculate_kpis_on_df(grouped_df)

            processed_dfs[api_dim] = grouped_df
            processed_tables[api_dim] = self._build_datarows_from_df(grouped_df.copy(), db_column)

        valid_dims = list(processed_tables.keys())

        segmento_data = processed_tables.get(valid_dims[0]) if len(valid_dims) > 0 else []
        negocio_data = processed_tables.get(valid_dims[1]) if len(valid_dims) > 1 else []

        integral_chart_data = []
        if len(valid_dims) > 0:
            first_dim_df = processed_dfs[valid_dims[0]]
            integral_chart_data = self._build_integral_chart_data(first_dim_df.copy(),
                                                                  self.api_to_db_map[valid_dims[0]])

        return DashboardData(
            segmentoData=segmento_data,
            negocioData=negocio_data,
            integralChartData=integral_chart_data,
        )

    @staticmethod
    def _calculate_kpis_on_df(df: pd.DataFrame):
        """
        Calcula todos los KPIs porcentuales y de ratio directamente en el DataFrame.
        Este método modifica el DataFrame 'in-place'.
        """
        # --- Totales para porcentajes sobre el total ---
        total_cuentas = df['cuentas_asignadas'].sum()
        total_deuda_asig = df['deuda_inicial_total'].sum()

        df['porcentajeCuentas'] = (df.get('cuentas_asignadas', 0) / total_cuentas * 100).fillna(
            0) if total_cuentas > 0 else 0
        df['porcentajeDeuda'] = (df.get('deuda_inicial_total', 0) / total_deuda_asig * 100).fillna(
            0) if total_deuda_asig > 0 else 0

        # --- Ratios por fila (división segura) ---
        cuentas_asignadas = df.get('cuentas_asignadas', 1).replace(0, 1)
        cuentas_gestionadas = df.get('cuentas_gestionadas', 1).replace(0, 1)

        # I. Cobertura y Contacto
        df['cobertura'] = (df.get('cuentas_gestionadas', 0) / cuentas_asignadas * 100).fillna(0)
        df['cuentas_con_contacto_total'] = df.get('cuentas_con_contacto_directo', 0) + df.get(
            'cuentas_con_contacto_indirecto', 0)
        df['contacto'] = (df['cuentas_con_contacto_total'] / cuentas_asignadas * 100).fillna(0)

        # II. Rendimiento y Eficiencia
        df['cierre'] = (df.get('cuentas_pagadoras', 0) / cuentas_asignadas * 100).fillna(0)
        df['inten'] = (df.get('total_gestiones_validas', 0) / cuentas_gestionadas).fillna(0)

        # III. Contadores derivados
        df['sgCount'] = df.get('cuentas_asignadas', 0) - df.get('cuentas_gestionadas', 0)
        df['pdpFracCount'] = df.get('cuentas_con_compromiso',
                                    0)  # Asumimos que no hay 'fraccionamiento' separado por ahora

    def _build_datarows_from_df(self, df: pd.DataFrame, group_by_dim_db: str) -> List[DataRow]:
        """
        Construye una lista de DataRow a partir de un DataFrame que ya tiene los KPIs calculados.
        """
        if df.empty:
            return []

        df = df.rename(columns={group_by_dim_db: 'name'})

        data_rows = []
        for _, row in df.iterrows():
            data_rows.append(
                DataRow(
                    id=str(row['name']), name=str(row['name']), status=IconStatus.NONE,
                    cuentas=int(row.get('cuentas_asignadas', 0)),
                    porcentajeCuentas=row.get('porcentajeCuentas', 0.0),
                    deudaAsig=row.get('deuda_inicial_total', 0.0),
                    porcentajeDeuda=row.get('porcentajeDeuda', 0.0),
                    porcentajeDeudaStatus=IconStatus.NONE,
                    deudaAct=row.get('deuda_actual_total', 0.0),
                    porcentajeDeudaAct=0.0, porcentajeDeudaActStatus=IconStatus.NONE,
                    cobertura=row.get('cobertura', 0.0),
                    contacto=row.get('contacto', 0.0), contactoStatus=IconStatus.NONE,
                    cd=0.0, ci=0.0, sc=0.0,  # Placeholders, se pueden calcular si es necesario
                    cierre=row.get('cierre', 0.0), cierreStatus=IconStatus.NONE,
                    inten=row.get('inten', 0.0), intenStatus=IconStatus.NONE,
                    cdCount=int(row.get('cuentas_con_contacto_directo', 0)),
                    ciCount=int(row.get('cuentas_con_contacto_indirecto', 0)),
                    scCount=0,  # Placeholder
                    sgCount=int(row.get('sgCount', 0)),
                    pdpCount=int(row.get('cuentas_con_compromiso', 0)),
                    fracCount=0,  # Placeholder
                    pdpFracCount=int(row.get('pdpFracCount', 0)),
                )
            )
        return data_rows

    def _build_integral_chart_data(self, df: pd.DataFrame, group_by_dim_db: str) -> List[IntegralChartDataPoint]:
        """
        Construye los datos para el gráfico integral. Reutiliza los KPIs ya calculados en el DF.
        """
        if df.empty:
            return []

        df = df.rename(columns={group_by_dim_db: 'name'})

        # KPIs adicionales para el gráfico
        cuentas_asignadas = df.get('cuentas_asignadas', 1).replace(0, 1)
        df['contactoDirecto_pct'] = (df.get('cuentas_con_contacto_directo', 0) / cuentas_asignadas * 100).fillna(0)
        df['contactoIndirecto_pct'] = (df.get('cuentas_con_contacto_indirecto', 0) / cuentas_asignadas * 100).fillna(0)

        chart_points = []
        for _, row in df.iterrows():
            chart_points.append(
                IntegralChartDataPoint(
                    name=str(row['name']),
                    cobertura=row.get('cobertura', 0.0),
                    contacto=row.get('contacto', 0.0),
                    contactoDirecto=row.get('contactoDirecto_pct', 0.0),
                    contactoIndirecto=row.get('contactoIndirecto_pct', 0.0),
                    tasaDeCierre=row.get('cierre', 0.0),
                    intensidad=row.get('inten', 0.0)
                )
            )
        return chart_points

    async def health_check(self) -> Dict[str, Any]:
        is_healthy = await self.repo.health_check()
        return {'status': 'healthy' if is_healthy else 'unhealthy', 'timestamp': datetime.now().isoformat(),
                'dependencies': ['Google BigQuery']}