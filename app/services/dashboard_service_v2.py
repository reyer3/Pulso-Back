"""
📊 Dashboard Service V2 - PostgreSQL-First Approach
Service layer that queries the local `pulso_db` for dashboard data.
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
import pandas as pd

from app.core.logging import LoggerMixin
from app.repositories.postgres_repo import PostgresRepository

class DashboardServiceV2(LoggerMixin):
    """
    Dashboard service that provides data by querying the local PostgreSQL database.
    """

    def __init__(self, postgres_repo: Optional[PostgresRepository] = None):
        """
        Initialize with a PostgresRepository.
        """
        self.repo = postgres_repo or PostgresRepository()

    async def get_dashboard_data(
        self,
        filters: Dict[str, Any],
        fecha_corte: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Generate dashboard data by querying the `dashboard_data` table.
        """
        if fecha_corte is None:
            fecha_corte = date.today()

        self.logger.info(f"Generating dashboard data for {fecha_corte} from pulso_db with filters: {filters}")

        query = "SELECT * FROM dashboard_data WHERE fecha_foto = $1"
        params = [fecha_corte]

        # Add filters to the query
        param_index = 2
        for key, value in filters.items():
            if value:
                query += f" AND {key} = ${param_index}"
                params.append(value)
                param_index += 1

        records = await self.repo.execute_query(query, tuple(params))

        if not records:
            return self._empty_dashboard_response()

        # The data is already processed by the ETL, so we can use it directly.
        # This simplifies the service significantly.
        df = pd.DataFrame(records)

        # The aggregation logic can be simplified or moved to the database in the future.
        # For now, we keep the existing pandas-based aggregation.
        dashboard_data = self._aggregate_dashboard_metrics(df, filters)

        self.logger.info(f"Generated dashboard data with {len(df)} records from pulso_db")

        return dashboard_data

    def _aggregate_dashboard_metrics(
        self,
        df: pd.DataFrame,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate metrics for dashboard views.
        This logic can be simplified or moved to SQL views later.
        """
        # This is a placeholder for the complex aggregation logic that was in the
        # original service. Since the data is now pre-aggregated by the ETL,
        # this can be greatly simplified. For now, we'll return the raw data
        # and a simplified structure.

        # A more advanced implementation would perform these aggregations in SQL.
        segmento_data = df.to_dict('records')
        negocio_data = df.groupby('servicio').sum().reset_index().to_dict('records')

        return {
            'segmentoData': segmento_data,
            'negocioData': negocio_data,
            'integralChartData': [], # This would need to be calculated
            'metadata': {
                'lastRefresh': datetime.now().isoformat(),
                'totalRecords': len(df),
                'filters': filters,
                'fechaCorte': fecha_corte.isoformat() if not df.empty else None,
                'dataSource': 'pulso_db'
            }
        }

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
                'fechaCorte': None,
                'dataSource': 'pulso_db'
            }
        }

    async def health_check(self) -> Dict[str, Any]:
        """
        Check health of the database connection.
        """
        is_healthy = await self.repo.health_check()
        return {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'dependencies': ['PostgreSQL (pulso_db)']
        }