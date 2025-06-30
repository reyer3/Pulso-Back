# etl/pipelines/mart_build_pipeline.py

import asyncio
from datetime import date, timedelta
from pathlib import Path

from etl.models import CampaignWindow

from shared.core.logging import LoggerMixin
from shared.database.connection import DatabaseManager


class MartBuildPipeline(LoggerMixin):
    """
    Pipeline dedicado a construir las capas auxiliares (aux) y de data marts (mart)
    a partir de los datos existentes en la capa cruda (raw).
    """

    def __init__(self, db_manager: DatabaseManager, project_uid: str):
        super().__init__()
        self.db = db_manager
        self.uid = project_uid
        self.sql_path = Path(__file__).parent.parent / "sql"
        self.schemas = {
            'raw': f"raw_{self.uid}",
            'aux': f"aux_{self.uid}",
            'mart': f"mart_{self.uid}"
        }

    async def _execute_sql_from_file(self, file_path: str, *params):
        """Lee, formatea y ejecuta un script SQL parametrizado."""
        try:
            full_path = self.sql_path / file_path
            with open(full_path, 'r', encoding='utf-8') as f:
                query = f.read().format(**self.schemas)

            await self.db.execute_query(query, *params, fetch="none")
            self.logger.debug(f"Successfully executed SQL from: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to execute SQL from {file_path} with params {params}: {e}", exc_info=True)
            raise

    async def run_for_campaign(self, campaign: CampaignWindow):
        """Ejecuta el proceso completo de construcción de marts para una campaña."""
        self.logger.info(f"🚀 Starting Mart Build Pipeline for campaign '{campaign.archivo}'...")

        await self._build_campaign_level_aux(campaign.archivo)

        process_end_date = campaign.fecha_cierre or date.today()
        current_date = campaign.fecha_apertura

        while current_date <= process_end_date:
            await self._build_daily_marts(campaign.archivo, current_date)
            current_date += timedelta(days=1)

        self.logger.info(f"✅ Mart Build Pipeline finished successfully for '{campaign.archivo}'.")

    async def _build_campaign_level_aux(self, campaign_archivo: str):
        """Construye tablas auxiliares a nivel de campaña."""
        self.logger.info(f"  -> Building campaign-level auxiliary tables for '{campaign_archivo}'...")
        tasks = [
            self._execute_sql_from_file("aux/build_cuenta_campana_state.sql", campaign_archivo),
            self._execute_sql_from_file("aux/build_gestiones_unificadas.sql", campaign_archivo)
        ]
        await asyncio.gather(*tasks)
        await self._execute_sql_from_file("aux/build_gestion_cuenta_impact.sql", campaign_archivo)
        await self._execute_sql_from_file("aux/build_pago_deduplication.sql", campaign_archivo)

    async def _build_daily_marts(self, campaign_archivo: str, fecha: date):
        """Construye los data marts para un solo día."""
        self.logger.info(f"  -> Building daily marts for {fecha}...")
        await self._cleanup_daily_data(campaign_archivo, fecha)
        await self._execute_sql_from_file("mart/build_dashboard_data.sql", campaign_archivo, fecha)
        # Añadir aquí la construcción de otros marts diarios si es necesario

    async def _cleanup_daily_data(self, campaign_archivo: str, fecha: date):
        """Limpia los datos del día para asegurar idempotencia."""
        self.logger.debug(f"Cleaning daily mart data for '{campaign_archivo}' on {fecha}...")
        tables_to_clean = {f"mart_{self.uid}.dashboard_data": ("fecha_foto", "archivo")}

        for table, (date_col, archive_col) in tables_to_clean.items():
            query = f"DELETE FROM {table} WHERE {date_col} = $1 AND {archive_col} = $2"
            await self.db.execute_query(query, fecha, campaign_archivo, fetch="none")