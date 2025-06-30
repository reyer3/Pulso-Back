# etl/pipelines/campaign_catchup_pipeline.py

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from etl.models import CampaignWindow, CampaignLoadResult, PipelineExecutionSummary
from etl.pipelines.mart_build_pipeline import MartBuildPipeline
from etl.pipelines.raw_data_pipeline import HybridRawDataPipeline
from etl.watermarks import WatermarkManager
from etl.config import ETLConfig
from shared.core.logging import LoggerMixin
from shared.database.connection import DatabaseManager


class CampaignCatchUpPipeline(LoggerMixin):
    """
    Pipeline principal que orquesta el ciclo ETL completo para campañas.

    ARQUITECTURA MEJORADA:
    - Separación clara entre Raw Data Pipeline y Mart Build Pipeline
    - Gestión robusta de errores y watermarks
    - Procesamiento en lotes con control de concurrencia
    - Cancelación graceful y recuperación de errores
    """

    def __init__(
            self,
            db_manager: DatabaseManager,
            watermark_manager: WatermarkManager,
            raw_data_pipeline: HybridRawDataPipeline,
            mart_build_pipeline: MartBuildPipeline,
    ):
        super().__init__()
        self.db = db_manager
        self.watermarks = watermark_manager
        self.raw_data_pipeline = raw_data_pipeline
        self.mart_build_pipeline = mart_build_pipeline
        self._cancel_event = asyncio.Event()
        self._is_running = False

    def is_running(self) -> bool:
        """Verifica si el pipeline ya está en ejecución."""
        return self._is_running

    def cancel(self):
        """Señala al pipeline que se detenga después del lote actual."""
        if self._is_running:
            self.logger.warning("🛑 Cancellation signal received. Process will stop after the current batch.")
            self._cancel_event.set()
        else:
            self.logger.info("Pipeline is not currently running.")

    async def get_campaign_windows(self, limit: Optional[int] = None) -> List[CampaignWindow]:
        """
        Obtiene las ventanas de campaña desde la tabla calendario.
        Usa configuración dinámica para esquemas.
        """
        # MEJORADO: Usar configuración dinámica en lugar de hardcodear
        calendario_fqn = ETLConfig.get_fq_table_name("calendario")

        query = f"""
        SELECT archivo, 
               fecha_apertura, 
               fecha_cierre, 
               tipo_cartera, 
               estado_cartera
        FROM {calendario_fqn}
        WHERE fecha_apertura IS NOT NULL
        ORDER BY fecha_apertura ASC
        """
        if limit:
            query += f" LIMIT {limit}"

        try:
            rows = await self.db.execute_query(query, fetch="all")
            campaigns = [CampaignWindow(**dict(row)) for row in rows]
            self.logger.info(f"Found {len(campaigns)} campaign windows in {calendario_fqn}.")
            return campaigns
        except Exception as e:
            self.logger.error(f"Failed to get campaign windows: {e}", exc_info=True)
            return []

    async def _should_process_campaign(
            self,
            campaign: CampaignWindow,
            force_refresh: bool = False
    ) -> bool:
        """
        Determina si una campaña debe ser procesada basándose en su estado y watermarks.
        """
        if force_refresh:
            return True

        # Verificar watermark
        watermark_name = f"campaign__{campaign.archivo}"
        watermark = await self.watermarks.get_watermark(watermark_name)

        # Procesar si:
        # 1. No hay watermark (nunca procesada)
        # 2. El watermark indica fallo
        # 3. La campaña está abierta (puede tener datos nuevos)
        if not watermark:
            return True

        if watermark.last_extraction_status != 'success':
            return True

        if campaign.is_active:
            return True

        return False

    async def run_all_pending_campaigns(
            self,
            batch_size: int = 3,
            max_campaigns: Optional[int] = None,
            force_refresh_all: bool = False,
    ) -> Dict[str, Any]:
        """
        Punto de entrada principal del pipeline.
        Procesa todas las campañas pendientes con control de concurrencia.
        """
        if self._is_running:
            self.logger.warning("Catch-up process is already running. Ignoring new request.")
            return {
                'status': 'already_running',
                'message': 'A catch-up process is already in progress.'
            }

        self._is_running = True
        self._cancel_event.clear()
        start_time = datetime.now(timezone.utc)

        try:
            self.logger.info(f"🚀 Starting intelligent campaign catch-up (force_refresh={force_refresh_all})")

            # Obtener todas las campañas
            all_campaigns = await self.get_campaign_windows(limit=max_campaigns)

            if not all_campaigns:
                return {
                    'status': 'completed',
                    'message': 'No campaigns found',
                    'duration_seconds': 0
                }

            # Filtrar campañas que necesitan procesamiento
            campaigns_to_process = []
            for campaign in all_campaigns:
                if await self._should_process_campaign(campaign, force_refresh_all):
                    campaigns_to_process.append(campaign)

            self.logger.info(
                f"Found {len(all_campaigns)} total campaigns. "
                f"Skipping {len(all_campaigns) - len(campaigns_to_process)} already processed. "
                f"Processing {len(campaigns_to_process)}."
            )

            if not campaigns_to_process:
                return {
                    'status': 'completed',
                    'message': 'All campaigns are up-to-date.',
                    'campaigns_total': len(all_campaigns),
                    'campaigns_processed': 0,
                    'duration_seconds': (datetime.now(timezone.utc) - start_time).total_seconds()
                }

            # Procesar las campañas en lotes
            all_results = await self._process_campaigns_in_batches(
                campaigns_to_process,
                batch_size
            )

            # Generar resumen final
            return self._generate_execution_summary(
                start_time,
                all_campaigns,
                campaigns_to_process,
                all_results
            )

        except Exception as e:
            self.logger.error(f"Critical error in campaign catch-up: {e}", exc_info=True)
            return {
                'status': 'critical_failure',
                'message': str(e),
                'duration_seconds': (datetime.now(timezone.utc) - start_time).total_seconds()
            }
        finally:
            self._is_running = False
            self._cancel_event.clear()

    async def _process_campaigns_in_batches(
            self,
            campaigns: List[CampaignWindow],
            batch_size: int
    ) -> List[CampaignLoadResult]:
        """Procesa campañas en lotes con control de concurrencia."""
        all_results: List[CampaignLoadResult] = []

        for i in range(0, len(campaigns), batch_size):
            if self._cancel_event.is_set():
                self.logger.info("Process cancelled by user. Stopping batch processing.")
                break

            batch = campaigns[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(campaigns) + batch_size - 1) // batch_size

            self.logger.info(
                f"📦 Processing batch {batch_num}/{total_batches}: "
                f"{[c.archivo for c in batch]}"
            )

            # Procesar lote con manejo de excepciones
            batch_results = await self._process_campaign_batch(batch)
            all_results.extend(batch_results)

        return all_results

    async def _process_campaign_batch(
            self,
            batch: List[CampaignWindow]
    ) -> List[CampaignLoadResult]:
        """Procesa un lote de campañas en paralelo."""
        tasks = [self.run_for_campaign(campaign) for campaign in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for i, res in enumerate(batch_results):
            if isinstance(res, CampaignLoadResult):
                results.append(res)
            elif isinstance(res, Exception):
                self.logger.error(
                    f"Campaign task failed unexpectedly for '{batch[i].archivo}': {res}",
                    exc_info=res
                )
                # Crear resultado de error
                error_result = CampaignLoadResult(
                    archivo=batch[i].archivo,
                    status="failed",
                    duration_seconds=0,
                    errors=[f"Unexpected error: {str(res)}"],
                    tables_loaded={}
                )
                results.append(error_result)

        return results

    async def run_for_campaign(self, campaign: CampaignWindow) -> CampaignLoadResult:
        """
        Ejecuta el pipeline E2E para una sola campaña.
        MEJORADO: Gestión granular de errores y métricas detalladas.
        """
        start_time = datetime.now(timezone.utc)
        watermark_name = f"campaign__{campaign.archivo}"

        self.logger.info(f"🔥 Starting End-to-End Pipeline for '{campaign.archivo}'")

        # Inicializar watermark
        await self.watermarks.start_extraction(
            watermark_name,
            f"e2e_run_{start_time.isoformat()}"
        )

        errors = []
        tables_loaded = {}
        raw_records = 0
        mart_records = 0
        status = "success"

        try:
            # Etapa 1: Cargar Datos Raw
            self.logger.info(f"  -> Stage 1: Raw Data Pipeline for '{campaign.archivo}'...")
            raw_records = await self.raw_data_pipeline.run_for_campaign(campaign)
            tables_loaded["raw_total"] = raw_records

        except Exception as e:
            error_msg = f"Raw data pipeline failed: {str(e)}"
            errors.append(error_msg)
            self.logger.error(f"Stage 1 failed for '{campaign.archivo}': {e}", exc_info=True)

        # Solo continuar a marts si raw fue exitoso
        if not errors:
            try:
                # Etapa 2: Construir Marts
                self.logger.info(f"  -> Stage 2: Mart Build Pipeline for '{campaign.archivo}'...")
                await self.mart_build_pipeline.run_for_campaign(campaign)

                # Obtener conteo de registros de marts (opcional)
                mart_records = await self._get_mart_records_count(campaign.archivo)
                tables_loaded["mart_total"] = mart_records

            except Exception as e:
                error_msg = f"Mart build pipeline failed: {str(e)}"
                errors.append(error_msg)
                self.logger.error(f"Stage 2 failed for '{campaign.archivo}': {e}", exc_info=True)

        # Determinar status final
        if errors:
            status = "partial" if raw_records > 0 else "failed"

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Actualizar watermark
        await self.watermarks.update_watermark(
            table_name=watermark_name,
            timestamp=datetime.now(timezone.utc),
            records_extracted=raw_records + mart_records,
            status=status,
            error_message="; ".join(errors) if errors else None,
            extraction_duration_seconds=duration
        )

        self.logger.info(
            f"✅ Finished E2E Pipeline for '{campaign.archivo}' "
            f"in {duration:.2f}s with status: {status}"
        )

        return CampaignLoadResult(
            archivo=campaign.archivo,
            status=status,
            duration_seconds=duration,
            errors=errors,
            tables_loaded=tables_loaded,
            raw_records_total=raw_records,
            mart_records_total=mart_records
        )

    async def _get_mart_records_count(self, archivo: str) -> int:
        """Obtiene el conteo de registros en las tablas de mart para una campaña."""
        try:
            # Ejemplo de consulta para contar registros en dashboard_data
            dashboard_fqn = ETLConfig.get_fq_table_name("dashboard_data", "mart")
            query = f"SELECT COUNT(*) as count FROM {dashboard_fqn} WHERE archivo = $1"
            result = await self.db.execute_query(query, archivo, fetch="one")
            return result["count"] if result else 0
        except Exception as e:
            self.logger.warning(f"Could not get mart record count for {archivo}: {e}")
            return 0

    def _generate_execution_summary(
            self,
            start_time: datetime,
            all_campaigns: List[CampaignWindow],
            processed_campaigns: List[CampaignWindow],
            results: List[CampaignLoadResult]
    ) -> Dict[str, Any]:
        """Genera el resumen final de la ejecución del pipeline."""
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        successful_campaigns = sum(1 for r in results if r.is_success)
        failed_campaigns = sum(1 for r in results if not r.is_success)

        campaigns_per_minute = (len(results) / (duration / 60)) if duration > 0 else 0

        return {
            'status': 'cancelled' if self._cancel_event.is_set() else 'completed',
            'duration_seconds': round(duration, 2),
            'campaigns_total_in_db': len(all_campaigns),
            'campaigns_eligible_for_processing': len(processed_campaigns),
            'campaigns_processed': len(results),
            'campaigns_successful': successful_campaigns,
            'campaigns_failed': failed_campaigns,
            'campaigns_per_minute': round(campaigns_per_minute, 2),
            'success_rate_percentage': round((successful_campaigns / len(results)) * 100, 1) if results else 0,
            'total_raw_records': sum(r.raw_records_total for r in results),
            'total_mart_records': sum(r.mart_records_total for r in results),
            'failed_details': [
                {
                    'archivo': r.archivo,
                    'errors': r.errors,
                    'duration_seconds': r.duration_seconds
                }
                for r in results if not r.is_success
            ]
        }