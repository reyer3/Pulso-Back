# etl/pipelines/hybrid_raw_data_pipeline.py

"""
🎯 PIPELINE HÍBRIDO: Calendario + Watermarks

ESTRATEGIA DUAL:
1. MODO CALENDARIO: Para cargas retroactivas guiadas por campañas históricas
2. MODO INCREMENTAL: Para cargas futuras basadas en watermarks

CASOS DE USO:
- Backfill de 3 meses históricos → usar calendario
- Cargas diarias nuevas → usar watermarks
- Recovery de fallos → híbrido inteligente
"""

import asyncio
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, AsyncGenerator, Dict, Any, Literal
from enum import Enum

from shared.core.logging import LoggerMixin
from etl.config import ETLConfig, ExtractionMode
from etl.extractors.bigquery_extractor import BigQueryExtractor
from etl.loaders.postgres_loader import PostgresLoader
from etl.transformers.raw_data_transformer import RawTransformerRegistry
from etl.models import CampaignWindow, TableLoadResult
from etl.watermarks import get_watermark_manager, WatermarkManager


class ExtractionStrategy(str, Enum):
    """Estrategias de extracción disponibles"""
    CALENDAR_DRIVEN = "calendar_driven"  # Guiado por fechas de campaña
    WATERMARK_DRIVEN = "watermark_driven"  # Guiado por watermarks
    HYBRID_AUTO = "hybrid_auto"  # Decisión automática inteligente


class HybridRawDataPipeline(LoggerMixin):
    """
    Pipeline híbrido que combina extracciones por calendario y watermarks.

    🎯 FUNCIONALIDADES:
    - Cargas retroactivas guiadas por calendario
    - Incrementales modernas con watermarks
    - Detección automática de estrategia óptima
    - Compatibilidad total con código existente
    """

    def __init__(
            self,
            extractor: BigQueryExtractor,
            transformer: RawTransformerRegistry,
            loader: PostgresLoader,
            watermark_manager: Optional[WatermarkManager] = None,
    ):
        super().__init__()
        self.sql_path = Path(__file__).parent.parent / "sql" / "raw"
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.watermark_manager = watermark_manager
        self.max_batch_size = 1000

    async def _get_watermark_manager(self) -> WatermarkManager:
        """Get watermark manager instance"""
        if self.watermark_manager is None:
            self.watermark_manager = await get_watermark_manager()
        return self.watermark_manager

    async def _determine_extraction_strategy(
            self,
            table_name: str,
            campaign: Optional[CampaignWindow],
            force_strategy: Optional[ExtractionStrategy] = None
    ) -> ExtractionStrategy:
        """
        🧠 Decide la estrategia óptima de extracción basada en contexto.

        LÓGICA DE DECISIÓN:
        1. Si force_strategy → usar forzada
        2. Si hay campaña Y no hay watermark → CALENDAR_DRIVEN
        3. Si hay watermark Y campaña es muy antigua → WATERMARK_DRIVEN
        4. Si hay watermark Y campaña es reciente → CALENDAR_DRIVEN
        5. Si solo hay watermark → WATERMARK_DRIVEN
        6. Default → CALENDAR_DRIVEN
        """
        if force_strategy:
            self.logger.info(f"🔧 Using forced strategy for {table_name}: {force_strategy.value}")
            return force_strategy

        watermark_manager = await self._get_watermark_manager()
        last_extraction = await watermark_manager.get_last_extraction_time(table_name)

        # 🎯 ESTRATEGIA: Priorizar calendario para backfill, watermarks para incremental
        if campaign:
            campaign_age_days = (date.today() - campaign.fecha_apertura).days

            if not last_extraction:
                # No hay watermark → usar calendario (caso backfill)
                strategy = ExtractionStrategy.CALENDAR_DRIVEN
                reason = "no watermark exists, using calendar for backfill"
            elif campaign_age_days > 90:  # Campaña más antigua que 3 meses
                # Campaña muy antigua → usar watermarks (más eficiente)
                strategy = ExtractionStrategy.WATERMARK_DRIVEN
                reason = f"campaign is {campaign_age_days} days old, using watermarks"
            elif campaign.fecha_apertura < last_extraction.date():
                # Campaña más antigua que último watermark → usar watermarks
                strategy = ExtractionStrategy.WATERMARK_DRIVEN
                reason = "campaign predates last extraction, using watermarks"
            else:
                # Campaña nueva o reciente → usar calendario (más preciso)
                strategy = ExtractionStrategy.CALENDAR_DRIVEN
                reason = f"recent campaign ({campaign_age_days}d old), using calendar precision"
        else:
            # Sin campaña → usar watermarks si existen
            if last_extraction:
                strategy = ExtractionStrategy.WATERMARK_DRIVEN
                reason = "no campaign context, using watermarks for incremental"
            else:
                strategy = ExtractionStrategy.CALENDAR_DRIVEN
                reason = "no campaign or watermark, defaulting to calendar"

        self.logger.info(f"🧠 Auto-selected strategy for {table_name}: {strategy.value} ({reason})")
        return strategy

    def _get_query_template(self, table_name: str) -> str:
        """Carga la plantilla SQL para una tabla desde archivos."""
        file_path = self.sql_path / f"{table_name}.sql"
        if not file_path.exists():
            raise FileNotFoundError(f"Query file not found for '{table_name}' at {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    async def _build_calendar_driven_query(
            self,
            table_name: str,
            campaign: CampaignWindow,
            extend_window: bool = True
    ) -> str:
        """
        🗓️ Construye query basada en fechas de campaña (modo calendario).

        VENTANAS INTELIGENTES por tipo de tabla:
        - extend_window=True → usar ventanas extendidas (backfill)
        - extend_window=False → usar solo fechas exactas de campaña
        """
        template = self._get_query_template(table_name)

        if extend_window:
            # Ventanas extendidas para capturar datos relacionados
            if table_name in ["asignaciones"]:
                start_date = campaign.fecha_apertura - timedelta(days=30)
                end_date = (campaign.fecha_cierre or date.today()) + timedelta(days=15)
            elif table_name in ["trandeuda"]:
                start_date = campaign.fecha_apertura - timedelta(days=7)
                end_date = (campaign.fecha_cierre or date.today()) + timedelta(days=30)
            elif table_name in ["pagos"]:
                start_date = campaign.fecha_apertura - timedelta(days=7)
                end_date = (campaign.fecha_cierre or date.today()) + timedelta(days=45)
            elif table_name in ["voicebot_gestiones", "mibotair_gestiones"]:
                start_date = campaign.fecha_apertura
                end_date = campaign.fecha_cierre or (campaign.fecha_apertura + timedelta(days=90))
            else:
                start_date = campaign.fecha_apertura - timedelta(days=15)
                end_date = (campaign.fecha_cierre or date.today()) + timedelta(days=15)
        else:
            # Ventanas exactas de campaña
            start_date = campaign.fecha_apertura
            end_date = campaign.fecha_cierre or date.today()

        incremental_filter = f"BETWEEN '{start_date}' AND '{end_date}'"

        self.logger.info(
            f"🗓️ Calendar query for {table_name}: {start_date} to {end_date} "
            f"(campaign: {campaign.archivo}, extended: {extend_window})"
        )

        return template.format(
            project_id=ETLConfig.PROJECT_ID,
            dataset_id=ETLConfig.BQ_DATASET,
            incremental_filter=incremental_filter,
            campaign_archivo=campaign.archivo
        )

    async def _build_watermark_driven_query(
            self,
            table_name: str,
            config: ETLConfig,
            campaign: Optional[CampaignWindow] = None
    ) -> str:
        """
        ⏰ Construye query basada en watermarks (modo incremental).
        """
        template = self._get_query_template(table_name)
        watermark_manager = await self._get_watermark_manager()

        if not config.incremental_column:
            raise ValueError(f"Table {table_name} has no incremental_column defined for watermark mode")

        last_extracted = await watermark_manager.get_last_extraction_time(table_name)

        if last_extracted:
            # Usar watermark con lookback para seguridad
            lookback_date = last_extracted - timedelta(days=config.lookback_days)
            end_date = datetime.now().date()

            incremental_filter = f"BETWEEN '{lookback_date.date()}' AND '{end_date}'"

            self.logger.info(
                f"⏰ Watermark query for {table_name}: from {lookback_date.date()} "
                f"(last extracted: {last_extracted.date()}, lookback: {config.lookback_days}d)"
            )
        else:
            # Sin watermark → full refresh o usar campaña como fallback
            if campaign:
                self.logger.warning(
                    f"⚠️ No watermark for {table_name}, falling back to campaign dates"
                )
                return await self._build_calendar_driven_query(table_name, campaign, extend_window=False)
            else:
                self.logger.warning(f"⚠️ No watermark or campaign for {table_name}, using full refresh")
                incremental_filter = "1=1"

        return template.format(
            project_id=ETLConfig.PROJECT_ID,
            dataset_id=ETLConfig.BQ_DATASET,
            incremental_filter=incremental_filter,
            campaign_archivo=campaign.archivo if campaign else "ALL"
        )

    async def _build_hybrid_query(
            self,
            table_name: str,
            config: ETLConfig,
            campaign: Optional[CampaignWindow],
            strategy: ExtractionStrategy,
            force_full_refresh: bool = False
    ) -> str:
        """
        🎯 Construye query usando la estrategia determinada.
        """
        if force_full_refresh or config.default_mode == ExtractionMode.FULL_REFRESH:
            template = self._get_query_template(table_name)
            self.logger.info(f"🔄 Full refresh for {table_name}")
            return template.format(
                project_id=ETLConfig.PROJECT_ID,
                dataset_id=ETLConfig.BQ_DATASET,
                incremental_filter="1=1",
                campaign_archivo=campaign.archivo if campaign else "ALL"
            )

        if strategy == ExtractionStrategy.CALENDAR_DRIVEN:
            if not campaign:
                raise ValueError(f"Calendar-driven extraction requires campaign context for {table_name}")
            return await self._build_calendar_driven_query(table_name, campaign)

        elif strategy == ExtractionStrategy.WATERMARK_DRIVEN:
            return await self._build_watermark_driven_query(table_name, config, campaign)

        else:
            raise ValueError(f"Unsupported extraction strategy: {strategy}")

    async def _transform_stream_with_backpressure(
            self,
            table_name: str,
            raw_stream: AsyncGenerator[List[Dict[str, Any]], None]
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Transforma stream de datos con control de back-pressure."""
        batch_count = 0
        total_processed = 0

        try:
            async for raw_batch in raw_stream:
                if not raw_batch:
                    continue

                batch_count += 1

                if len(raw_batch) > self.max_batch_size:
                    self.logger.warning(
                        f"Large batch detected for {table_name}: {len(raw_batch)} records. "
                        f"Splitting into smaller chunks."
                    )

                    for i in range(0, len(raw_batch), self.max_batch_size):
                        chunk = raw_batch[i:i + self.max_batch_size]
                        try:
                            transformed_chunk = self.transformer.transform_raw_table_data(table_name, chunk)
                            if transformed_chunk:
                                total_processed += len(transformed_chunk)
                                yield transformed_chunk
                        except Exception as e:
                            self.logger.error(
                                f"Transformation error in {table_name} batch {batch_count}, "
                                f"chunk {i // self.max_batch_size + 1}: {e}"
                            )
                            continue
                else:
                    try:
                        transformed_batch = self.transformer.transform_raw_table_data(table_name, raw_batch)
                        if transformed_batch:
                            total_processed += len(transformed_batch)
                            yield transformed_batch

                        if batch_count % 10 == 0:
                            self.logger.debug(
                                f"Progress {table_name}: {batch_count} batches, "
                                f"{total_processed} records transformed"
                            )

                    except Exception as e:
                        self.logger.error(f"Transformation error in {table_name} batch {batch_count}: {e}")
                        continue

        except Exception as e:
            self.logger.error(f"Stream processing failed for {table_name}: {e}")
            raise

        finally:
            self.logger.info(
                f"Stream transformation completed for {table_name}: "
                f"{batch_count} batches, {total_processed} total records"
            )

    async def _etl_table_stream(
            self,
            table_name: str,
            campaign: Optional[CampaignWindow] = None,
            force_full_refresh: bool = False,
            extraction_strategy: Optional[ExtractionStrategy] = None,
            update_watermark: bool = True
    ) -> TableLoadResult:
        """
        🎯 Ejecuta ETL para una tabla con estrategia híbrida.

        Args:
            table_name: Nombre de la tabla
            campaign: Contexto de campaña (opcional)
            force_full_refresh: Forzar refresh completo
            extraction_strategy: Estrategia específica o auto-detección
            update_watermark: Si actualizar watermark al finalizar
        """
        extraction_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        watermark_manager = await self._get_watermark_manager()

        self.logger.info(f"🚀 Starting ETL for {table_name} (ID: {extraction_id})")

        try:
            config = ETLConfig.get_config(table_name)

            # Determinar estrategia de extracción
            if extraction_strategy == ExtractionStrategy.HYBRID_AUTO or extraction_strategy is None:
                strategy = await self._determine_extraction_strategy(table_name, campaign)
            else:
                strategy = extraction_strategy

            # Marcar inicio en watermark si es apropiado
            if update_watermark:
                await watermark_manager.start_extraction(table_name, extraction_id)

            # Construir query según estrategia
            query = await self._build_hybrid_query(
                table_name, config, campaign, strategy, force_full_refresh
            )

            # Stream de datos: Extract → Transform → Load
            raw_stream = self.extractor.stream_custom_query(query, config.batch_size)
            transformed_stream = self._transform_stream_with_backpressure(table_name, raw_stream)

            # Cargar datos
            load_result = await self.loader.load_data_streaming(
                table_name=config.table_name,
                table_type=config.table_type,
                data_stream=transformed_stream,
                primary_key=config.primary_key,
                upsert=True
            )

            if load_result.status not in ["success", "partial_success"]:
                raise Exception(f"Load failed: {load_result.error_message}")

            duration = (datetime.now() - start_time).total_seconds()
            records_loaded = getattr(load_result, 'inserted_records', 0) + getattr(load_result, 'updated_records', 0)

            # Actualizar watermark solo si es apropiado
            if update_watermark:
                # Para estrategia calendario, usar la fecha más reciente de la campaña
                if strategy == ExtractionStrategy.CALENDAR_DRIVEN and campaign:
                    watermark_timestamp = campaign.fecha_cierre or campaign.fecha_apertura
                    if isinstance(watermark_timestamp, date):
                        watermark_timestamp = datetime.combine(watermark_timestamp, datetime.min.time())
                else:
                    watermark_timestamp = datetime.now()

                await watermark_manager.update_watermark(
                    table_name=table_name,
                    timestamp=watermark_timestamp,
                    records_extracted=records_loaded,
                    extraction_duration_seconds=duration,
                    status="success",
                    extraction_id=extraction_id,
                    metadata={"strategy": strategy.value, "campaign": campaign.archivo if campaign else None}
                )

            self.logger.info(
                f"✅ ETL completed for {table_name}: {records_loaded} records, "
                f"strategy: {strategy.value}, duration: {duration:.2f}s"
            )

            return TableLoadResult(
                table_name=table_name,
                records_processed=getattr(load_result, 'processed_records', 0) or records_loaded,
                records_loaded=records_loaded,
                duration_seconds=duration,
                status="success",
                error_message=None
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"ETL failed for {table_name}: {str(e)}"
            self.logger.error(error_msg)

            if update_watermark:
                try:
                    await watermark_manager.update_watermark(
                        table_name=table_name,
                        timestamp=datetime.now(),
                        status="failed",
                        error_message=error_msg,
                        extraction_id=extraction_id,
                        extraction_duration_seconds=duration
                    )
                except Exception as wm_error:
                    self.logger.error(f"Failed to update watermark: {wm_error}")

            return TableLoadResult(
                table_name=table_name,
                records_processed=0,
                records_loaded=0,
                duration_seconds=duration,
                status="failed",
                error_message=error_msg
            )

    async def run_for_single_campaign(self, campaign: CampaignWindow) -> int:
        """
        🎯 NUEVO MÉTODO: Ejecuta la carga de TODAS las tablas raw para UNA SOLA campaña.
        Este es el punto de entrada que usará el orquestador principal.
        Devuelve el número total de registros cargados.
        """
        self.logger.info(f"🚀 Starting Raw Data load for single campaign: '{campaign.archivo}'")

        tables_to_load = ETLConfig.get_raw_source_tables()
        semaphore = asyncio.Semaphore(3)  # Limita la concurrencia

        async def process_table(table_name: str):
            async with semaphore:
                return await self._etl_table_stream(
                    table_name=table_name,
                    campaign=campaign,
                    extraction_strategy=ExtractionStrategy.CALENDAR_DRIVEN
                )

        tasks = [process_table(table_name) for table_name in tables_to_load]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_records = 0
        failed_tables_info = []
        for i, res in enumerate(results):
            if isinstance(res, TableLoadResult) and res.status == 'success':
                total_records += res.records_loaded
            else:
                table_name = tables_to_load[i]
                error_msg = str(res.error_message) if isinstance(res, TableLoadResult) else str(res)
                failed_tables_info.append(f"{table_name}: {error_msg}")

        if failed_tables_info:
            raise Exception(
                f"Raw data pipeline failed for campaign '{campaign.archivo}'. Errors in tables: {'; '.join(failed_tables_info)}")

        self.logger.info(f"✅ Raw Data load for '{campaign.archivo}' finished. Loaded {total_records} records.")
        return total_records


    async def run_calendar_backfill(
            self,
            campaigns: List[CampaignWindow],
            specific_tables: Optional[List[str]] = None,
            extend_windows: bool = True,
            update_watermarks: bool = True
    ) -> Dict[str, Any]:
        """
        🗓️ Ejecuta backfill masivo guiado por calendario.

        IDEAL PARA: Cargas retroactivas de múltiples campañas históricas.
        """
        start_time = datetime.now()
        self.logger.info(f"🗓️ Starting calendar-driven backfill for {len(campaigns)} campaigns")

        tables_to_load = specific_tables or ETLConfig.get_raw_source_tables()
        total_results = []

        for i, campaign in enumerate(campaigns, 1):
            campaign_start = datetime.now()
            self.logger.info(f"📅 Processing campaign {i}/{len(campaigns)}: {campaign.archivo}")

            # Procesar todas las tablas para esta campaña
            semaphore = asyncio.Semaphore(2)  # Menos paralelismo para backfill

            async def process_table_for_campaign(table_name: str):
                async with semaphore:
                    return await self._etl_table_stream(
                        table_name=table_name,
                        campaign=campaign,
                        force_full_refresh=False,
                        extraction_strategy=ExtractionStrategy.CALENDAR_DRIVEN,
                        update_watermark=update_watermarks
                    )

            tasks = [process_table_for_campaign(table_name) for table_name in tables_to_load]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Procesar resultados de esta campaña
            campaign_successful = 0
            campaign_failed = 0
            campaign_records = 0

            for j, result in enumerate(results):
                if isinstance(result, TableLoadResult) and result.status == "success":
                    campaign_successful += 1
                    campaign_records += result.records_loaded
                else:
                    campaign_failed += 1

            campaign_duration = (datetime.now() - campaign_start).total_seconds()

            campaign_result = {
                "campaign": campaign.archivo,
                "tables_successful": campaign_successful,
                "tables_failed": campaign_failed,
                "records_loaded": campaign_records,
                "duration_seconds": campaign_duration
            }

            total_results.append(campaign_result)

            self.logger.info(
                f"📅 Campaign {campaign.archivo} completed: "
                f"{campaign_successful}/{len(tables_to_load)} tables, "
                f"{campaign_records} records, {campaign_duration:.2f}s"
            )

        total_duration = (datetime.now() - start_time).total_seconds()
        total_records = sum(r["records_loaded"] for r in total_results)
        successful_campaigns = sum(1 for r in total_results if r["tables_failed"] == 0)

        summary = {
            "status": "success" if successful_campaigns == len(campaigns) else "partial",
            "mode": "calendar_backfill",
            "total_campaigns": len(campaigns),
            "successful_campaigns": successful_campaigns,
            "total_records": total_records,
            "duration_seconds": total_duration,
            "campaign_results": total_results,
            "watermarks_updated": update_watermarks
        }

        self.logger.info(
            f"🏁 Calendar backfill completed: {successful_campaigns}/{len(campaigns)} campaigns, "
            f"{total_records} total records in {total_duration:.2f}s"
        )

        return summary

    async def run_incremental_refresh(
            self,
            specific_tables: Optional[List[str]] = None,
            force_full_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        ⏰ Ejecuta refresh incremental puro basado en watermarks.

        IDEAL PARA: Cargas diarias automáticas sin contexto de campaña.
        """
        start_time = datetime.now()
        self.logger.info("⏰ Starting watermark-driven incremental refresh")

        # Cleanup previo
        watermark_manager = await self._get_watermark_manager()
        cleaned_count = await watermark_manager.cleanup_stale_extractions(timeout_minutes=30)

        if cleaned_count > 0:
            self.logger.warning(f"🧹 Cleaned up {cleaned_count} stale extractions")

        tables_to_load = specific_tables or ETLConfig.get_raw_source_tables()

        # Procesar tablas en paralelo
        semaphore = asyncio.Semaphore(3)

        async def process_table_incremental(table_name: str):
            async with semaphore:
                return await self._etl_table_stream(
                    table_name=table_name,
                    campaign=None,
                    force_full_refresh=force_full_refresh,
                    extraction_strategy=ExtractionStrategy.WATERMARK_DRIVEN,
                    update_watermark=True
                )

        tasks = [process_table_incremental(table_name) for table_name in tables_to_load]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Procesar resultados
        successful_tables = []
        failed_tables = []
        total_records = 0
        table_results = {}

        for i, result in enumerate(results):
            table_name = tables_to_load[i]

            if isinstance(result, Exception):
                failed_tables.append(table_name)
                table_results[table_name] = TableLoadResult(
                    table_name=table_name,
                    records_processed=0,
                    records_loaded=0,
                    duration_seconds=0,
                    status="failed",
                    error_message=str(result)
                )
            elif isinstance(result, TableLoadResult):
                table_results[table_name] = result
                if result.status == "success":
                    successful_tables.append(table_name)
                    total_records += result.records_loaded
                else:
                    failed_tables.append(table_name)

        duration = (datetime.now() - start_time).total_seconds()

        final_status = "success" if not failed_tables else ("partial" if successful_tables else "failed")

        summary = {
            "status": final_status,
            "mode": "incremental_refresh",
            "duration_seconds": duration,
            "total_records_loaded": total_records,
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "tables_loaded": {name: result.records_loaded for name, result in table_results.items() if
                              result.status == "success"},
            "table_results": {name: result.__dict__ for name, result in table_results.items()},
            "stale_extractions_cleaned": cleaned_count
        }

        self.logger.info(
            f"🏁 Incremental refresh completed: {len(successful_tables)}/{len(tables_to_load)} tables, "
            f"{total_records} records in {duration:.2f}s"
        )

        return summary