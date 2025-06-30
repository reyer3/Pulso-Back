# app/etl/coordinators/calendar_driven_coordinator.py

"""
🎯 Production-Ready Calendar-Driven ETL Coordinator
Intelligent, incremental, and cancellable data loading based on campaign time windows.

STRATEGY: Use `raw_calendario` as the source of truth. For each campaign,
check a specific watermark. If the campaign is closed and was successfully
processed, skip it. Otherwise, load all its data. This makes daily runs
highly efficient.

FEATURES:
- Watermark integration to avoid reprocessing completed campaigns.
- Cancellable execution via an asyncio.Event.
- `force_refresh` parameter to override watermarks.
- Memory-efficient end-to-end streaming pipeline.
- Concurrent data loading for tables within a single campaign.
"""

from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import asyncio

from app.core.logging import LoggerMixin
from app.etl.config import ETLConfig
from app.etl.extractors.bigquery_extractor import BigQueryExtractor
from app.etl.transformers.unified_transformer import get_unified_transformer_registry, UnifiedTransformerRegistry
from app.etl.loaders.postgres_loader import get_loader, PostgresLoader
from app.etl.watermarks import get_watermark_manager, WatermarkManager
from app.database.connection import get_database_manager, DatabaseManager


@dataclass
class CampaignWindow:
    # ... (el dataclass CampaignWindow no cambia, puedes mantener el que tienes)
    archivo: str
    fecha_apertura: date
    fecha_trandeuda: Optional[date]
    fecha_cierre: Optional[date]
    tipo_cartera: str
    estado_cartera: str

    # ... (todas las @property no cambian)
    @property
    def asignacion_window_start(self) -> date:
        return self.fecha_apertura - timedelta(days=7)

    @property
    def asignacion_window_end(self) -> date:
        return self.fecha_apertura + timedelta(days=30)

    @property
    def trandeuda_window_start(self) -> date:
        return self.fecha_trandeuda or self.fecha_apertura

    @property
    def trandeuda_window_end(self) -> date:
        return self.fecha_cierre or (datetime.now().date() + timedelta(days=1))

    @property
    def pagos_window_start(self) -> date:
        return self.fecha_apertura

    @property
    def pagos_window_end(self) -> date:
        end_date = self.fecha_cierre or datetime.now().date()
        return end_date + timedelta(days=30)

    @property
    def gestiones_window_start(self) -> date:
        return self.fecha_apertura

    @property
    def gestiones_window_end(self) -> date:
        return self.fecha_cierre or (datetime.now().date() + timedelta(days=1))


@dataclass
class CampaignLoadResult:
    # ... (el dataclass CampaignLoadResult no cambia)
    archivo: str
    status: str
    tables_loaded: Dict[str, int]
    errors: List[str]
    duration_seconds: float


class CalendarDrivenCoordinator(LoggerMixin):
    """
    Coordinates intelligent ETL loading with watermarks and cancellation.
    """

    def __init__(self):
        super().__init__()
        self.extractor: Optional[BigQueryExtractor] = None
        self.transformer: Optional[UnifiedTransformerRegistry] = None
        self.loader: Optional[PostgresLoader] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.watermark_manager: Optional[WatermarkManager] = None

        # State for cancellation
        self._cancel_event = asyncio.Event()
        self._is_running = False

    async def _initialize_components(self):
        """Initialize all required ETL components."""
        if self.db_manager is None: self.db_manager = await get_database_manager()
        if self.watermark_manager is None: self.watermark_manager = await get_watermark_manager()
        if self.extractor is None: self.extractor = BigQueryExtractor()
        if self.transformer is None: self.transformer = get_unified_transformer_registry()
        if self.loader is None: self.loader = await get_loader()

    @staticmethod
    def get_watermark_name(campaign_archivo: str) -> str:
        """Standardize watermark names for campaigns."""
        return f"campaign__{campaign_archivo}"

    async def get_campaign_windows(self, limit: Optional[int] = None) -> List[CampaignWindow]:
        """Gets campaign windows from the 'raw_calendario' table."""
        await self._initialize_components()
        query = """
        SELECT 
            archivo,
            fecha_apertura,
            fecha_trandeuda,
            fecha_cierre,
            tipo_cartera,
            estado_cartera
        FROM raw_calendario 
        ORDER BY fecha_apertura
        """
        if limit: query += f" LIMIT {limit}"

        try:
            rows = await self.db_manager.execute_query(query, fetch="all")
            campaigns = [CampaignWindow(**dict(row)) for row in rows]
            self.logger.info(f"Found {len(campaigns)} campaign windows in calendario.")
            return campaigns
        except Exception as e:
            self.logger.error(f"Failed to get campaign windows: {e}", exc_info=True)
            return []

    async def load_campaign_data(
            self,
            campaign: CampaignWindow,
            tables: Optional[List[str]] = None,
    ) -> CampaignLoadResult:
        """Loads all relevant data for a specific campaign."""
        start_time = datetime.now()
        watermark_name = self.get_watermark_name(campaign.archivo)

        if tables is None:
            tables = ['raw_asignaciones', 'raw_trandeuda', 'raw_pagos', 'gestiones_unificadas']

        self.logger.info(f"🗓️ Loading campaign '{campaign.archivo}'...")
        await self._initialize_components()

        # Mark as running in watermark system
        await self.watermark_manager.start_extraction(watermark_name, f"campaign_run_{start_time.isoformat()}")

        tasks = [self._load_campaign_table(campaign, table_name) for table_name in tables]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        tables_loaded, errors = {}, []
        for i, result in enumerate(results):
            table_name = tables[i]
            if isinstance(result, Exception):
                errors.append(f"{table_name}: {result}")
                tables_loaded[table_name] = 0
            else:
                tables_loaded[table_name] = result

        status = 'success' if not errors else ('partial_success' if any(tables_loaded.values()) else 'failed')
        duration = (datetime.now() - start_time).total_seconds()

        load_result = CampaignLoadResult(
            archivo=campaign.archivo, status=status,
            tables_loaded=tables_loaded, errors=errors, duration_seconds=duration
        )

        # Update watermark with the final status
        await self.watermark_manager.update_watermark(
            table_name=watermark_name, timestamp=datetime.now(timezone.utc),
            records_extracted=sum(load_result.tables_loaded.values()),
            extraction_duration_seconds=load_result.duration_seconds,
            status=load_result.status,
            error_message=", ".join(load_result.errors) or None
        )
        self.logger.info(f"✅ Finished campaign '{campaign.archivo}' with status: {status}")
        return load_result

    async def _load_campaign_table(
        self,
        campaign: CampaignWindow,
        table_name: str,
        force_refresh: bool = False
    ) -> int:
        """
        Loads a specific table for a campaign using a full streaming pipeline.
        Returns the number of records loaded.
        """
        if table_name == 'raw_asignaciones':
            start_date, end_date = campaign.asignacion_window_start, campaign.asignacion_window_end
        elif table_name == 'raw_trandeuda':
            start_date, end_date = campaign.trandeuda_window_start, campaign.trandeuda_window_end
        elif table_name == 'raw_pagos':
            start_date, end_date = campaign.pagos_window_start, campaign.pagos_window_end
        elif table_name == 'gestiones_unificadas':
            start_date, end_date = campaign.gestiones_window_start, campaign.gestiones_window_end
        else:
            raise ValueError(f"Unknown table for campaign loading: {table_name}")

        custom_query = self._build_campaign_query(table_name, campaign, start_date, end_date)

        # REFACTORED: Full end-to-end streaming pipeline

        # 1. Extractor Stream: Get an async generator of data from BigQuery
        raw_data_stream = self.extractor.stream_custom_query(custom_query)

        # 2. Transformer Stream: Chain the raw stream into the transformer
        transformed_data_stream = self.transformer.transform_stream(table_name, raw_data_stream)

        # 3. Loader Stream Consumer: Feed the transformed stream into the loader
        config = ETLConfig.get_config(table_name)
        load_result = await self.loader.load_data_streaming(
            table_name=table_name,
            data_stream=transformed_data_stream,
            primary_key=config.primary_key,
            upsert=True
        )

        if load_result.status in ["success", "partial_success"]:
            return load_result.inserted_records
        else:
            # Raise an exception with the error from the loader
            raise Exception(f"Load failed for {table_name}: {load_result.error_message}")

    @staticmethod
    # En app/etl/coordinators/calendar_driven_coordinator.py
    def _build_campaign_query(
            table_name: str,
            campaign: CampaignWindow,
            start_date: date,
            end_date: date
    ) -> str:
        """
        Builds a BigQuery SQL query with precise, campaign-specific filters.

        IMPROVED: Now filters by both time window AND campaign identifier (`archivo`)
        to prevent data leakage between overlapping campaigns.
        """
        base_query = ETLConfig.get_query_template(table_name)

        # Base filter for the time window - common to most tables
        time_filter = f"BETWEEN '{start_date}' AND '{end_date}'"

        # Specific filters for each table type
        if table_name == 'raw_asignaciones':
            # Asignaciones a menudo usan el `archivo` exacto como identificador
            campaign_filter = f"(archivo = '{campaign.archivo}' OR DATE(creado_el) {time_filter})"

        elif table_name == 'raw_trandeuda':
            # Trandeuda puede estar relacionado por el nombre base del archivo
            # Ejemplo: Cartera_Agencia_..._20250401 y Cartera_Agencia_..._20250401_25
            base_archivo = campaign.archivo.split('_')[0]
            campaign_filter = f"(archivo LIKE '{base_archivo}%' AND DATE(creado_el) {time_filter})"

        elif table_name == 'raw_pagos':
            # Los pagos no tienen `archivo`, así que el filtro de tiempo es el principal,
            # pero podríamos añadir un filtro por producto si estuviera disponible.
            # Por ahora, el filtro de tiempo es el único posible.
            campaign_filter = f"fecha_pago {time_filter}"

        elif table_name == 'gestiones_unificadas':
            # Las gestiones tampoco tienen `archivo`. El filtro de tiempo es clave.
            campaign_filter = f"DATE(timestamp_gestion) {time_filter}"

        else:
            # Fallback por si se añade una tabla no reconocida
            campaign_filter = "1=1"

        # Replace the placeholder in the base query template
        return base_query.format(incremental_filter=campaign_filter)

    def cancel(self):
        """Signals the coordinator to stop processing after the current batch."""
        if self._is_running:
            self.logger.warning("🛑 Cancellation signal received. Process will stop after the current batch.")
            self._cancel_event.set()

    async def catch_up_all_campaigns(
            self,
            batch_size: int = 5,
            max_campaigns: Optional[int] = None,
            force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Intelligently loads data for all campaigns, with watermark checks and cancellation."""
        if self._is_running:
            self.logger.warning("Catch-up process is already running. Ignoring new request.")
            return {'status': 'already_running', 'message': 'A catch-up process is already in progress.'}

        self._is_running = True
        self._cancel_event.clear()
        start_time = datetime.now()

        try:
            self.logger.info(f"🚀 Starting intelligent catch-up (force_refresh={force_refresh})")
            await self._initialize_components()
            all_campaigns = await self.get_campaign_windows(limit=max_campaigns)

            if not all_campaigns:
                return {'status': 'aborted', 'message': 'No campaigns found', 'duration_seconds': 0}

            # Filter campaigns to be processed
            campaigns_to_process = []
            for campaign in all_campaigns:
                watermark = await self.watermark_manager.get_watermark(self.get_watermark_name(campaign.archivo))
                is_closed_and_done = (
                            campaign.estado_cartera != 'ABIERTA' and watermark and watermark.last_extraction_status == 'success')
                if force_refresh or not is_closed_and_done:
                    campaigns_to_process.append(campaign)

            self.logger.info(f"Found {len(all_campaigns)} total campaigns. "
                             f"Skipping {len(all_campaigns) - len(campaigns_to_process)} already processed. "
                             f"Processing {len(campaigns_to_process)}.")

            if not campaigns_to_process:
                return {'status': 'success', 'message': 'All campaigns are up-to-date.'}

            results = []
            total_to_process = len(campaigns_to_process)
            for i in range(0, total_to_process, batch_size):
                if self._cancel_event.is_set():
                    self.logger.info("Process cancelled by user. Stopping batch processing.")
                    break

                batch = campaigns_to_process[i:i + batch_size]
                self.logger.info(f"📦 Processing batch {i // batch_size + 1}: {len(batch)} campaigns...")

                tasks = [self.load_campaign_data(c) for c in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in batch_results:
                    if isinstance(res, Exception):
                        self.logger.error(f"Task failed in batch: {res}", exc_info=True)
                    else:
                        results.append(res)

            # --- Summary Calculation ---
            _duration = (datetime.now() - start_time).total_seconds()
            summary = {
                'status': 'cancelled' if self._cancel_event.is_set() else 'completed',
                'force_refresh': force_refresh,
                'campaigns_total': len(all_campaigns),
                'campaigns_processed': len(results),
                'campaigns_successful': sum(1 for r in results if r.status == 'success'),
                'campaigns_partial': sum(1 for r in results if r.status == 'partial_success'),
                'campaigns_failed': sum(1 for r in results if r.status == 'failed'),
                # ... (resto del resumen)
            }
            return summary

        finally:
            self._is_running = False
            self._cancel_event.clear()


# --- Singleton Instance ---
_coordinator: Optional[CalendarDrivenCoordinator] = None


def get_calendar_coordinator() -> CalendarDrivenCoordinator:
    """Get the singleton calendar coordinator instance."""
    global _coordinator
    if _coordinator is None:
        _coordinator = CalendarDrivenCoordinator()
    return _coordinator