# app/etl/pipelines/campaign_catchup_pipeline.py

"""
🎯 Campaign Catch-Up Pipeline
Orchestrates ETL for processing historical campaigns from raw data through to mart layers.
"""

from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Any, Optional, Type
from dataclasses import dataclass
import asyncio

from app.core.logging import LoggerMixin
from app.etl.config import ETLConfig, TableType # Updated import
from app.etl.extractors.bigquery_extractor import BigQueryExtractor
# Assuming RawDataTransformer is the correct one, not UnifiedTransformerRegistry directly for raw transformation
from app.etl.transformers.raw_data_transformer import RawDataTransformer
from app.etl.loaders.postgres_loader import PostgresLoader, LoadResult
from app.etl.watermarks import get_watermark_manager, WatermarkManager
from app.database.connection import get_database_manager, DatabaseManager
from app.etl.builders.mart_builder import MartBuilder # New import


@dataclass
class CampaignWindow:
    archivo: str
    fecha_apertura: date
    fecha_trandeuda: Optional[date]
    fecha_cierre: Optional[date]
    tipo_cartera: str
    estado_cartera: str

    # Properties to calculate date windows for different tables
    @property
    def asignacion_window_start(self) -> date:
        return self.fecha_apertura - timedelta(days=7) # Example: lookback for assignments

    @property
    def asignacion_window_end(self) -> date:
        # Assignments might be relevant for some time after campaign start
        return self.fecha_apertura + timedelta(days=30)

    @property
    def trandeuda_window_start(self) -> date:
        return self.fecha_trandeuda or self.fecha_apertura

    @property
    def trandeuda_window_end(self) -> date:
        # Up to campaign closure or current date if ongoing
        return self.fecha_cierre or (datetime.now(timezone.utc).date() + timedelta(days=1))

    @property
    def pagos_window_start(self) -> date:
        return self.fecha_apertura

    @property
    def pagos_window_end(self) -> date:
        end_date = self.fecha_cierre or datetime.now(timezone.utc).date()
        # Payments might come in for a while after campaign closure
        return end_date + timedelta(days=30)

    @property
    def gestiones_window_start(self) -> date: # For voicebot & mibotair gestiones
        return self.fecha_apertura

    @property
    def gestiones_window_end(self) -> date: # For voicebot & mibotair gestiones
        return self.fecha_cierre or (datetime.now(timezone.utc).date() + timedelta(days=1))


@dataclass
class CampaignPipelineResult: # Renamed from CampaignLoadResult for clarity
    archivo: str
    status: str # success, partial_success, failed
    raw_tables_loaded: Dict[str, int] # Records loaded per raw table
    mart_build_status: Optional[str] = None # Status of MartBuilder execution
    mart_build_error: Optional[str] = None
    errors: List[str] # Errors during raw data loading
    duration_seconds: float


class CampaignCatchUpPipeline(LoggerMixin):
    """
    Pipeline for processing historical campaigns:
    1. Loads all relevant raw data for a campaign window.
    2. Triggers MartBuilder to build aux and mart layers for that campaign.
    """

    def __init__(
        self,
        extractor: BigQueryExtractor,
        transformer: RawDataTransformer, # Specific transformer for raw data
        loader: PostgresLoader,
        mart_builder: MartBuilder,
        db_manager: DatabaseManager,
        config: Type[ETLConfig], # Pass ETLConfig class itself
        watermark_manager: WatermarkManager
    ):
        super().__init__()
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.mart_builder = mart_builder
        self.db_manager = db_manager
        self.config_class = config # Store the class
        self.watermark_manager = watermark_manager

        self._cancel_event = asyncio.Event()
        self._is_running = False

    @staticmethod
    def get_campaign_watermark_name(campaign_archivo: str) -> str:
        """Standardize watermark names for overall campaign processing status."""
        return f"campaign_pipeline__{campaign_archivo}"

    async def get_campaign_windows_to_process(
        self,
        limit: Optional[int] = None,
        force_refresh: bool = False
    ) -> List[CampaignWindow]:
        """
        Gets campaign windows from 'calendario' that need processing.
        Skips campaigns that are closed and successfully processed, unless force_refresh is True.
        """
        calendario_fqn = self.config_class.get_fq_table_name("calendario", TableType.RAW)
        query = f"""
        SELECT
            archivo, fecha_apertura, fecha_trandeuda, fecha_cierre,
            tipo_cartera, estado_cartera
        FROM {calendario_fqn}
        ORDER BY fecha_apertura DESC -- Process more recent campaigns first, or adjust as needed
        """
        if limit:
            query += f" LIMIT {limit}"

        try:
            rows = await self.db_manager.execute_query(query, fetch="all")
            all_campaign_windows = [CampaignWindow(**dict(row)) for row in rows]
            self.logger.info(f"Found {len(all_campaign_windows)} total campaign windows in {calendario_fqn}.")

            if force_refresh:
                self.logger.info("Force refresh requested, processing all fetched campaigns.")
                return all_campaign_windows

            campaigns_to_process = []
            for cw in all_campaign_windows:
                watermark_name = self.get_campaign_watermark_name(cw.archivo)
                watermark = await self.watermark_manager.get_watermark(watermark_name)

                # Process if:
                # 1. No watermark exists OR
                # 2. Watermark status is not 'success' OR
                # 3. Campaign is still 'ABIERTA' (even if previously successful, might need updates)
                if not watermark or watermark.last_extraction_status != 'success' or cw.estado_cartera == 'ABIERTA':
                    campaigns_to_process.append(cw)
                else:
                    self.logger.info(f"Skipping already processed campaign '{cw.archivo}' (Status: {watermark.last_extraction_status}, Estado Cartera: {cw.estado_cartera}).")

            self.logger.info(f"Selected {len(campaigns_to_process)} campaigns for processing after watermark check.")
            return campaigns_to_process

        except Exception as e:
            self.logger.error(f"Failed to get campaign windows: {e}", exc_info=True)
            return []

    async def run_for_campaign(
        self,
        campaign: CampaignWindow,
        force_refresh_raw: bool = False # Force refresh for individual raw tables
    ) -> CampaignPipelineResult:
        """
        Orchestrates the ETL process for a single campaign window.
        1. Loads all relevant raw tables.
        2. If raw loading is successful, calls MartBuilder.
        """
        start_time = datetime.now(timezone.utc)
        pipeline_watermark_name = self.get_campaign_watermark_name(campaign.archivo)
        self.logger.info(f"🚀 Starting CampaignCatchUpPipeline for '{campaign.archivo}'...")

        await self.watermark_manager.start_extraction(pipeline_watermark_name, f"campaign_pipeline_run_{start_time.isoformat()}")

        raw_tables_loaded: Dict[str, int] = {}
        errors: List[str] = []

        # Define raw tables to load for a campaign
        # These are base names, ETLConfig.get_config will be used
        raw_tables_to_process = [
            "asignaciones", "trandeuda", "pagos",
            "voicebot_gestiones", "mibotair_gestiones",
            "homologacion_mibotair", "homologacion_voicebot", "ejecutivos"
        ]
        # Calendario is assumed to be pre-loaded or loaded by a separate dimension pipeline.

        for table_base_name in raw_tables_to_process:
            if self._cancel_event.is_set():
                self.logger.warning(f"Cancellation signal received during raw load for '{campaign.archivo}'. Aborting.")
                errors.append("Pipeline cancelled during raw data loading.")
                break
            try:
                load_count = await self._load_single_raw_table_for_campaign(
                    campaign,
                    table_base_name,
                    force_refresh=force_refresh_raw
                )
                raw_tables_loaded[table_base_name] = load_count
            except Exception as e:
                self.logger.error(f"Failed to load raw table '{table_base_name}' for campaign '{campaign.archivo}': {e}", exc_info=True)
                errors.append(f"Error loading {table_base_name}: {str(e)}")
                raw_tables_loaded[table_base_name] = 0

        mart_build_status: Optional[str] = None
        mart_build_error: Optional[str] = None

        if not errors and not self._cancel_event.is_set(): # Proceed to mart building only if raw loads were successful
            self.logger.info(f"Raw data loading successful for '{campaign.archivo}'. Proceeding to MartBuilder.")
            try:
                await self.mart_builder.build_for_campaign(campaign)
                mart_build_status = "success"
                self.logger.info(f"MartBuilder completed successfully for '{campaign.archivo}'.")
            except Exception as e:
                self.logger.error(f"MartBuilder failed for campaign '{campaign.archivo}': {e}", exc_info=True)
                mart_build_status = "failed"
                mart_build_error = str(e)
                errors.append(f"MartBuilder error: {str(e)}")
        elif self._cancel_event.is_set():
            mart_build_status = "cancelled"
        else:
            mart_build_status = "skipped_due_to_raw_errors"
            self.logger.warning(f"Skipping MartBuilder for '{campaign.archivo}' due to errors in raw data loading.")

        final_status = "success"
        if errors:
            final_status = "partial_success" if any(raw_tables_loaded.values()) or mart_build_status == "success" else "failed"
        if self._cancel_event.is_set():
            final_status = "cancelled"

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        await self.watermark_manager.update_watermark(
            table_name=pipeline_watermark_name,
            timestamp=datetime.now(timezone.utc),
            records_extracted=sum(raw_tables_loaded.values()), # Sum of records from all raw tables
            extraction_duration_seconds=duration,
            status=final_status,
            error_message=", ".join(errors) or mart_build_error or None,
            metadata={"mart_build_status": mart_build_status}
        )

        self.logger.info(f"🏁 Finished CampaignCatchUpPipeline for '{campaign.archivo}' with status: {final_status} in {duration:.2f}s.")
        return CampaignPipelineResult(
            archivo=campaign.archivo,
            status=final_status,
            raw_tables_loaded=raw_tables_loaded,
            mart_build_status=mart_build_status,
            mart_build_error=mart_build_error,
            errors=errors,
            duration_seconds=duration
        )

    async def _load_single_raw_table_for_campaign(
        self,
        campaign: CampaignWindow,
        table_base_name: str, # Base name, e.g., "asignaciones"
        force_refresh: bool = False
    ) -> int:
        """Loads a single raw table for a specific campaign window."""
        self.logger.debug(f"Loading raw table '{table_base_name}' for campaign '{campaign.archivo}'...")
        config = self.config_class.get_config(table_base_name)

        # Determine date range for query based on table type and campaign window
        start_date, end_date = self._get_date_range_for_table(campaign, table_base_name)

        # Build campaign-specific query
        # The get_query method in ETLConfig needs to handle since_date properly.
        # For campaign-specific loads, we might not use the watermark's since_date directly,
        # but rather the campaign window's dates.
        # The "incremental_filter" placeholder in templates will be replaced by campaign-specific date logic.

        query_template = self.config_class.get_query_template(table_base_name)

        # This logic is simplified; real implementation might need more robust date handling
        # for different tables (e.g. `creado_el` vs `fecha_gestion` vs `fecha_pago`)
        incremental_column = config.incremental_column or "1=1" # Fallback if no incremental column
        date_filter_column = incremental_column # Default to incremental_column for filtering

        if table_base_name == "calendario": # Calendario is special
            date_filter_column = "fecha_apertura" # Or how it's filtered
            # This specific pipeline assumes calendario is already mostly up-to-date.
            # A full load of calendario might be handled by a DimensionLoadPipeline.
            # Here, we might just ensure the specific campaign's entry is present if needed.
            # For simplicity, we'll assume it's extracted based on a general incremental logic.
            # Or, this pipeline doesn't load "calendario" itself but consumes it.
            # For now, this method won't load "calendario".
            if table_base_name == "calendario":
                 self.logger.info("Skipping direct load of 'calendario' table within campaign pipeline run.")
                 return 0


        # Construct the filter condition string
        # This needs to be robust. The original _build_campaign_query had specific logic per table.
        filter_condition = "1=1" # Default for full refresh tables
        if config.default_mode != ExtractionMode.FULL_REFRESH and start_date and end_date and date_filter_column != "1=1":
             filter_condition = f"DATE({date_filter_column}) BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"
             if table_base_name == "asignaciones": # Special handling as in original coordinator
                 filter_condition = f"(archivo = '{campaign.archivo}' OR {filter_condition})"
             elif table_base_name == "trandeuda":
                 base_archivo = campaign.archivo.split('_')[0] # Example: Cartera_Agencia_...
                 filter_condition = f"(archivo LIKE '{base_archivo}%' AND {filter_condition})"


        # Format the query
        if table_base_name in ["voicebot_gestiones", "mibotair_gestiones"]: # These need project_id, dataset_id
            custom_query = query_template.format(
                project_id=self.config_class.PROJECT_ID,
                dataset_id=self.config_class.DATASET,
                incremental_filter=filter_condition
            )
        else:
            custom_query = query_template.format(incremental_filter=filter_condition)

        self.logger.debug(f"Query for {table_base_name} (Campaign: {campaign.archivo}):\n{custom_query}")

        raw_data_stream = self.extractor.stream_custom_query(custom_query, batch_size=config.batch_size)

        # All raw tables are transformed by RawDataTransformer
        transformed_data_stream = self.transformer.transform_stream(table_base_name, raw_data_stream)

        load_result = await self.loader.load_data_streaming(
            table_name=table_base_name,
            table_type=config.table_type, # Pass the configured TableType
            data_stream=transformed_data_stream,
            primary_key=config.primary_key,
            upsert=True
        )

        if load_result.status in ["success", "partial_success"]:
            self.logger.info(f"Successfully loaded {load_result.inserted_records} records for '{table_base_name}' in campaign '{campaign.archivo}'.")
            return load_result.inserted_records
        else:
            raise Exception(f"Load failed for {table_base_name} in campaign '{campaign.archivo}': {load_result.error_message}")

    def _get_date_range_for_table(self, campaign: CampaignWindow, table_base_name: str) -> tuple[Optional[date], Optional[date]]:
        """Determines the appropriate start and end date for querying a table for a given campaign."""
        if table_base_name == "asignaciones":
            return campaign.asignacion_window_start, campaign.asignacion_window_end
        elif table_base_name == "trandeuda":
            return campaign.trandeuda_window_start, campaign.trandeuda_window_end
        elif table_base_name == "pagos":
            return campaign.pagos_window_start, campaign.pagos_window_end
        elif table_base_name in ["voicebot_gestiones", "mibotair_gestiones"]:
            return campaign.gestiones_window_start, campaign.gestiones_window_end
        elif table_base_name in ["homologacion_mibotair", "homologacion_voicebot", "ejecutivos"]:
            return None, None # These are typically full refresh, no specific campaign date range needed for query
        # Add other table-specific date logic if needed
        self.logger.warning(f"No specific date range logic for table '{table_base_name}', defaulting to full campaign span or no date filter.")
        # Fallback to full campaign span if no specific logic, or None if not applicable
        return campaign.fecha_apertura, campaign.fecha_cierre or datetime.now(timezone.utc).date()


    async def run_all_pending_campaigns(
        self,
        batch_size: int = 1, # Process one campaign at a time by default for stability
        max_campaigns: Optional[int] = None,
        force_refresh_all: bool = False, # Force re-processing of all raw data and mart build for fetched campaigns
        force_refresh_raw_data_only: bool = False # Force re-load of raw data, but MartBuilder might skip if aux is fresh
    ) -> Dict[str, Any]:
        """
        Intelligently loads data for all pending campaigns.
        """
        if self._is_running:
            self.logger.warning("Campaign catch-up process is already running.")
            return {'status': 'already_running', 'message': 'A catch-up process is already in progress.'}

        self._is_running = True
        self._cancel_event.clear()
        overall_start_time = datetime.now(timezone.utc)

        summary_results: List[CampaignPipelineResult] = []

        try:
            self.logger.info(f"🚀 Starting campaign catch-up (force_refresh_all={force_refresh_all}, force_refresh_raw_data_only={force_refresh_raw_data_only})")

            campaigns_to_run = await self.get_campaign_windows_to_process(
                limit=max_campaigns,
                force_refresh=force_refresh_all # force_refresh at this level means re-evaluate campaign for processing
            )

            if not campaigns_to_run:
                self.logger.info("No pending campaigns to process.")
                return {'status': 'no_pending_campaigns', 'message': 'All campaigns are up-to-date or none found.', 'duration_seconds': 0}

            self.logger.info(f"Processing {len(campaigns_to_run)} campaigns in batches of {batch_size}.")

            for i in range(0, len(campaigns_to_run), batch_size):
                if self._cancel_event.is_set():
                    self.logger.info("Process cancelled by user. Stopping batch processing.")
                    break

                current_batch = campaigns_to_run[i:i + batch_size]
                self.logger.info(f"📦 Processing campaign batch {i // batch_size + 1}/{ (len(campaigns_to_run) + batch_size -1) // batch_size }: {[c.archivo for c in current_batch]}")

                tasks = [
                    self.run_for_campaign(
                        campaign=cw,
                        force_refresh_raw=(force_refresh_all or force_refresh_raw_data_only)
                    ) for cw in current_batch
                ]
                batch_run_results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in batch_run_results:
                    if isinstance(res, CampaignPipelineResult):
                        summary_results.append(res)
                    elif isinstance(res, Exception):
                        # This should ideally be handled within run_for_campaign to produce a CampaignPipelineResult
                        self.logger.error(f"Unhandled exception during campaign batch processing: {res}", exc_info=True)
                        # Create a dummy error result if needed
                        summary_results.append(CampaignPipelineResult(
                            archivo="UNKNOWN_DUE_TO_ERROR", status="failed",
                            raw_tables_loaded={}, errors=[str(res)], duration_seconds=0
                        ))

            final_status = 'completed'
            if self._cancel_event.is_set():
                final_status = 'cancelled'
            elif any(r.status == 'failed' for r in summary_results):
                final_status = 'completed_with_errors'
            elif any(r.status == 'partial_success' for r in summary_results):
                final_status = 'completed_with_partial_success'

            return {
                'status': final_status,
                'force_refresh_all_used': force_refresh_all,
                'force_refresh_raw_data_only_used': force_refresh_raw_data_only,
                'total_campaign_windows_fetched': len(campaigns_to_run), # This is actually campaigns attempted
                'campaigns_processed_count': len(summary_results),
                'successful_campaigns': sum(1 for r in summary_results if r.status == 'success'),
                'partial_success_campaigns': sum(1 for r in summary_results if r.status == 'partial_success'),
                'failed_campaigns': sum(1 for r in summary_results if r.status == 'failed'),
                'duration_seconds': (datetime.now(timezone.utc) - overall_start_time).total_seconds(),
                'details': [res.__dict__ for res in summary_results]
            }

        except Exception as e:
            self.logger.error(f"Critical error during campaign catch-up process: {e}", exc_info=True)
            return {
                'status': 'critical_failure',
                'message': str(e),
                'duration_seconds': (datetime.now(timezone.utc) - overall_start_time).total_seconds()
            }
        finally:
            self._is_running = False
            self._cancel_event.clear()

    def cancel_processing(self):
        """Signals the pipeline to stop processing after the current campaign/batch."""
        if self._is_running:
            self.logger.warning("🛑 Cancellation signal received for CampaignCatchUpPipeline.")
            self._cancel_event.set()
        else:
            self.logger.info("CampaignCatchUpPipeline is not currently running.")


# --- Singleton Instance ---
_campaign_catchup_pipeline: Optional[CampaignCatchUpPipeline] = None

async def get_campaign_catchup_pipeline() -> CampaignCatchUpPipeline:
    """
    Gets the singleton CampaignCatchUpPipeline instance, initializing components if needed.
    """
    global _campaign_catchup_pipeline
    if _campaign_catchup_pipeline is None:
        # Initialize components first
        db_manager = await get_database_manager()
        watermark_manager = await get_watermark_manager()
        extractor = BigQueryExtractor() # Uses ETLConfig internally
        # The transformer for raw data is RawDataTransformer
        raw_transformer = RawDataTransformer() # Does not depend on other components for init
        loader = PostgresLoader(db_manager=db_manager) # Needs db_manager

        # MartBuilder needs db_manager and project_uid from ETLConfig
        mart_builder = MartBuilder(db_manager=db_manager, project_uid=ETLConfig.PROJECT_UID)

        _campaign_catchup_pipeline = CampaignCatchUpPipeline(
            extractor=extractor,
            transformer=raw_transformer, # Pass RawDataTransformer instance
            loader=loader,
            mart_builder=mart_builder,
            db_manager=db_manager,
            config=ETLConfig, # Pass the class
            watermark_manager=watermark_manager
        )
    return _campaign_catchup_pipeline
