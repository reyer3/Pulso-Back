"""
🎯 Calendar-Driven ETL Coordinator
Intelligent data loading strategy based on campaign time windows

STRATEGY: Use calendario as master to define relevant time windows for each campaign,
then load only the data that falls within those windows.

BENEFITS:
- Efficient incremental loading 
- Respects business time windows
- Avoids loading irrelevant historical data
- Campaign-by-campaign progress tracking
"""

from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import asyncio

from app.core.logging import LoggerMixin
from app.etl.config import ETLConfig
from app.etl.extractors.bigquery_extractor import BigQueryExtractor
from app.etl.transformers.unified_transformer import get_unified_transformer
from app.etl.loaders.postgres_loader import get_loader
from app.etl.watermarks import WatermarkManager
from app.database.connection import get_database_manager


@dataclass
class CampaignWindow:
    """Campaign time window definition from calendario"""
    archivo: str
    fecha_apertura: date
    fecha_trandeuda: Optional[date]
    fecha_cierre: Optional[date]
    tipo_cartera: str
    estado_cartera: str
    
    @property
    def asignacion_window_start(self) -> date:
        """When to start looking for asignacion data"""
        # Look 7 days before apertura for assignment data
        return self.fecha_apertura - timedelta(days=7)
    
    @property
    def asignacion_window_end(self) -> date:
        """When to stop looking for asignacion data"""
        # Stop looking 30 days after apertura
        return self.fecha_apertura + timedelta(days=30)
    
    @property
    def trandeuda_window_start(self) -> date:
        """When to start looking for trandeuda data"""
        return self.fecha_trandeuda or self.fecha_apertura
    
    @property
    def trandeuda_window_end(self) -> date:
        """When to stop looking for trandeuda data"""
        return self.fecha_cierre or (datetime.now().date() + timedelta(days=1))
    
    @property
    def pagos_window_start(self) -> date:
        """When to start looking for pagos data"""
        return self.fecha_apertura
    
    @property
    def pagos_window_end(self) -> date:
        """When to stop looking for pagos data"""
        # Look for payments until 30 days after campaign closure
        end_date = self.fecha_cierre or datetime.now().date()
        return end_date + timedelta(days=30)
    
    @property
    def gestiones_window_start(self) -> date:
        """When to start looking for gestiones data"""
        return self.fecha_apertura
    
    @property
    def gestiones_window_end(self) -> date:
        """When to stop looking for gestiones data"""
        return self.fecha_cierre or (datetime.now().date() + timedelta(days=1))


@dataclass
class CampaignLoadResult:
    """Result of loading a specific campaign"""
    archivo: str
    status: str  # 'success', 'partial', 'failed', 'skipped'
    tables_loaded: Dict[str, int]  # table_name -> record_count
    errors: List[str]
    duration_seconds: float


class CalendarDrivenCoordinator(LoggerMixin):
    """
    Coordinates intelligent ETL loading based on calendar campaign windows
    """
    
    def __init__(self):
        super().__init__()
        self.extractor = None
        self.transformer = None
        self.loader = None
        self.watermark_manager = None
        
    async def _initialize_components(self):
        """Initialize ETL components"""
        if self.extractor is None:
            self.extractor = BigQueryExtractor()
        if self.transformer is None:
            self.transformer = get_unified_transformer()
        if self.loader is None:
            self.loader = await get_loader()
        if self.watermark_manager is None:
            db_manager = await get_database_manager()
            self.watermark_manager = WatermarkManager(db_manager)
    
    async def get_campaign_windows(self, limit: Optional[int] = None) -> List[CampaignWindow]:
        """
        Get campaign windows from calendario table
        
        Args:
            limit: Maximum number of campaigns to return (None for all)
            
        Returns:
            List of campaign windows ordered by fecha_apertura
        """
        await self._initialize_components()
        
        # Query PostgreSQL calendario for campaign definitions
        db_manager = await get_database_manager()
        
        query = """
        SELECT 
            archivo,
            fecha_apertura,
            fecha_trandeuda,
            fecha_cierre,
            tipo_cartera,
            estado_cartera
        FROM raw_calendario 
        WHERE fecha_apertura IS NOT NULL
        ORDER BY fecha_apertura ASC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            rows = await db_manager.fetch_all(query)
            
            campaigns = []
            for row in rows:
                campaign = CampaignWindow(
                    archivo=row['archivo'],
                    fecha_apertura=row['fecha_apertura'],
                    fecha_trandeuda=row['fecha_trandeuda'],
                    fecha_cierre=row['fecha_cierre'],
                    tipo_cartera=row['tipo_cartera'] or 'UNKNOWN',
                    estado_cartera=row['estado_cartera'] or 'UNKNOWN'
                )
                campaigns.append(campaign)
            
            self.logger.info(f"Found {len(campaigns)} campaign windows in calendario")
            return campaigns
            
        except Exception as e:
            self.logger.error(f"Failed to get campaign windows: {e}")
            return []
    
    async def load_campaign_data(
        self, 
        campaign: CampaignWindow,
        tables: List[str] = None,
        force_refresh: bool = False
    ) -> CampaignLoadResult:
        """
        Load all relevant data for a specific campaign
        
        Args:
            campaign: Campaign window definition
            tables: Specific tables to load (default: all relevant tables)
            force_refresh: Whether to force reload even if data exists
            
        Returns:
            Result of the campaign data loading
        """
        start_time = datetime.now()
        tables_loaded = {}
        errors = []
        
        if tables is None:
            tables = ['raw_asignaciones', 'raw_trandeuda', 'raw_pagos', 'gestiones_unificadas']
        
        self.logger.info(f"🗓️ Loading campaign {campaign.archivo} ({campaign.fecha_apertura} - {campaign.fecha_cierre})")
        
        try:
            await self._initialize_components()
            
            # Load each table with campaign-specific time windows
            for table_name in tables:
                try:
                    record_count = await self._load_campaign_table(campaign, table_name, force_refresh)
                    tables_loaded[table_name] = record_count
                    self.logger.info(f"✅ {table_name}: {record_count} records loaded")
                    
                except Exception as table_error:
                    error_msg = f"Failed to load {table_name}: {table_error}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)
                    tables_loaded[table_name] = 0
            
            # Determine overall status
            if not errors:
                status = 'success'
            elif any(count > 0 for count in tables_loaded.values()):
                status = 'partial'
            else:
                status = 'failed'
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return CampaignLoadResult(
                archivo=campaign.archivo,
                status=status,
                tables_loaded=tables_loaded,
                errors=errors,
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Campaign loading failed: {e}"
            self.logger.error(error_msg)
            
            return CampaignLoadResult(
                archivo=campaign.archivo,
                status='failed',
                tables_loaded=tables_loaded,
                errors=[error_msg],
                duration_seconds=duration
            )
    
    async def _load_campaign_table(
        self, 
        campaign: CampaignWindow, 
        table_name: str,
        force_refresh: bool = False
    ) -> int:
        """
        Load a specific table for a campaign using time window filtering
        
        Returns:
            Number of records loaded
        """
        
        # Get time window for this table
        if table_name == 'raw_asignaciones':
            start_date = campaign.asignacion_window_start
            end_date = campaign.asignacion_window_end
        elif table_name == 'raw_trandeuda':
            start_date = campaign.trandeuda_window_start
            end_date = campaign.trandeuda_window_end
        elif table_name == 'raw_pagos':
            start_date = campaign.pagos_window_start
            end_date = campaign.pagos_window_end
        elif table_name == 'gestiones_unificadas':
            start_date = campaign.gestiones_window_start
            end_date = campaign.gestiones_window_end
        else:
            raise ValueError(f"Unknown table for campaign loading: {table_name}")
        
        # Build custom query with campaign-specific filters
        custom_query = self._build_campaign_query(table_name, campaign, start_date, end_date)
        
        # Extract data using custom query
        extracted_data = await self.extractor.extract_data_with_query(custom_query)
        
        if not extracted_data:
            self.logger.info(f"No data found for {table_name} in campaign {campaign.archivo}")
            return 0
        
        # Transform data
        transformed_data = await self.transformer.transform_table_data(table_name, extracted_data)
        
        if not transformed_data:
            self.logger.warning(f"No valid data after transformation for {table_name}")
            return 0
        
        # Load data
        config = ETLConfig.get_config(table_name)
        result = await self.loader.load_data_batch(
            table_name=table_name,
            data=transformed_data,
            primary_key=config.primary_key,
            upsert=True
        )
        
        if result.status == 'success':
            return result.inserted_records
        else:
            raise Exception(f"Load failed: {result.error_message}")
    
    def _build_campaign_query(
        self, 
        table_name: str, 
        campaign: CampaignWindow,
        start_date: date, 
        end_date: date
    ) -> str:
        """
        Build BigQuery with campaign-specific filters
        """
        
        # Get base query template
        base_query = ETLConfig.get_query_template(table_name)
        
        # Build campaign-specific filters
        if table_name == 'raw_asignaciones':
            campaign_filter = f"""
            (archivo = '{campaign.archivo}' 
             OR DATE(creado_el) BETWEEN '{start_date}' AND '{end_date}')
            """
        elif table_name == 'raw_trandeuda':
            campaign_filter = f"""
            (archivo LIKE '%{campaign.archivo.split('_')[0]}%'
             OR DATE(creado_el) BETWEEN '{start_date}' AND '{end_date}')
            """
        elif table_name == 'raw_pagos':
            campaign_filter = f"""
            fecha_pago BETWEEN '{start_date}' AND '{end_date}'
            """
        elif table_name == 'gestiones_unificadas':
            campaign_filter = f"""
            DATE(timestamp_gestion) BETWEEN '{start_date}' AND '{end_date}'
            """
        else:
            campaign_filter = "1=1"
        
        # Replace the incremental filter with campaign filter
        custom_query = base_query.format(incremental_filter=campaign_filter)
        
        return custom_query
    
    async def catch_up_all_campaigns(
        self,
        batch_size: int = 5,
        max_campaigns: Optional[int] = None,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Intelligent catch-up: Load all campaign data in chronological order
        
        Args:
            batch_size: Number of campaigns to process in parallel
            max_campaigns: Maximum campaigns to process (None for all)
            force_refresh: Whether to force reload existing data
            
        Returns:
            Summary of catch-up operation
        """
        start_time = datetime.now()
        
        self.logger.info(f"🚀 Starting intelligent catch-up for campaigns (batch_size={batch_size})")
        
        # Get all campaign windows
        campaigns = await self.get_campaign_windows(limit=max_campaigns)
        
        if not campaigns:
            return {
                'status': 'failed',
                'message': 'No campaigns found in calendario',
                'campaigns_processed': 0,
                'duration_seconds': 0
            }
        
        # Process campaigns in batches
        results = []
        total_campaigns = len(campaigns)
        processed_campaigns = 0
        
        for i in range(0, total_campaigns, batch_size):
            batch = campaigns[i:i + batch_size]
            
            self.logger.info(f"📦 Processing batch {i//batch_size + 1}: campaigns {i+1}-{min(i+batch_size, total_campaigns)}")
            
            # Process batch in parallel
            batch_tasks = [
                self.load_campaign_data(campaign, force_refresh=force_refresh)
                for campaign in batch
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    self.logger.error(f"Campaign loading exception: {result}")
                else:
                    results.append(result)
                    processed_campaigns += 1
        
        # Calculate summary statistics
        successful_campaigns = sum(1 for r in results if r.status == 'success')
        partial_campaigns = sum(1 for r in results if r.status == 'partial')
        failed_campaigns = sum(1 for r in results if r.status == 'failed')
        
        total_records = {}
        for result in results:
            for table, count in result.tables_loaded.items():
                total_records[table] = total_records.get(table, 0) + count
        
        duration = (datetime.now() - start_time).total_seconds()
        
        summary = {
            'status': 'success' if failed_campaigns == 0 else 'partial',
            'campaigns_total': total_campaigns,
            'campaigns_processed': processed_campaigns,
            'campaigns_successful': successful_campaigns,
            'campaigns_partial': partial_campaigns,
            'campaigns_failed': failed_campaigns,
            'records_loaded_by_table': total_records,
            'duration_seconds': duration,
            'campaigns_per_minute': (processed_campaigns / duration) * 60 if duration > 0 else 0
        }
        
        self.logger.info(f"🎯 Catch-up completed: {successful_campaigns}/{total_campaigns} campaigns successful")
        self.logger.info(f"📊 Total records loaded: {sum(total_records.values())}")
        
        return summary


# Global coordinator instance
_coordinator: Optional[CalendarDrivenCoordinator] = None

def get_calendar_coordinator() -> CalendarDrivenCoordinator:
    """Get singleton calendar coordinator instance"""
    global _coordinator
    
    if _coordinator is None:
        _coordinator = CalendarDrivenCoordinator()
    
    return _coordinator
