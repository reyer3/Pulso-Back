"""
🚀 Production ETL Pipeline for Pulso Dashboard
Orchestrates incremental extraction, transformation, and loading

Features:
- End-to-end ETL pipeline with data transformation
- Support for single table or multi-table operations
- Comprehensive monitoring and status reporting
- Production-ready transaction management
"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, AsyncGenerator
import logging
import time
from contextlib import asynccontextmanager

from app.etl.config import ETLConfig, ExtractionConfig, ExtractionMode
from app.etl.extractors.bigquery_extractor import BigQueryExtractor
from app.etl.transformers.data_transformer import get_transformer_registry, TransformerRegistry
from app.etl.loaders.postgres_loader import PostgresLoader, LoadResult
from app.etl.watermarks import get_watermark_manager, WatermarkManager
from app.core.logging import LoggerMixin


class PipelineResult:
    """Result of a complete ETL pipeline execution"""
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.start_time = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.status = "running"
        self.tables_processed: List[str] = []
        self.tables_failed: List[str] = []
        self.load_results: Dict[str, LoadResult] = {}
        self.transformation_stats: Dict[str, Dict[str, int]] = {}
        self.total_records_processed = 0
        self.total_records_transformed = 0
        self.error_message: Optional[str] = None
        self.metadata: Dict[str, Any] = {}
    
    def mark_completed(self, status: str = "success", error: str = None):
        """Mark pipeline as completed"""
        self.end_time = datetime.now(timezone.utc)
        self.status = status
        if error:
            self.error_message = error
    
    @property
    def duration_seconds(self) -> float:
        """Get pipeline duration in seconds"""
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "pipeline_id": self.pipeline_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "tables_processed": self.tables_processed,
            "tables_failed": self.tables_failed,
            "total_records_source_processed_by_transformer": self.total_records_processed, # Records transformer attempted
            "total_records_transformed_successfully": self.total_records_transformed,   # Records output by transformer
            "detailed_transformation_stats_per_table": self.transformation_stats, # Transformer's own detailed stats
            "detailed_load_results_per_table": {
                table: {
                    "attempted_to_load": result.total_records, # Records received by loader
                    "loaded_successfully": result.inserted_records, # Records successfully upserted/inserted by loader
                    # updated_records is currently simplified in loader, might be 0
                    "updated_records_in_load": result.updated_records,
                    "skipped_by_loader": result.skipped_records, # Records loader skipped (e.g. PK null after transformation)
                    "load_duration_seconds": result.load_duration_seconds,
                    "load_status": result.status,
                    "load_error_message": result.error_message
                }
                for table, result in self.load_results.items()
            },
            "global_error_message": self.error_message,
            "metadata": self.metadata
        }


class ETLPipeline(LoggerMixin):
    """
    Production ETL Pipeline for Pulso Dashboard
    
    Orchestrates the complete Extract-Transform-Load process with:
    - Intelligent incremental processing
    - Data transformation and validation
    - Error handling and recovery
    - Comprehensive monitoring
    - Transaction consistency
    """
    
    def __init__(self):
        super().__init__()
        self.extractor: Optional[BigQueryExtractor] = None
        self.transformer: Optional[TransformerRegistry] = None
        self.loader: Optional[PostgresLoader] = None
        self.watermark_manager: Optional[WatermarkManager] = None
        
        # Pipeline tracking
        self.active_pipelines: Dict[str, PipelineResult] = {}
        self.max_concurrent_pipelines = 3
    
    async def _ensure_components(self):
        """Ensure all pipeline components are initialized"""
        if self.extractor is None:
            self.extractor = BigQueryExtractor()
        
        if self.transformer is None:
            self.transformer = get_transformer_registry()
        
        if self.loader is None:
            self.loader = PostgresLoader()
        
        if self.watermark_manager is None:
            self.watermark_manager = await get_watermark_manager()
    
    async def _transform_data_stream(
        self,
        table_name: str,
        raw_data_stream: AsyncGenerator[List[Dict[str, Any]], None]
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Transform data stream with batch processing
        
        Args:
            table_name: Name of target table
            raw_data_stream: Stream of raw data batches from BigQuery
            
        Yields:
            Transformed data batches ready for PostgreSQL
        """
        async for raw_batch in raw_data_stream:
            if not raw_batch:
                continue
            
            try:
                # Transform the batch
                transformed_batch = self.transformer.transform_table_data(table_name, raw_batch)
                
                if transformed_batch:
                    self.logger.debug(
                        f"Transformed batch for {table_name}: "
                        f"{len(raw_batch)} raw → {len(transformed_batch)} clean records"
                    )
                    yield transformed_batch
                
            except Exception as e:
                self.logger.error(f"Transformation error for {table_name}: {str(e)}")
                # Continue with next batch rather than failing entire stream
                continue
    
    async def extract_transform_and_load_table(
        self, 
        table_name: str,
        mode: ExtractionMode = ExtractionMode.INCREMENTAL,
        force: bool = False,
        pipeline_result: Optional[PipelineResult] = None
    ) -> LoadResult:
        """
        Extract, transform, and load a single table
        
        Args:
            table_name: Name of table to process
            mode: Extraction mode
            force: Force extraction even if recent
            pipeline_result: Parent pipeline result for tracking
            
        Returns:
            LoadResult with operation statistics
        """
        await self._ensure_components()
        
        config = ETLConfig.get_config(table_name)
        
        log_prefix = f"[Pipeline: {pipeline_result.pipeline_id if pipeline_result else 'N/A'}] " if pipeline_result else ""

        self.logger.info(
            f"{log_prefix}Starting ETL for table: {table_name} "
            f"(mode: {mode}, force: {force})"
        )
        
        try:
            # Step 1: Extract data stream from BigQuery
            raw_data_stream = self.extractor.extract_table_incremental(
                table_name=table_name,
                mode=mode,
                force=force
            )
            
            # Step 2: Transform data stream
            transformed_data_stream = self._transform_data_stream(
                table_name=table_name,
                raw_data_stream=raw_data_stream
            )
            
            # Step 3: Load transformed data with streaming
            load_result = await self.loader.load_data_streaming(
                table_name=table_name,
                data_stream=transformed_data_stream,
                primary_key=config.primary_key,
                upsert=True
            )
            
            # Step 4: Collect transformation statistics
            transformation_stats = self.transformer.get_transformation_stats()
            
            # Track in pipeline result
            if pipeline_result:
                pipeline_result.load_results[table_name] = load_result
                pipeline_result.transformation_stats[table_name] = transformation_stats
                # total_records_processed sums records that entered the transformation stage for this table
                pipeline_result.total_records_processed += transformation_stats.get('records_processed', 0)
                # total_records_transformed sums records successfully output by the transformer for this table
                pipeline_result.total_records_transformed += transformation_stats.get('records_transformed', 0)
                
                if load_result.status == "success" or load_result.status == "partial_success": # Consider partial success of load as table processed
                    pipeline_result.tables_processed.append(table_name)
                else:
                    pipeline_result.tables_failed.append(table_name)
            
            self.logger.info(
                f"{log_prefix}Completed ETL for table: {table_name}: "
                f"{transformation_stats.get('records_processed', 0)} from source → "
                f"{transformation_stats.get('records_transformed', 0)} transformed → "
                f"{load_result.inserted_records} loaded. Status: {load_result.status}. " # Using inserted_records from LoadResult
                f"Transformer skipped: {transformation_stats.get('records_skipped',0)}. Loader skipped: {load_result.skipped_records}."
            )
            
            return load_result
            
        except Exception as e:
            error_msg = f"{log_prefix}Failed ETL for table: {table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            # Create error result
            load_result = LoadResult(
                table_name=table_name,
                total_records=0,
                inserted_records=0,
                updated_records=0,
                skipped_records=0,
                load_duration_seconds=0,
                status="failed",
                error_message=error_msg
            )
            
            # Track in pipeline result
            if pipeline_result:
                pipeline_result.load_results[table_name] = load_result
                pipeline_result.tables_failed.append(table_name)
            
            return load_result
        
        finally:
            # Reset transformer stats for next table
            if self.transformer:
                self.transformer.transformer.reset_stats()
    
    async def run_incremental_pipeline(
        self, 
        table_names: Optional[List[str]] = None,
        mode: ExtractionMode = ExtractionMode.INCREMENTAL,
        force: bool = False,
        max_concurrent: int = 2
    ) -> PipelineResult:
        """
        Run incremental ETL pipeline for multiple tables
        
        Args:
            table_names: List of tables to process (None = all configured tables)
            mode: Extraction mode for all tables
            force: Force extraction even if recent
            max_concurrent: Maximum concurrent table processing
            
        Returns:
            PipelineResult with comprehensive statistics
        """
        pipeline_id = str(uuid.uuid4())
        result = PipelineResult(pipeline_id)
        
        # Track active pipeline
        self.active_pipelines[pipeline_id] = result
        
        try:
            # Determine tables to process
            if table_names is None:
                table_names = ETLConfig.list_tables()
            
            result.metadata = {
                "mode": mode.value,
                "force": force,
                "max_concurrent": max_concurrent,
                "total_tables": len(table_names),
                "etl_stages": ["extract", "transform", "load"]
            }
            
            self.logger.info(
                f"Starting ETL pipeline {pipeline_id} "
                f"for {len(table_names)} tables (mode: {mode})"
            )
            
            # Process tables with concurrency control
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_table(table_name: str):
                async with semaphore:
                    return await self.extract_transform_and_load_table(
                        table_name=table_name,
                        mode=mode,
                        force=force,
                        pipeline_result=result
                    )
            
            # Start all tasks
            tasks = [process_table(table) for table in table_names]
            
            # Wait for completion
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Determine final status
            if result.tables_failed:
                if result.tables_processed:
                    result.mark_completed("partial_success")
                else:
                    result.mark_completed("failed", f"All {len(result.tables_failed)} tables failed")
            else:
                result.mark_completed("success")
            
            self.logger.info(
                f"ETL Pipeline {pipeline_id} completed: "
                f"{len(result.tables_processed)} success, "
                f"{len(result.tables_failed)} failed, "
                f"{result.total_records_transformed}/{result.total_records_processed} transformed, "
                f"{result.duration_seconds:.2f}s"
            )
            
        except Exception as e:
            error_msg = f"ETL Pipeline {pipeline_id} failed: {str(e)}"
            self.logger.error(error_msg)
            result.mark_completed("failed", error_msg)
        
        finally:
            # Remove from active pipelines
            if pipeline_id in self.active_pipelines:
                del self.active_pipelines[pipeline_id]
        
        return result
    
    async def run_dashboard_refresh(self, force: bool = False) -> PipelineResult:
        """
        Run refresh specifically for dashboard tables
        
        This is the main method called by the HTTP API
        
        Args:
            force: Force refresh even if recently updated
            
        Returns:
            PipelineResult for dashboard refresh
        """
        dashboard_tables = ETLConfig.get_dashboard_tables()
        
        return await self.run_incremental_pipeline(
            table_names=dashboard_tables,
            mode=ExtractionMode.INCREMENTAL,
            force=force,
            max_concurrent=2  # Conservative for dashboard tables
        )
    
    async def run_full_refresh(
        self, 
        table_names: Optional[List[str]] = None
    ) -> PipelineResult:
        """
        Run full refresh for specified tables
        
        Args:
            table_names: Tables to refresh (None = all tables)
            
        Returns:
            PipelineResult for full refresh
        """
        return await self.run_incremental_pipeline(
            table_names=table_names,
            mode=ExtractionMode.FULL_REFRESH,
            force=True,  # Full refresh is always forced
            max_concurrent=1  # Sequential for full refresh
        )
    
    async def test_transformation(self, table_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Test transformation for a specific table with sample data
        
        Args:
            table_name: Name of table to test
            sample_size: Number of sample records to test
            
        Returns:
            Transformation test results
        """
        await self._ensure_components()
        
        try:
            # Get sample data from BigQuery
            config = ETLConfig.get_config(table_name)
            base_query = ETLConfig.get_query(table_name)
            
            # Modify query to get sample data
            sample_query = f"{base_query.replace('{incremental_filter}', '1=1')} LIMIT {sample_size}"
            
            # Test query execution
            test_result = await self.extractor.test_query(sample_query)
            
            if test_result.get("status") == "success" and test_result.get("sample_data"):
                # Test transformation
                raw_data = test_result["sample_data"]
                transformed_data = self.transformer.transform_table_data(table_name, raw_data)
                transformation_stats = self.transformer.get_transformation_stats()
                
                return {
                    "status": "success",
                    "table_name": table_name,
                    "sample_size": len(raw_data),
                    "raw_sample": raw_data[:2],  # First 2 raw records
                    "transformed_sample": transformed_data[:2],  # First 2 transformed records
                    "transformation_stats": transformation_stats,
                    "schema_mapping": {
                        "raw_fields": list(raw_data[0].keys()) if raw_data else [],
                        "transformed_fields": list(transformed_data[0].keys()) if transformed_data else []
                    }
                }
            else:
                return {
                    "status": "failed",
                    "error": "Failed to extract sample data",
                    "query_result": test_result
                }
                
        except Exception as e:
            return {
                "status": "failed",
                "table_name": table_name,
                "error": str(e)
            }
        
        finally:
            # Reset transformer stats
            if self.transformer:
                self.transformer.transformer.reset_stats()
    
    async def get_pipeline_status(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a running pipeline"""
        if pipeline_id in self.active_pipelines:
            return self.active_pipelines[pipeline_id].to_dict()
        return None
    
    async def list_active_pipelines(self) -> List[Dict[str, Any]]:
        """List all currently active pipelines"""
        return [result.to_dict() for result in self.active_pipelines.values()]
    
    async def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary of recent extractions across all tables"""
        await self._ensure_components()
        
        # Get watermark summary
        watermark_summary = await self.watermark_manager.get_extraction_summary()
        
        # Get table-specific info
        table_info = {}
        for table_name in ETLConfig.list_tables():
            try:
                info = await self.extractor.get_table_info(table_name)
                table_info[table_name] = info
            except Exception as e:
                table_info[table_name] = {"error": str(e)}
        
        return {
            "summary": watermark_summary,
            "tables": table_info,
            "active_pipelines": len(self.active_pipelines),
            "configured_tables": len(ETLConfig.list_tables()),
            "dashboard_tables": ETLConfig.get_dashboard_tables(),
            "transformation_support": self.transformer.get_supported_tables() if self.transformer else []
        }
    
    async def cleanup_and_recover(self) -> Dict[str, Any]:
        """Clean up failed extractions and attempt recovery"""
        await self._ensure_components()
        
        # Cleanup stale extractions
        cleanup_result = await self.extractor.cleanup_failed_extractions()
        
        # Get failed extractions
        failed_extractions = await self.watermark_manager.get_failed_extractions()
        
        recovery_attempts = []
        
        # Attempt to recover failed extractions
        for failed in failed_extractions:
            try:
                self.logger.info(f"Attempting recovery for {failed.table_name}")
                
                # Try incremental extraction with transformation
                load_result = await self.extract_transform_and_load_table(
                    table_name=failed.table_name,
                    mode=ExtractionMode.INCREMENTAL,
                    force=True
                )
                
                recovery_attempts.append({
                    "table_name": failed.table_name,
                    "status": load_result.status,
                    "records": load_result.total_records,
                    "error": load_result.error_message
                })
                
            except Exception as e:
                recovery_attempts.append({
                    "table_name": failed.table_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        return {
            "cleanup": cleanup_result,
            "recovery_attempts": recovery_attempts,
            "recovered_tables": [
                attempt["table_name"] 
                for attempt in recovery_attempts 
                if attempt["status"] == "success"
            ]
        }


# 🎯 Global pipeline instance
_pipeline: Optional[ETLPipeline] = None

def get_pipeline() -> ETLPipeline:
    """Get singleton pipeline instance"""
    global _pipeline
    if _pipeline is None:
        _pipeline = ETLPipeline()
    return _pipeline


# 🚀 Convenience functions for HTTP API
async def trigger_dashboard_refresh(force: bool = False) -> Dict[str, Any]:
    """Trigger dashboard refresh - called by HTTP API"""
    pipeline = get_pipeline()
    result = await pipeline.run_dashboard_refresh(force=force)
    return result.to_dict()


async def trigger_table_refresh(
    table_name: str, 
    force: bool = False
) -> Dict[str, Any]:
    """Trigger refresh for a specific table"""
    pipeline = get_pipeline()
    
    load_result = await pipeline.extract_transform_and_load_table(
        table_name=table_name,
        mode=ExtractionMode.INCREMENTAL,
        force=force
    )
    
    return {
        "table_name": table_name,
        "result": load_result.__dict__
    }


async def test_table_transformation(table_name: str) -> Dict[str, Any]:
    """Test transformation for a specific table"""
    pipeline = get_pipeline()
    return await pipeline.test_transformation(table_name)


async def get_etl_status() -> Dict[str, Any]:
    """Get comprehensive ETL status - called by HTTP API"""
    pipeline = get_pipeline()
    return await pipeline.get_extraction_summary()
