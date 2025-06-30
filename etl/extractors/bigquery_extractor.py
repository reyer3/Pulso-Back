"""
🚀 Production BigQuery Extractor - FIXED QueryJob.num_rows ISSUE
Intelligent incremental extraction with robust BigQuery row handling

FIXED: QueryJob.num_rows AttributeError - use query_job.result().total_rows instead
ADDED: Better error handling and row processing
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, AsyncGenerator

import pandas as pd
from google.auth import default
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from app.core.logging import LoggerMixin
from etl.config import ETLConfig, ExtractionMode
from etl.watermarks import get_watermark_manager, WatermarkManager


class BigQueryExtractor(LoggerMixin):
    """
    Production-ready BigQuery extractor for incremental data extraction
    
    FIXED: QueryJob.num_rows AttributeError
    """
    
    def __init__(self, project_id: str = None):
        super().__init__()
        self.project_id = project_id or ETLConfig.PROJECT_ID
        self.client: Optional[bigquery.Client] = None
        self.watermark_manager: Optional[WatermarkManager] = None
        
        # Performance settings
        self.max_retries = ETLConfig.MAX_RETRY_ATTEMPTS
        self.retry_delay = ETLConfig.RETRY_DELAY_SECONDS
        self.default_timeout = 300  # 5 minutes default query timeout
        
    async def _ensure_client(self) -> bigquery.Client:
        """Ensure BigQuery client is initialized"""
        if self.client is None:
            # Initialize with default credentials
            credentials, _ = default()
            self.client = bigquery.Client(
                project=self.project_id,
                credentials=credentials
            )
            self.logger.info(f"BigQuery client initialized for project: {self.project_id}")
        
        return self.client
    
    async def _ensure_watermark_manager(self) -> WatermarkManager:
        """Ensure watermark manager is initialized"""
        if self.watermark_manager is None:
            self.watermark_manager = await get_watermark_manager()
        return self.watermark_manager
    
    def _serialize_bigquery_row(self, row) -> Dict[str, Any]:
        """
        Safely convert BigQuery row to dictionary with proper serialization
        
        FIXED: Handles BigQuery row objects correctly
        """
        try:
            # Try different approaches to convert row to dict
            if hasattr(row, '_fields'):
                # Named tuple style (most common)
                row_dict = dict(zip(row._fields, row))
            elif hasattr(row, 'items'):
                # Dict-like object
                row_dict = dict(row.items())
            elif hasattr(row, 'keys') and hasattr(row, 'values'):
                # Row with keys/values methods
                row_dict = dict(zip(row.keys(), row.values()))
            else:
                # Fallback: try direct dict conversion
                row_dict = dict(row)
            
            # Handle datetime and other special types
            serialized_dict = {}
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    serialized_dict[key] = value.isoformat()
                elif isinstance(value, (list, tuple)):
                    # Handle arrays/repeated fields
                    serialized_dict[key] = list(value) if value else []
                elif value is None:
                    serialized_dict[key] = None
                else:
                    # Keep as is for primitives (str, int, float, bool)
                    serialized_dict[key] = value
            
            return serialized_dict
            
        except Exception as e:
            self.logger.error(f"Failed to serialize BigQuery row: {str(e)}")
            self.logger.debug(f"Row type: {type(row)}, Row attributes: {dir(row)}")
            raise ValueError(f"Cannot serialize BigQuery row: {str(e)}")
    
    async def _execute_query_streaming(
        self, 
        query: str, 
        batch_size: int = 10000
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Execute BigQuery query and yield results in batches
        
        FIXED: QueryJob.num_rows AttributeError and error handling
        """
        client = await self._ensure_client()
        
        try:
            # Configure query job
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                maximum_bytes_billed=10**10,  # 10GB limit for safety
            )
            
            # Start query job
            self.logger.info(f"Starting BigQuery job for batch size {batch_size}")
            query_job = client.query(query, job_config=job_config)
            
            # Wait for job to complete with timeout
            query_result = query_job.result(timeout=self.default_timeout)
            
            # FIXED: Get total rows from query result, not query job
            try:
                total_rows = query_result.total_rows if hasattr(query_result, 'total_rows') else 'unknown'
                self.logger.debug(f"Query job completed. Total rows: {total_rows}")
            except Exception as e:
                self.logger.debug(f"Could not get total rows count: {str(e)}")
            
            # Stream results in batches
            total_processed = 0
            batch_count = 0
            
            # Process results in pages/batches
            for page in query_result.pages:
                batch_data = []
                for row in page:
                    try:
                        # Use improved row serialization
                        row_dict = self._serialize_bigquery_row(row)
                        batch_data.append(row_dict)
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process row: {str(e)}")
                        # Log row details for debugging
                        self.logger.debug(f"Problematic row type: {type(row)}")
                        # Skip this row and continue
                        continue
                
                if batch_data:
                    total_processed += len(batch_data)
                    batch_count += 1
                    
                    self.logger.debug(f"Yielding batch {batch_count} with {len(batch_data)} rows")
                    yield batch_data
                    
                    # Respect batch size limits
                    if len(batch_data) >= batch_size:
                        break
            
            self.logger.info(f"Query completed: {total_processed} rows processed in {batch_count} batches")
            
        except GoogleCloudError as e:
            self.logger.error(f"BigQuery error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in query execution: {str(e)}")
            raise
    
    async def extract_table_incremental(
        self, 
        table_name: str,
        mode: ExtractionMode = ExtractionMode.INCREMENTAL,
        since_date: Optional[datetime] = None,
        force: bool = False
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Extract data for a specific table incrementally
        
        FIXED: Query generation compatibility
        """
        # Get configuration
        config = ETLConfig.get_config(table_name)
        
        # Generate extraction ID for tracking
        extraction_id = str(uuid.uuid4())
        
        watermark_manager = await self._ensure_watermark_manager()
        
        try:
            # Start extraction tracking
            await watermark_manager.start_extraction(table_name, extraction_id)
            
            # Determine since_date if not provided
            if since_date is None and mode == ExtractionMode.INCREMENTAL:
                last_extracted = await watermark_manager.get_last_extraction_time(table_name)
                if last_extracted and not force:
                    # Check if extraction is recent enough
                    hours_since_last = (datetime.now(timezone.utc) - last_extracted).total_seconds() / 3600
                    if hours_since_last < config.refresh_frequency_hours:
                        self.logger.info(
                            f"Skipping {table_name} - last extracted {hours_since_last:.1f}h ago "
                            f"(frequency: {config.refresh_frequency_hours}h)"
                        )
                        return
                    
                    since_date = last_extracted
            
            # FIXED: Use the correct method to get formatted query
            final_query = ETLConfig.get_query(table_name, since_date)
            
            self.logger.info(
                f"Starting extraction for {table_name} "
                f"(mode: {mode}, since: {since_date}, ID: {extraction_id})"
            )
            
            self.logger.debug(f"Executing query for {table_name}:\\n{final_query}")
            
            start_time = time.time()
            total_records = 0
            
            # Stream data in batches
            async for batch in self._execute_query_streaming(
                final_query, 
                config.batch_size
            ):
                total_records += len(batch)
                yield batch
            
            # Update watermark on success
            duration = time.time() - start_time
            await watermark_manager.update_watermark(
                table_name=table_name,
                timestamp=datetime.now(timezone.utc),
                records_extracted=total_records,
                extraction_duration_seconds=duration,
                status="success",
                extraction_id=extraction_id,
                metadata={
                    "mode": mode.value,
                    "since_date": since_date.isoformat() if since_date else None,
                    "batch_size": config.batch_size,
                    "force": force
                }
            )
            
            self.logger.info(
                f"Extraction completed for {table_name}: "
                f"{total_records} records in {duration:.2f}s"
            )
            
        except Exception as e:
            # Update watermark on failure
            await watermark_manager.update_watermark(
                table_name=table_name,
                timestamp=datetime.now(timezone.utc),
                status="failed",
                error_message=str(e),
                extraction_id=extraction_id
            )
            
            self.logger.error(f"Extraction failed for {table_name}: {str(e)}")
            raise
    
    async def extract_table_full(
        self, 
        table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Extract complete table data (for smaller tables)
        
        Returns all data in memory - use carefully!
        """
        all_data = []
        
        async for batch in self.extract_table_incremental(
            table_name=table_name,
            mode=ExtractionMode.FULL_REFRESH
        ):
            all_data.extend(batch)
        
        return all_data
    
    async def extract_multiple_tables(
        self, 
        table_names: List[str],
        mode: ExtractionMode = ExtractionMode.INCREMENTAL,
        max_concurrent: int = 3
    ) -> Dict[str, int]:
        """
        Extract multiple tables concurrently
        
        Args:
            table_names: List of table names to extract
            mode: Extraction mode for all tables  
            max_concurrent: Maximum concurrent extractions
            
        Returns:
            Dictionary with table_name -> record_count
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}
        
        async def extract_single(table_name: str) -> int:
            async with semaphore:
                total_records = 0
                try:
                    async for batch in self.extract_table_incremental(table_name, mode):
                        total_records += len(batch)
                    return total_records
                except Exception as e:
                    self.logger.error(f"Failed to extract {table_name}: {str(e)}")
                    return -1  # Indicate failure
        
        # Start all extractions
        tasks = []
        for table_name in table_names:
            task = asyncio.create_task(extract_single(table_name))
            tasks.append((table_name, task))
        
        # Wait for completion
        for table_name, task in tasks:
            try:
                record_count = await task
                results[table_name] = record_count
                self.logger.info(f"Completed {table_name}: {record_count} records")
            except Exception as e:
                results[table_name] = -1
                self.logger.error(f"Failed {table_name}: {str(e)}")
        
        return results

    # En la clase BigQueryExtractor, añádelo después de extract_multiple_tables

    async def run_custom_query_to_list(self, query: str) -> list[dict]:
        """
        Executes an arbitrary SQL query and returns all results in a single list.

        NOTE: Use with caution, as this loads all results into memory.
        Ideal for the CalendarDrivenCoordinator which processes one campaign at a time.

        Args:
            query: The SQL query string to execute.

        Returns:
            A list of dictionaries containing all rows from the query.
        """
        self.logger.info(f"Running custom query and collecting all results into memory...")

        all_results = []
        # Usamos el eficiente método de streaming interno para obtener los datos
        async for batch in self._execute_query_streaming(query):
            all_results.extend(batch)

        self.logger.info(f"Custom query collected {len(all_results)} records.")
        return all_results

        # En: app/etl/extractors/bigquery_extractor.py
        # Dentro de la clase BigQueryExtractor

    async def stream_custom_query(
            self,
            query: str,
            batch_size: int = 10000
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Executes an arbitrary SQL query and yields results in batches.
        This is the preferred memory-efficient method for large queries.

        Args:
            query: The SQL query string to execute.
            batch_size: The number of rows to fetch per batch.

        Yields:
            A batch of data as a list of dictionaries.
        """
        self.logger.info(f"Streaming custom query results with batch size {batch_size}...")
        self.logger.debug(f"Query Snippet: {query[:500]}...")

        # This method simply acts as a public entrypoint to the
        # internal streaming logic that is already well-implemented.
        async for batch in self._execute_query_streaming(query, batch_size):
            yield batch
    
    async def test_query(self, query: str) -> Dict[str, Any]:
        """
        Test a query without full execution (for validation)
        
        Returns query metadata and sample results
        """
        client = await self._ensure_client()
        
        try:
            # Add LIMIT to avoid large results in test
            test_query = f"SELECT * FROM ({query}) LIMIT 10"
            
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                dry_run=False  # We want actual results for testing
            )
            
            start_time = time.time()
            query_job = client.query(test_query, job_config=job_config)
            results = query_job.result(timeout=30)  # Short timeout for test
            
            # Get sample data using improved serialization
            sample_data = []
            for row in results:
                try:
                    serialized_row = self._serialize_bigquery_row(row)
                    sample_data.append(serialized_row)
                except Exception as e:
                    self.logger.warning(f"Failed to serialize sample row: {str(e)}")
            
            return {
                "status": "success",
                "execution_time_seconds": time.time() - start_time,
                "sample_records": len(sample_data),
                "schema": [
                    {"name": field.name, "type": field.field_type} 
                    for field in query_job.schema
                ],
                "sample_data": sample_data[:3],  # First 3 rows
                "total_bytes_processed": query_job.total_bytes_processed,
                "job_id": query_job.job_id
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "query": test_query
            }
    
    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get metadata about a configured table"""
        try:
            config = ETLConfig.get_config(table_name)
            watermark_manager = await self._ensure_watermark_manager()
            watermark = await watermark_manager.get_watermark(table_name)
            
            return {
                "table_name": table_name,
                "table_type": config.table_type.value,
                "description": config.description,
                "primary_key": config.primary_key,
                "incremental_column": config.incremental_column,
                "lookback_days": config.lookback_days,
                "batch_size": config.batch_size,
                "refresh_frequency_hours": config.refresh_frequency_hours,
                "last_extraction": {
                    "timestamp": watermark.last_extracted_at.isoformat() if watermark else None,
                    "status": watermark.last_extraction_status if watermark else "never",
                    "records": watermark.records_extracted if watermark else 0,
                    "duration": watermark.extraction_duration_seconds if watermark else 0
                } if watermark else None
            }
            
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e)
            }
    
    # 🎯 Convenience functions for easy imports
async def get_extractor() -> BigQueryExtractor:
    """Get configured BigQuery extractor instance"""
    return BigQueryExtractor()


async def quick_extract(table_name: str) -> List[Dict[str, Any]]:
    """Quick extraction for small tables (returns all data)"""
    extractor = await get_extractor()
    return await extractor.extract_table_full(table_name)


async def extract_dashboard_tables() -> Dict[str, int]:
    """Extract all dashboard-related tables"""
    extractor = await get_extractor()
    dashboard_tables = ETLConfig.get_dashboard_tables()
    
    return await extractor.extract_multiple_tables(
        table_names=dashboard_tables,
        mode=ExtractionMode.INCREMENTAL,
        max_concurrent=2  # Conservative for dashboard tables
    )
