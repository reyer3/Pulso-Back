"""
🚀 Production BigQuery Extractor
Intelligent incremental extraction with automatic optimization

Features:
- Incremental extraction with watermark management
- Automatic query optimization and batching  
- Robust error handling and retry logic
- Memory-efficient streaming for large datasets
"""

import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, AsyncGenerator, Union
import logging
import time

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.auth import default
import pandas as pd

from app.etl.config import ETLConfig, ExtractionConfig, ExtractionMode
from app.etl.watermarks import get_watermark_manager, WatermarkManager
from app.core.logging import LoggerMixin


class BigQueryExtractor(LoggerMixin):
    """
    Production-ready BigQuery extractor for incremental data extraction
    
    Supports:
    - Multiple extraction modes (incremental, full, sliding window)
    - Automatic watermark management
    - Query optimization and result streaming
    - Comprehensive error handling
    """
    
    def __init__(self, project_id: str = None):
        super().__init__()
        self.project_id = project_id or ETLConfig.PROJECT_ID
        self.client: Optional[bigquery.Client] = None
        self.watermark_manager: Optional[WatermarkManager] = None
        
        # Performance settings
        self.max_retries: int = ETLConfig.MAX_RETRY_ATTEMPTS
        self.retry_delay_seconds: int = ETLConfig.RETRY_DELAY_SECONDS
        self.default_query_timeout_seconds: int = 300  # 5 minutes default query timeout
        
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
    
    def _build_incremental_query(
        self, 
        base_query: str, 
        config: ExtractionConfig,
        mode: ExtractionMode = ExtractionMode.INCREMENTAL,
        since_date: Optional[datetime] = None
    ) -> tuple[str, Optional[List[bigquery.ScalarQueryParameter]]]:
        """
        Build query with incremental filter and prepares query parameters.
        
        Args:
            base_query: Base SQL query with {incremental_filter} placeholder.
            config: Extraction configuration.
            mode: Extraction mode.
            since_date: Extract data since this date (if None, uses watermark).
            
        Returns:
            A tuple containing the final SQL query string and a list of ScalarQueryParameter objects.
        """
        query_params: Optional[List[bigquery.ScalarQueryParameter]] = None

        if mode == ExtractionMode.FULL_REFRESH:
            incremental_filter_condition = "1=1"
        else:
            # Determine the actual since_date if not provided or if sliding window
            if mode == ExtractionMode.SLIDING_WINDOW:
                # For sliding window, since_date is effectively NOW, lookback is applied from NOW.
                # The get_incremental_filter handles the date calculation for sliding window correctly.
                # We pass datetime.now() so it calculates lookback from today.
                filter_since_date = datetime.now(timezone.utc)
            elif since_date is None:
                # Default for INCREMENTAL: use lookback from config for data quality from today
                filter_since_date = datetime.now(timezone.utc) - timedelta(days=config.lookback_days)
            else:
                filter_since_date = since_date

            # Get filter condition template and parameters
            condition_template, params_dict = ETLConfig.get_incremental_filter(
                config.table_name,
                filter_since_date, # Pass the determined since_date for filter calculation
                mode=mode # Pass mode to get_incremental_filter
            )
            incremental_filter_condition = condition_template

            if params_dict:
                query_params = []
                for name, value in params_dict.items():
                    # Determine BigQuery type. Assuming DATE for now.
                    # This might need to be more sophisticated if other types are used.
                    param_type = "DATE"
                    # Convert Python date/datetime to string if BQ expects date string,
                    # or pass directly if BQ client handles it. BQ client handles Python date/datetime.
                    if isinstance(value, str): # If it's already a 'YYYY-MM-DD' string
                        param_value = datetime.strptime(value, '%Y-%m-%d').date()
                    elif isinstance(value, datetime):
                        param_value = value.date()
                    else: # Assumes value is already a date object
                        param_value = value
                    query_params.append(bigquery.ScalarQueryParameter(name, param_type, param_value))
        
        # Replace placeholder in query
        final_query = base_query.replace("{incremental_filter}", incremental_filter_condition)
        
        self.logger.debug(f"Generated query for {config.table_name}: {final_query}, Params: {params_dict if mode != ExtractionMode.FULL_REFRESH else 'N/A'}")
        return final_query, query_params
    
    async def _execute_query_streaming(
        self, 
        query: str, 
        batch_size: int = 10000,
        query_params: Optional[List[bigquery.ScalarQueryParameter]] = None
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Execute BigQuery query and yield results in batches
        
        Memory-efficient streaming approach for large datasets
        Includes retry logic for transient errors.
        """
        client = await self._ensure_client()
        
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            maximum_bytes_billed=10**10,  # 10GB limit for safety
        )
        if query_params:
            job_config.query_parameters = query_params

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                self.logger.info(
                    f"Attempt {attempt + 1}/{self.max_retries} - Starting BigQuery job. "
                    f"Query: {query[:200]}... "
                    f"Params: {[(p.name, p.value) for p in query_params or []]}"
                )
                query_job = client.query(query, job_config=job_config)
                self.logger.info(f"BigQuery job initiated with ID: {query_job.job_id} (Attempt {attempt + 1})")

                # Wait for job to complete with timeout
                # result() can raise google.api_core.exceptions.GoogleAPICallError for job errors
                iterator = query_job.result(timeout=self.default_query_timeout_seconds)
                
                # Stream results in batches
                total_rows = 0
                batch_count = 0

                # query_job.result() returns an iterator directly if successful
                for page in iterator.pages: # Iterate through pages for batching
                    batch_data = []
                    for row in page:
                        row_dict = dict(row)
                        for key, value in row_dict.items():
                            if isinstance(value, datetime):
                                row_dict[key] = value.isoformat()
                        batch_data.append(row_dict)
                    
                    if batch_data:
                        total_rows += len(batch_data)
                        batch_count += 1
                        self.logger.debug(f"Yielding batch {batch_count} with {len(batch_data)} rows from job {query_job.job_id}")
                        yield batch_data

                self.logger.info(f"Query job {query_job.job_id} completed successfully: {total_rows} rows in {batch_count} batches")
                return # Successful execution, exit retry loop

            except (GoogleCloudError, asyncio.TimeoutError) as e: # Catch more specific transient errors if possible
                last_exception = e
                self.logger.warning(
                    f"BigQuery job {getattr(query_job, 'job_id', 'N/A')} "
                    f"failed on attempt {attempt + 1}/{self.max_retries}: {str(e)}"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay_seconds * (2**attempt))  # Exponential backoff
                else:
                    self.logger.error(f"BigQuery job {getattr(query_job, 'job_id', 'N/A')} failed after {self.max_retries} attempts.")
                    raise  # Re-raise the last exception after all retries fail
            except Exception as e: # Catch other unexpected errors
                self.logger.error(f"Unexpected error in query execution (job ID: {getattr(query_job, 'job_id', 'N/A')}): {str(e)}")
                raise # Re-raise immediately for non-transient errors

        # This part should ideally not be reached if logic is correct, but as a safeguard:
        if last_exception:
            raise last_exception

    async def extract_table_incremental(
        self, 
        table_name: str,
        mode: ExtractionMode = ExtractionMode.INCREMENTAL,
        since_date: Optional[datetime] = None,
        force: bool = False
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Extract data for a specific table incrementally
        
        Args:
            table_name: Name of table to extract
            mode: Extraction mode
            since_date: Override since date (optional)
            force: Force extraction even if recent
            
        Yields:
            Batches of records as list of dictionaries
        """
        # Get configuration
        config = ETLConfig.get_config(table_name)
        base_query = ETLConfig.get_query(table_name)
        
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
            
            # Build final query and parameters
            final_query, query_params = self._build_incremental_query(
                base_query, config, mode, since_date
            )
            
            self.logger.info(
                f"Starting extraction for {table_name} "
                f"(mode: {mode}, since: {since_date}, ID: {extraction_id})"
            )
            
            start_time = time.time()
            total_records = 0
            
            # Stream data in batches
            async for batch in self._execute_query_streaming(
                query=final_query,
                batch_size=config.batch_size,
                query_params=query_params
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
            
            # Get sample data
            sample_data = [dict(row) for row in results]
            
            # Serialize datetime objects
            for row in sample_data:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
            
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
    
    async def cleanup_failed_extractions(self) -> Dict[str, Any]:
        """Clean up stale/failed extractions"""
        watermark_manager = await self._ensure_watermark_manager()
        
        # Clean up stale running extractions
        stale_count = await watermark_manager.cleanup_stale_extractions(
            timeout_minutes=30
        )
        
        # Get current status
        failed_extractions = await watermark_manager.get_failed_extractions()
        running_extractions = await watermark_manager.get_running_extractions()
        
        return {
            "stale_extractions_cleaned": stale_count,
            "current_failed_count": len(failed_extractions),
            "current_running_count": len(running_extractions),
            "failed_tables": [ext.table_name for ext in failed_extractions],
            "running_tables": [ext.table_name for ext in running_extractions]
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
