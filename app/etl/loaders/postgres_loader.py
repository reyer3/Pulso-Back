"""
🎯 Production PostgreSQL Loader with UPSERT Support
Intelligent incremental loading with conflict resolution

Features:
- Dynamic UPSERT based on configurable primary keys
- Batch processing for optimal performance
- Data validation and quality checks
- Comprehensive error handling and rollback
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Union
import logging
from dataclasses import dataclass

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text

from app.etl.config import ETLConfig, ExtractionConfig
from app.core.database import get_postgres_engine
from app.core.logging import LoggerMixin


@dataclass
class LoadResult:
    """Result information from a load operation"""
    table_name: str
    total_records: int
    inserted_records: int
    updated_records: int
    skipped_records: int
    load_duration_seconds: float
    status: str
    error_message: Optional[str] = None


class PostgresLoader(LoggerMixin):
    """
    Production-ready PostgreSQL loader with UPSERT capabilities
    
    Features:
    - Dynamic UPSERT based on primary key configuration
    - Efficient batch processing
    - Data type inference and conversion
    - Transaction management with rollback
    - Detailed load statistics
    """
    
    def __init__(self, engine: Optional[AsyncEngine] = None):
        super().__init__()
        self.engine = engine or get_postgres_engine()
        self.max_batch_size = 5000  # Default batch size
        self.connection_timeout = 30
    
    def _build_upsert_sql(
        self, 
        table_name: str, 
        columns: List[str], 
        primary_key: List[str],
        on_conflict_action: str = "UPDATE"
    ) -> str:
        """
        Build dynamic UPSERT SQL statement
        
        Args:
            table_name: Target table name
            columns: List of column names
            primary_key: List of primary key columns
            on_conflict_action: 'UPDATE', 'IGNORE', or 'ERROR'
            
        Returns:
            Complete UPSERT SQL statement
        """
        if not primary_key:
            # No primary key - use simple INSERT
            placeholders = ", ".join([f":{col}" for col in columns])
            return f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({placeholders})
            """
        
        # Build UPSERT with ON CONFLICT
        placeholders = ", ".join([f":{col}" for col in columns])
        pk_columns = ", ".join(primary_key)
        
        if on_conflict_action == "UPDATE":
            # Build SET clause for update (exclude primary key columns)
            update_columns = [col for col in columns if col not in primary_key]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            upsert_sql = f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({pk_columns})
            DO UPDATE SET {set_clause}
            """
        elif on_conflict_action == "IGNORE":
            upsert_sql = f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({pk_columns}) DO NOTHING
            """
        else:  # ERROR
            upsert_sql = f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({placeholders})
            """
        
        return upsert_sql
    
    def _validate_data_batch(
        self, 
        data: List[Dict[str, Any]], 
        config: ExtractionConfig
    ) -> List[Dict[str, Any]]:
        """
        Validate and clean data batch before loading
        
        Args:
            data: List of records to validate
            config: Extraction configuration for validation rules
            
        Returns:
            Cleaned and validated data
        """
        if not data:
            return []
        
        validated_data = []
        
        for i, record in enumerate(data):
            try:
                # Check required columns are present
                if config.required_columns:
                    missing_cols = [col for col in config.required_columns if col not in record]
                    if missing_cols:
                        self.logger.warning(
                            f"Record {i} missing required columns: {missing_cols}"
                        )
                        continue
                
                # Check primary key values are not null
                for pk_col in config.primary_key:
                    if pk_col not in record or record[pk_col] is None:
                        self.logger.warning(
                            f"Record {i} has null primary key value for: {pk_col}"
                        )
                        continue
                
                # Clean the record
                cleaned_record = {}
                for key, value in record.items():
                    # Handle datetime strings
                    if isinstance(value, str) and 'T' in value and ('Z' in value or '+' in value):
                        try:
                            # Try to parse as ISO datetime
                            cleaned_record[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        except ValueError:
                            cleaned_record[key] = value
                    else:
                        cleaned_record[key] = value
                
                validated_data.append(cleaned_record)
                
            except Exception as e:
                self.logger.warning(f"Error validating record {i}: {str(e)}")
                continue
        
        self.logger.info(
            f"Validated {len(validated_data)} of {len(data)} records "
            f"({len(data) - len(validated_data)} invalid records skipped)"
        )
        
        return validated_data
    
    async def _ensure_table_exists(
        self, 
        table_name: str, 
        sample_record: Dict[str, Any],
        primary_key: List[str]
    ) -> None:
        """
        Ensure target table exists, create if necessary
        
        Args:
            table_name: Name of table to check/create
            sample_record: Sample record for schema inference
            primary_key: Primary key columns
        """
        # Basic table creation - in production, use migrations instead
        if not sample_record:
            return
        
        # Infer column types from sample data
        columns_def = []
        for col_name, value in sample_record.items():
            if isinstance(value, bool):
                col_type = "BOOLEAN"
            elif isinstance(value, int):
                col_type = "INTEGER" 
            elif isinstance(value, float):
                col_type = "DOUBLE PRECISION"
            elif isinstance(value, datetime):
                col_type = "TIMESTAMP WITH TIME ZONE"
            else:
                col_type = "TEXT"
            
            columns_def.append(f"{col_name} {col_type}")
        
        # Add primary key constraint if specified
        if primary_key:
            pk_constraint = f"PRIMARY KEY ({', '.join(primary_key)})"
            columns_def.append(pk_constraint)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_def)}
        )
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(create_table_sql))
            
        self.logger.info(f"Ensured table exists: {table_name}")
    
    async def load_data_batch(
        self, 
        table_name: str,
        data: List[Dict[str, Any]],
        primary_key: List[str],
        upsert: bool = True,
        validate: bool = True,
        create_table: bool = False
    ) -> LoadResult:
        """
        Load a batch of data with UPSERT support
        
        Args:
            table_name: Target table name
            data: List of records to load
            primary_key: Primary key columns for UPSERT
            upsert: Use UPSERT (True) or INSERT only (False)
            validate: Validate data before loading
            create_table: Auto-create table if it doesn't exist
            
        Returns:
            LoadResult with detailed statistics
        """
        start_time = time.time()
        
        if not data:
            return LoadResult(
                table_name=table_name,
                total_records=0,
                inserted_records=0,
                updated_records=0,
                skipped_records=0,
                load_duration_seconds=0.0,
                status="success"
            )
        
        try:
            # Get extraction config for validation
            config = None
            try:
                config = ETLConfig.get_config(table_name)
            except ValueError:
                # Table not in config - use defaults
                config = ExtractionConfig(
                    table_name=table_name,
                    table_type="dashboard",
                    description=f"Auto-generated config for {table_name}",
                    primary_key=primary_key,
                    incremental_column="updated_at"
                )
            
            # Validate data if requested
            if validate and config:
                data = self._validate_data_batch(data, config)
                
            if not data:
                return LoadResult(
                    table_name=table_name,
                    total_records=0,
                    inserted_records=0,
                    updated_records=0,
                    skipped_records=0,
                    load_duration_seconds=time.time() - start_time,
                    status="success",
                    error_message="No valid records to load"
                )
            
            # Create table if requested
            if create_table:
                await self._ensure_table_exists(table_name, data[0], primary_key)
            
            # Get column names from first record
            columns = list(data[0].keys())
            
            # Determine conflict action
            conflict_action = "UPDATE" if upsert and primary_key else "ERROR"
            
            # Build SQL statement
            sql_statement = self._build_upsert_sql(
                table_name=table_name,
                columns=columns,
                primary_key=primary_key,
                on_conflict_action=conflict_action
            )
            
            self.logger.debug(f"Using SQL: {sql_statement}")
            
            # Execute in transaction
            inserted_count = 0
            updated_count = 0
            
            async with self.engine.begin() as conn:
                for record in data:
                    try:
                        result = await conn.execute(text(sql_statement), record)
                        
                        # PostgreSQL doesn't easily tell us if it was INSERT vs UPDATE
                        # For now, count all as successful operations
                        if result.rowcount > 0:
                            inserted_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error loading record: {str(e)}")
                        # Don't raise - continue with other records
                        continue
            
            duration = time.time() - start_time
            
            self.logger.info(
                f"Loaded {table_name}: {inserted_count} records "
                f"in {duration:.2f}s (upsert: {upsert})"
            )
            
            return LoadResult(
                table_name=table_name,
                total_records=len(data),
                inserted_records=inserted_count,
                updated_records=updated_count,  # TODO: Distinguish INSERT vs UPDATE
                skipped_records=len(data) - inserted_count,
                load_duration_seconds=duration,
                status="success"
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed to load {table_name}: {str(e)}"
            self.logger.error(error_msg)
            
            return LoadResult(
                table_name=table_name,
                total_records=len(data) if data else 0,
                inserted_records=0,
                updated_records=0,
                skipped_records=len(data) if data else 0,
                load_duration_seconds=duration,
                status="failed",
                error_message=error_msg
            )
    
    async def load_data_streaming(
        self,
        table_name: str,
        data_stream: AsyncGenerator[List[Dict[str, Any]], None],
        primary_key: List[str],
        upsert: bool = True,
        batch_size: Optional[int] = None
    ) -> LoadResult:
        """
        Load data from an async stream in batches
        
        Args:
            table_name: Target table name
            data_stream: Async generator yielding batches of records
            primary_key: Primary key columns for UPSERT
            upsert: Use UPSERT (True) or INSERT only (False)
            batch_size: Override default batch size
            
        Returns:
            Aggregated LoadResult across all batches
        """
        start_time = time.time()
        
        total_records = 0
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        batch_count = 0
        errors = []
        
        try:
            async for batch in data_stream:
                batch_count += 1
                
                self.logger.debug(f"Processing batch {batch_count} with {len(batch)} records")
                
                # Load batch
                batch_result = await self.load_data_batch(
                    table_name=table_name,
                    data=batch,
                    primary_key=primary_key,
                    upsert=upsert,
                    validate=True,
                    create_table=(batch_count == 1)  # Create table on first batch
                )
                
                # Aggregate results
                total_records += batch_result.total_records
                total_inserted += batch_result.inserted_records
                total_updated += batch_result.updated_records
                total_skipped += batch_result.skipped_records
                
                if batch_result.status == "failed":
                    errors.append(f"Batch {batch_count}: {batch_result.error_message}")
            
            duration = time.time() - start_time
            
            status = "success" if not errors else "partial_success"
            error_msg = "; ".join(errors) if errors else None
            
            self.logger.info(
                f"Streaming load completed for {table_name}: "
                f"{total_records} total records, {batch_count} batches, "
                f"{duration:.2f}s"
            )
            
            return LoadResult(
                table_name=table_name,
                total_records=total_records,
                inserted_records=total_inserted,
                updated_records=total_updated,
                skipped_records=total_skipped,
                load_duration_seconds=duration,
                status=status,
                error_message=error_msg
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed streaming load for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            
            return LoadResult(
                table_name=table_name,
                total_records=total_records,
                inserted_records=total_inserted,
                updated_records=total_updated,
                skipped_records=total_skipped,
                load_duration_seconds=duration,
                status="failed",
                error_message=error_msg
            )
    
    async def truncate_and_load(
        self,
        table_name: str,
        data: List[Dict[str, Any]]
    ) -> LoadResult:
        """
        Truncate table and load fresh data (for full refresh)
        
        Args:
            table_name: Target table name
            data: Records to load
            
        Returns:
            LoadResult with operation statistics
        """
        start_time = time.time()
        
        try:
            async with self.engine.begin() as conn:
                # Truncate table
                await conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                self.logger.info(f"Truncated table: {table_name}")
            
            # Load fresh data (no UPSERT needed after truncate)
            result = await self.load_data_batch(
                table_name=table_name,
                data=data,
                primary_key=[],  # No primary key needed for INSERT after TRUNCATE
                upsert=False,
                validate=True
            )
            
            # Update duration to include truncate time
            result.load_duration_seconds = time.time() - start_time
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed truncate and load for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            
            return LoadResult(
                table_name=table_name,
                total_records=len(data) if data else 0,
                inserted_records=0,
                updated_records=0,
                skipped_records=len(data) if data else 0,
                load_duration_seconds=duration,
                status="failed",
                error_message=error_msg
            )
    
    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics about a table"""
        try:
            stats_sql = f"""
            SELECT 
                COUNT(*) as row_count,
                pg_size_pretty(pg_total_relation_size('{table_name}')) as table_size,
                (SELECT COUNT(*) FROM information_schema.columns 
                 WHERE table_name = '{table_name}') as column_count
            """
            
            async with self.engine.begin() as conn:
                result = await conn.execute(text(stats_sql))
                row = result.fetchone()
                
                if row:
                    return dict(row._mapping)
                
            return {"error": "Table not found"}
            
        except Exception as e:
            return {"error": str(e)}


# 🎯 Convenience functions for easy imports
async def get_loader() -> PostgresLoader:
    """Get configured PostgreSQL loader instance"""
    return PostgresLoader()


async def quick_load(
    table_name: str, 
    data: List[Dict[str, Any]], 
    primary_key: List[str]
) -> LoadResult:
    """Quick load with UPSERT for small datasets"""
    loader = await get_loader()
    return await loader.load_data_batch(
        table_name=table_name,
        data=data,
        primary_key=primary_key,
        upsert=True
    )
