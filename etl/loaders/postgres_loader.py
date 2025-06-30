"""
🎯 High-Performance PostgreSQL Loader with pure asyncpg - STREAMING FIX
Efficient, asynchronous data loading for ETL pipelines.

Features:
- Pure asyncpg for maximum performance and low overhead
- Dynamic UPSERT statements with `ON CONFLICT DO UPDATE`
- Asynchronous streaming and batch processing
- Data validation and sanitization
- Detailed load statistics and error reporting

CRITICAL STREAMING FIX: Replaced individual record processing (23k+ queries) 
with true batch processing using executemany() for efficient streaming loads
"""

import time
from datetime import datetime
from typing import List, Dict, Any, Optional, AsyncGenerator
from dataclasses import dataclass
import logging

from app.database.connection import get_database_manager, DatabaseManager
from app.core.logging import LoggerMixin
# Added imports for ETLConfig and TableType
from etl.config import ETLConfig, TableType

logger = logging.getLogger(__name__)


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
    A high-performance data loader for PostgreSQL using asyncpg.
    STREAMING FIX: True batch processing for efficient streaming pipeline
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        super().__init__()
        self.db_manager = db_manager
        self.max_batch_size = 1000
        self.connection_timeout = 30

    async def _get_db_manager(self) -> DatabaseManager:
        if self.db_manager is None:
            self.db_manager = await get_database_manager()
        return self.db_manager

    def _validate_and_sanitize_batch(
        self,
        data: List[Dict[str, Any]],
        primary_key: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Validates and sanitizes a batch of data before loading.
        FIXED: Removed automatic fecha_procesamiento addition that caused errors
        """
        if not data:
            return []

        validated_data = []
        for i, record in enumerate(data):
            # Check for null primary key values
            if any(record.get(pk) is None for pk in primary_key):
                self.logger.warning(f"Record {i} has a null primary key value. Skipping.")
                continue

            # Sanitize and format data - FIXED: Only sanitize existing data
            sanitized_record = {}
            for key, value in record.items():
                if isinstance(value, str) and 'T' in value and ('Z'in value or '+' in value):
                    try:
                        sanitized_record[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except ValueError:
                        sanitized_record[key] = value
                else:
                    sanitized_record[key] = value

            validated_data.append(sanitized_record)

        return validated_data

    async def load_data_batch(
        self,
        table_name: str, # This will now be the base table name, e.g., "calendario"
        table_type: TableType, # Added to determine schema
        data: List[Dict[str, Any]],
        primary_key: List[str],
        upsert: bool = True,
        validate: bool = True,
        # Optional parameter for special cases like test tables not in ETLConfig
        # If fq_table_name is provided, table_type is ignored for FQN construction.
        fq_table_name_override: Optional[str] = None
    ) -> LoadResult:
        """
        🚀 STREAMING FIX: Loads a batch using TRUE batch processing with executemany()
        
        BEFORE: 23,333 individual queries causing timeout/hanging
        AFTER: Single executemany() operation for maximum streaming efficiency
        """
        start_time = time.time()

        if not data:
            return LoadResult(
                table_name=table_name, # table_name here is base_name, consider if FQN should be in LoadResult
                total_records=0,
                inserted_records=0,
                updated_records=0,
                skipped_records=0,
                load_duration_seconds=0.0,
                status="success"
            )

        if validate:
            validated_data = self._validate_and_sanitize_batch(data, primary_key)
            skipped_count = len(data) - len(validated_data)
            data = validated_data
        else:
            skipped_count = 0

        # Determine the fully qualified table name
        if fq_table_name_override:
            fq_table_name = fq_table_name_override
        else:
            fq_table_name = ETLConfig.get_fq_table_name(table_name, table_type)


        if not data:
            return LoadResult(
                table_name=fq_table_name, # Using FQN in result
                total_records=skipped_count,
                inserted_records=0,
                updated_records=0,
                skipped_records=skipped_count,
                load_duration_seconds=time.time() - start_time,
                status="success",
                error_message="No valid records to load after validation."
            )

        db = await self._get_db_manager()
        pool = await db.get_pool()

        async with pool.acquire() as conn:
            try:
                # 🚀 STREAMING FIX: Build query once for batch processing
                columns = list(data[0].keys())
                columns_str = ", ".join(f'"{c}"' for c in columns)
                placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
                pk_str = ", ".join(f'"{pk}"' for pk in primary_key)
                
                update_columns = [col for col in columns if col not in primary_key]
                update_str = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)

                if upsert and update_columns:
                    # Use fq_table_name in the query
                    query = f"""
                        INSERT INTO {fq_table_name} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT ({pk_str}) DO UPDATE SET {update_str}
                    """
                else:
                    # Use fq_table_name in the query
                    query = f"""
                        INSERT INTO {fq_table_name} ({columns_str})
                        VALUES ({placeholders})
                    """

                # 🚀 STREAMING FIX: Prepare all values for executemany()
                batch_values = []
                for record in data:
                    values = [record.get(col) for col in columns]
                    batch_values.append(values)

                # 🚀 STREAMING FIX: Single executemany() call instead of 23k individual queries
                self.logger.debug(f"Executing batch INSERT for {len(batch_values)} records into {table_name}")
                
                await conn.executemany(query, batch_values)
                
                inserted_count = len(batch_values)
                duration = time.time() - start_time
                
                self.logger.info(f"✅ Loaded {inserted_count} records for {fq_table_name}") # Log FQN

                return LoadResult(
                    table_name=fq_table_name, # Use FQN in result
                    total_records=len(data) + skipped_count,
                    inserted_records=inserted_count,
                    updated_records=0,  # Simplified for performance
                    skipped_records=skipped_count,
                    load_duration_seconds=duration,
                    status="success"
                )

            except Exception as e:
                duration = time.time() - start_time
                error_msg = f"Failed to load batch into {fq_table_name}: {e}" # Log FQN
                self.logger.error(error_msg)
                self.logger.debug(f"Failed batch size: {len(data)} records")
                
                return LoadResult(
                    table_name=fq_table_name, # Use FQN in result
                    total_records=len(data) + skipped_count,
                    inserted_records=0,
                    updated_records=0,
                    skipped_records=len(data) + skipped_count,
                    load_duration_seconds=duration,
                    status="failed",
                    error_message=error_msg
                )

    async def load_data_streaming(
        self,
        table_name: str, # This will now be the base table name
        table_type: TableType, # Added to determine schema
        data_stream: AsyncGenerator[List[Dict[str, Any]], None],
        primary_key: List[str],
        upsert: bool = True,
        # Optional parameter for special cases like test tables not in ETLConfig
        fq_table_name_override: Optional[str] = None
    ) -> LoadResult:
        """
        🚀 STREAMING FIX: Loads data from an async stream in efficient batches
        
        This is the method called by CalendarDrivenCoordinator._load_campaign_table()
        """
        start_time = time.time()
        total_records, total_inserted, total_skipped = 0, 0, 0
        errors = []

        # Determine the fully qualified table name once
        if fq_table_name_override:
            fq_table_name = fq_table_name_override
        else:
            fq_table_name = ETLConfig.get_fq_table_name(table_name, table_type)

        self.logger.debug(f"Starting streaming load for {fq_table_name}") # Log FQN

        try:
            async for batch in data_stream:
                if not batch:
                    continue

                # 🚀 STREAMING FIX: Each batch now uses efficient executemany()
                # Pass base table_name and table_type, or the override
                batch_result = await self.load_data_batch(
                    table_name=table_name, # base name
                    table_type=table_type, # type
                    data=batch,
                    primary_key=primary_key,
                    upsert=upsert,
                    fq_table_name_override=fq_table_name_override # Pass override if present
                )

                total_records += batch_result.total_records
                total_inserted += batch_result.inserted_records
                total_skipped += batch_result.skipped_records

                if batch_result.status == "failed":
                    errors.append(batch_result.error_message)
                    self.logger.warning(f"Batch failed for {table_name}: {batch_result.error_message}")

            duration = time.time() - start_time
            status = "success" if not errors else ("partial_success" if total_inserted > 0 else "failed")
            error_message = "; ".join(errors) if errors else None

            # 🎯 This is what gets returned to _load_campaign_table()
            final_result = LoadResult(
                table_name=fq_table_name, # Use FQN in result
                total_records=total_records,
                inserted_records=total_inserted,
                updated_records=0,
                skipped_records=total_skipped,
                load_duration_seconds=duration,
                status=status,
                error_message=error_message
            )

            if status == "success":
                self.logger.info(f"🎯 Streaming load completed for {table_name}: {total_inserted} records loaded")
            else:
                self.logger.error(f"❌ Streaming load failed for {table_name}: {error_message}")

            return final_result

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Streaming load failed for {table_name}: {e}"
            self.logger.error(error_msg, exc_info=True)
            
            return LoadResult(
                table_name=fq_table_name, # Use FQN in result
                total_records=total_records,
                inserted_records=total_inserted,
                updated_records=0,
                skipped_records=total_records - total_inserted,
                load_duration_seconds=duration,
                status="failed",
                error_message=error_msg
            )

    async def truncate_and_load(
        self,
        table_name: str, # Base table name
        table_type: TableType, # Added
        data: List[Dict[str, Any]],
        primary_key: List[str],
        fq_table_name_override: Optional[str] = None
    ) -> LoadResult:
        """
        Truncates a table and then loads new data.
        """
        start_time = time.time()
        db = await self._get_db_manager()

        if fq_table_name_override:
            fq_table_name = fq_table_name_override
        else:
            fq_table_name = ETLConfig.get_fq_table_name(table_name, table_type)

        try:
            await db.execute_query(f"TRUNCATE TABLE {fq_table_name} RESTART IDENTITY")
            self.logger.info(f"Truncated table: {fq_table_name}")

            load_result = await self.load_data_batch(
                table_name=table_name, # base name
                table_type=table_type, # type
                data=data,
                primary_key=primary_key,
                upsert=False, # Always False for truncate and load
                fq_table_name_override=fq_table_name_override
            )

            load_result.load_duration_seconds = time.time() - start_time
            return load_result

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed truncate and load for {fq_table_name}: {e}" # Log FQN
            self.logger.error(error_msg)
            return LoadResult(
                table_name=fq_table_name, # Use FQN in result
                total_records=len(data),
                inserted_records=0,
                updated_records=0,
                skipped_records=len(data),
                load_duration_seconds=duration,
                status="failed",
                error_message=error_msg
            )

async def get_loader() -> PostgresLoader:
    """Returns a configured instance of the PostgresLoader."""
    db_manager = await get_database_manager()
    return PostgresLoader(db_manager)
