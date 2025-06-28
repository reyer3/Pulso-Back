"""
🎯 Production PostgreSQL Loader with SQLAlchemy Models
Intelligent incremental loading using TimescaleDB schema models

Features:
- SQLAlchemy ORM integration with TimescaleDB models
- Dynamic UPSERT based on configurable primary keys
- Batch processing for optimal performance
- Data validation and quality checks
- Comprehensive error handling and rollback
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Union, Type
import logging
from dataclasses import dataclass

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app.etl.config import ETLConfig, ExtractionConfig
from app.core.database import get_postgres_engine
from app.core.logging import LoggerMixin
from app.models.database import (
    TABLE_MODEL_MAPPING,
    DashboardDataModel,
    EvolutionDataModel,
    AssignmentDataModel,
    OperationDataModel,
    ProductivityDataModel,
    Base
)


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
    Production-ready PostgreSQL loader with SQLAlchemy ORM integration
    
    Features:
    - SQLAlchemy models for type safety and validation
    - PostgreSQL UPSERT using ON CONFLICT DO UPDATE
    - Efficient batch processing with ORM
    - TimescaleDB hypertable optimization
    - Detailed load statistics and error tracking
    """
    
    def __init__(self, engine: Optional[AsyncEngine] = None):
        super().__init__()
        self.engine = engine or get_postgres_engine()
        self.async_session_factory = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self.max_batch_size = 1000  # Smaller batches for ORM operations
        self.connection_timeout = 30
    
    def _get_model_class(self, table_name: str) -> Type[Base]:
        """Get SQLAlchemy model class for table"""
        if table_name not in TABLE_MODEL_MAPPING:
            raise ValueError(f"No model mapping found for table: {table_name}")
        return TABLE_MODEL_MAPPING[table_name]
    
    def _get_primary_key_columns(self, model_class: Type[Base]) -> List[str]:
        """Extract primary key column names from SQLAlchemy model"""
        return [col.name for col in model_class.__table__.primary_key.columns]
    
    def _parse_datetime_fields(self, record: Dict[str, Any], model_class: Type[Base]) -> Dict[str, Any]:
        """Ensure datetime fields are Python datetime objects."""
        parsed_record = record.copy()
        for column in model_class.__table__.columns:
            col_name = column.name
            if col_name in parsed_record:
                value = parsed_record[col_name]
                # Check if the column is a DateTime, Date, or Time type in SQLAlchemy
                if isinstance(column.type, (sqltypes.DateTime, sqltypes.Date, sqltypes.Time)):
                    if isinstance(value, str):
                        try:
                            # Handle ISO format strings (common from BigQuery extractor)
                            if 'T' in value and ('Z'in value or '+' in value or '-' in value.split('T')[-1]):
                                dt_value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                if isinstance(column.type, sqltypes.Date):
                                    parsed_record[col_name] = dt_value.date()
                                elif isinstance(column.type, sqltypes.Time):
                                    parsed_record[col_name] = dt_value.time()
                                else: # DateTime
                                    # Ensure timezone-aware if model expects it, or naive if not
                                    if column.type.timezone:
                                        parsed_record[col_name] = dt_value.astimezone(timezone.utc) if dt_value.tzinfo is None else dt_value
                                    else:
                                        parsed_record[col_name] = dt_value.replace(tzinfo=None)
                            # Handle YYYY-MM-DD for date fields
                            elif isinstance(column.type, sqltypes.Date) and re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                                parsed_record[col_name] = datetime.strptime(value, '%Y-%m-%d').date()
                        except ValueError:
                            self.logger.warning(f"Could not parse string '{value}' for datetime column '{col_name}'. Keeping original.")
                    elif isinstance(value, date) and not isinstance(value, datetime) and isinstance(column.type, sqltypes.DateTime):
                         # If model expects DateTime but gets Date, convert to midnight DateTime UTC
                        parsed_record[col_name] = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

        return parsed_record

    def _validate_data_batch(
        self, 
        data: List[Dict[str, Any]], 
        model_class: Type[Base]
    ) -> List[Dict[str, Any]]:
        """
        Validate and clean data batch before loading
        
        Args:
            data: List of records to validate
            model_class: SQLAlchemy model class for validation
            
        Returns:
            Cleaned and validated data
        """
        if not data:
            return []
        
        validated_data = []
        primary_key_cols = self._get_primary_key_columns(model_class)
        
        for i, record in enumerate(data):
            try:
                # Ensure datetime fields are actual datetime objects
                # This is important for SQLAlchemy to handle them correctly.
                record_with_pydt = self._parse_datetime_fields(record, model_class)

                # Check primary key values are not null
                valid_pk = True
                for pk_col in primary_key_cols:
                    if pk_col not in record_with_pydt or record_with_pydt[pk_col] is None:
                        self.logger.warning(
                            f"Record {i} has null primary key value for: {pk_col}. Skipping. Record: {record_with_pydt}"
                        )
                        valid_pk = False
                        break
                if not valid_pk:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                # Add processing timestamp if not present and if model has the column
                if 'fecha_procesamiento' in model_class.__table__.columns and 'fecha_procesamiento' not in record_with_pydt:
                    record_with_pydt['fecha_procesamiento'] = datetime.now(timezone.utc)
                
                # Ensure 'created_at' and 'updated_at' are not set by client if model handles them
                # Or ensure they are valid datetimes if client is supposed to set them (less common for these fields)
                # For now, assume model default or DB default handles created_at. updated_at handled by UPSERT.
                if 'created_at' in record_with_pydt and 'created_at' not in model_class.__table__.columns:
                    del record_with_pydt['created_at']
                if 'updated_at' in record_with_pydt and 'updated_at' not in model_class.__table__.columns:
                    del record_with_pydt['updated_at']

                validated_data.append(record_with_pydt)
                
            except Exception as e:
                self.logger.warning(f"Error validating record {i}: {str(e)}. Record: {record}")
                self.transformation_stats['records_skipped'] += 1
                continue
        
        self.logger.info(
            f"Validated {len(validated_data)} of {len(data)} records "
            f"({len(data) - len(validated_data)} invalid records skipped)"
        )
        
        return validated_data
    
    async def load_data_batch(
        self, 
        table_name: str,
        data: List[Dict[str, Any]],
        primary_key: Optional[List[str]] = None,  # Ignored - uses model definition
        upsert: bool = True,
        validate: bool = True
    ) -> LoadResult:
        """
        Load a batch of data using SQLAlchemy models with UPSERT support
        
        Args:
            table_name: Target table name
            data: List of records to load
            primary_key: Ignored - primary key taken from model definition
            upsert: Use UPSERT (True) or INSERT only (False)
            validate: Validate data before loading
            
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
            # Get model class
            model_class = self._get_model_class(table_name)
            
            # Validate data if requested
            if validate:
                data = self._validate_data_batch(data, model_class)
                
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
            
            # Process in batches using SQLAlchemy 2.0 style bulk operations
            total_records_in_batch = len(data)
            processed_count = 0
            
            async with self.async_session_factory() as session:
                async with session.begin(): # Ensure transaction block
                    if upsert:
                        # Prepare for PostgreSQL ON CONFLICT DO UPDATE
                        primary_key_cols = self._get_primary_key_columns(model_class)
                        if not primary_key_cols:
                            raise ValueError(f"No primary key defined for model {model_class.__name__}, UPSERT impossible.")

                        stmt = insert(model_class).values(data)
                        
                        # Columns to update, excluding primary keys and created_at
                        update_cols = {
                            c.name: c
                            for c in stmt.excluded if c.name not in primary_key_cols and c.name != 'created_at'
                        }
                        # Ensure 'updated_at' is set if the model has it
                        if 'updated_at' in model_class.__table__.columns:
                            update_cols['updated_at'] = datetime.now(timezone.utc)

                        # ON CONFLICT DO UPDATE SET (col1=excluded.col1, ...)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=primary_key_cols,
                            set_=update_cols
                        )
                        result = await session.execute(stmt)
                        processed_count = result.rowcount # rowcount for UPSERT indicates affected rows

                    else: # Plain bulk insert
                        # For plain inserts, SQLAlchemy 2.0 style is session.execute(insert(Model).values(data_list))
                        # However, the `data` here is already a list of dicts.
                        # The insert().values() can take a list of dicts directly.
                        stmt = insert(model_class).values(data)
                        result = await session.execute(stmt)
                        processed_count = result.rowcount # rowcount for INSERT indicates inserted rows
            
            duration = time.time() - start_time
            
            self.logger.info(
                f"Loaded {table_name}: {processed_count} records processed "
                f"(out of {total_records_in_batch} valid records) in {duration:.2f}s (UPSERT: {upsert})"
            )
            
            # Note: Distinguishing between truly inserted vs updated in a single UPSERT batch
            # can be complex without further RETURNING clauses and processing.
            # For now, inserted_records will reflect processed_count, and updated_records will be 0.
            return LoadResult(
                table_name=table_name,
                total_records=total_records_in_batch, # Number of records attempted in this batch
                inserted_records=processed_count,
                updated_records=0, # Simplified for now
                skipped_records=total_records_in_batch - processed_count, # if rowcount is less than batch size
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
        primary_key: Optional[List[str]] = None,  # Ignored
        upsert: bool = True,
        batch_size: Optional[int] = None
    ) -> LoadResult:
        """
        Load data from an async stream in batches using SQLAlchemy models
        
        Args:
            table_name: Target table name
            data_stream: Async generator yielding batches of records
            primary_key: Ignored - uses model definition
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
                
                # Load batch using the model-based approach
                batch_result = await self.load_data_batch(
                    table_name=table_name,
                    data=batch,
                    upsert=upsert,
                    validate=True
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
        Truncate table and load fresh data using SQLAlchemy models
        
        Args:
            table_name: Target table name
            data: Records to load
            
        Returns:
            LoadResult with operation statistics
        """
        start_time = time.time()
        
        try:
            # Get model class
            model_class = self._get_model_class(table_name)
            
            async with self.async_session_factory() as session:
                # Truncate table
                await session.execute(text(f"TRUNCATE TABLE {model_class.__tablename__}"))
                await session.commit()
                
            self.logger.info(f"Truncated table: {table_name}")
            
            # Load fresh data (no UPSERT needed after truncate)
            result = await self.load_data_batch(
                table_name=table_name,
                data=data,
                upsert=False,  # No need for UPSERT after TRUNCATE
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
        """Get statistics about a table using its model"""
        try:
            model_class = self._get_model_class(table_name)
            
            async with self.async_session_factory() as session:
                # Basic row count
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM {model_class.__tablename__}")
                )
                row_count = count_result.scalar()
                
                # Table size
                size_result = await session.execute(
                    text(f"""
                    SELECT pg_size_pretty(pg_total_relation_size('{model_class.__tablename__}')) as table_size
                    """)
                )
                table_size = size_result.scalar()
                
                # Column count from model
                column_count = len(model_class.__table__.columns)
                
                return {
                    "table_name": table_name,
                    "row_count": row_count,
                    "table_size": table_size,
                    "column_count": column_count,
                    "primary_key": self._get_primary_key_columns(model_class),
                    "model_class": model_class.__name__
                }
                
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e)
            }
    
    async def validate_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Validate that the actual table schema matches the SQLAlchemy model"""
        try:
            model_class = self._get_model_class(table_name)
            
            async with self.async_session_factory() as session:
                # Get actual table schema from database
                schema_result = await session.execute(
                    text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = :table_name
                    ORDER BY ordinal_position
                    """),
                    {"table_name": model_class.__tablename__}
                )
                
                db_columns = {row[0]: {"type": row[1], "nullable": row[2] == "YES"} 
                             for row in schema_result.fetchall()}
                
                # Get model columns
                model_columns = {col.name: {"type": str(col.type), "nullable": col.nullable} 
                               for col in model_class.__table__.columns}
                
                # Compare schemas
                missing_in_db = set(model_columns.keys()) - set(db_columns.keys())
                missing_in_model = set(db_columns.keys()) - set(model_columns.keys())
                
                return {
                    "table_name": table_name,
                    "schema_valid": len(missing_in_db) == 0 and len(missing_in_model) == 0,
                    "db_columns": len(db_columns),
                    "model_columns": len(model_columns),
                    "missing_in_db": list(missing_in_db),
                    "missing_in_model": list(missing_in_model),
                    "status": "valid" if len(missing_in_db) == 0 and len(missing_in_model) == 0 else "invalid"
                }
                
        except Exception as e:
            return {
                "table_name": table_name,
                "schema_valid": False,
                "error": str(e)
            }


# 🎯 Convenience functions for easy imports
async def get_loader() -> PostgresLoader:
    """Get configured PostgreSQL loader instance"""
    return PostgresLoader()


async def quick_load(
    table_name: str, 
    data: List[Dict[str, Any]]
) -> LoadResult:
    """Quick load with UPSERT for small datasets using models"""
    loader = await get_loader()
    return await loader.load_data_batch(
        table_name=table_name,
        data=data,
        upsert=True
    )
