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
    
    def _build_upsert_statement(
        self, 
        model_class: Type[Base], 
        data_batch: List[Dict[str, Any]]
    ) -> str:
        """
        Build PostgreSQL UPSERT statement using ON CONFLICT DO UPDATE
        
        Args:
            model_class: SQLAlchemy model class
            data_batch: List of data dictionaries
            
        Returns:
            PostgreSQL UPSERT statement
        """
        if not data_batch:
            raise ValueError("Data batch cannot be empty")
        
        table_name = model_class.__tablename__
        primary_key_cols = self._get_primary_key_columns(model_class)
        
        # Get all columns from the first record
        all_columns = list(data_batch[0].keys())
        
        # Prepare column lists
        columns_str = ", ".join(all_columns)
        conflict_columns_str = ", ".join(primary_key_cols)
        
        # Build SET clause for UPDATE (exclude primary key columns and metadata)
        update_columns = [
            col for col in all_columns 
            if col not in primary_key_cols 
            and col not in ['created_at', 'updated_at', 'fecha_procesamiento']
        ]
        
        set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        # Add metadata update
        set_clause += ", updated_at = CURRENT_TIMESTAMP"
        
        # Build VALUES clause
        values_placeholder = ", ".join([
            f"({', '.join([f':{col}_{i}' for col in all_columns])})"
            for i in range(len(data_batch))
        ])
        
        upsert_sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES {values_placeholder}
        ON CONFLICT ({conflict_columns_str})
        DO UPDATE SET {set_clause}
        """
        
        return upsert_sql
    
    def _prepare_batch_parameters(
        self, 
        data_batch: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Prepare parameters for batch UPSERT operation
        
        Args:
            data_batch: List of data dictionaries
            
        Returns:
            Flattened parameter dictionary for SQL execution
        """
        params = {}
        
        for i, record in enumerate(data_batch):
            for col, value in record.items():
                # Handle datetime serialization
                if isinstance(value, str) and 'T' in value:
                    try:
                        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except ValueError:
                        pass  # Keep as string if not valid datetime
                
                params[f"{col}_{i}"] = value
        
        return params
    
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
                # Check primary key values are not null
                for pk_col in primary_key_cols:
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
                            cleaned_record[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        except ValueError:
                            cleaned_record[key] = value
                    else:
                        cleaned_record[key] = value
                
                # Add processing timestamp if not present
                if 'fecha_procesamiento' not in cleaned_record:
                    cleaned_record['fecha_procesamiento'] = datetime.now(timezone.utc)
                
                validated_data.append(cleaned_record)
                
            except Exception as e:
                self.logger.warning(f"Error validating record {i}: {str(e)}")
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
            
            # Process in batches
            total_processed = 0
            
            async with self.async_session_factory() as session:
                # Split data into batches
                for i in range(0, len(data), self.max_batch_size):
                    batch = data[i:i + self.max_batch_size]
                    
                    if upsert:
                        # Use raw SQL UPSERT for better performance
                        upsert_sql = self._build_upsert_statement(model_class, batch)
                        params = self._prepare_batch_parameters(batch)
                        
                        result = await session.execute(text(upsert_sql), params)
                        total_processed += len(batch)
                        
                    else:
                        # Use ORM bulk insert
                        objects = [model_class(**record) for record in batch]
                        session.add_all(objects)
                        total_processed += len(objects)
                
                # Commit all changes
                await session.commit()
            
            duration = time.time() - start_time
            
            self.logger.info(
                f"Loaded {table_name}: {total_processed} records "
                f"in {duration:.2f}s (UPSERT: {upsert})"
            )
            
            return LoadResult(
                table_name=table_name,
                total_records=len(data),
                inserted_records=total_processed,  # TODO: Distinguish INSERT vs UPDATE
                updated_records=0,
                skipped_records=len(data) - total_processed,
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
