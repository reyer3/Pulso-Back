import pytest
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from etl import PostgresLoader
from app.services.postgres_service import PostgresService
from app.models.database import Base, TABLE_MODEL_MAPPING
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

# Define a mock SQLAlchemy Base for testing purposes
TestBase = declarative_base()

class MockTableModel(TestBase):
    __tablename__ = "mock_table"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(Integer)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    fecha_procesamiento = Column(DateTime, default=datetime.now(timezone.utc))

# Temporarily add the mock model to the mapping for testing
original_table_model_mapping = TABLE_MODEL_MAPPING.copy()
TABLE_MODEL_MAPPING["mock_table"] = MockTableModel

@pytest.fixture
def mock_postgres_service():
    return AsyncMock(spec=PostgresService)

@pytest.fixture
def postgres_loader(mock_postgres_service):
    return PostgresLoader(pg_service=mock_postgres_service)

@pytest.mark.asyncio
async def test_load_data_batch_upsert_success(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    data = [
        {"id": 1, "name": "test1", "value": 100},
        {"id": 2, "name": "test2", "value": 200},
    ]

    mock_postgres_service.execute.return_value = "INSERT 0 2" # Simulate successful UPSERT

    result = await postgres_loader.load_data_batch(table_name, data, upsert=True)

    assert result.status == "success"
    assert result.total_records == 2
    assert result.inserted_records == 2
    assert result.skipped_records == 0
    mock_postgres_service.execute.assert_called_once()
    # Verify the SQL and parameters passed to execute
    call_args, call_kwargs = mock_postgres_service.execute.call_args
    assert "INSERT INTO mock_table" in call_args[0]
    assert "ON CONFLICT (id) DO UPDATE SET" in call_args[0]
    assert call_args[1:] == (1, "test1", 100, None, None, None, 2, "test2", 200, None, None, None) # Positional parameters

@pytest.mark.asyncio
async def test_load_data_batch_insert_success(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    data = [
        {"id": 3, "name": "test3", "value": 300},
    ]

    mock_postgres_service.execute.return_value = "INSERT 0 1" # Simulate successful INSERT

    result = await postgres_loader.load_data_batch(table_name, data, upsert=False)

    assert result.status == "success"
    assert result.total_records == 1
    assert result.inserted_records == 1
    assert result.skipped_records == 0
    assert mock_postgres_service.execute.call_count == 1 # One call per record for non-optimized insert
    # Verify the SQL and parameters passed to execute
    call_args, call_kwargs = mock_postgres_service.execute.call_args
    assert "INSERT INTO mock_table (id, name, value, created_at, updated_at, fecha_procesamiento) VALUES ($1, $2, $3, $4, $5, $6)" in call_args[0]
    assert call_args[1:] == (3, "test3", 300, None, None, None) # Positional parameters

@pytest.mark.asyncio
async def test_load_data_batch_empty_data(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    data = []

    result = await postgres_loader.load_data_batch(table_name, data)

    assert result.status == "success"
    assert result.total_records == 0
    assert result.inserted_records == 0
    assert result.skipped_records == 0
    mock_postgres_service.execute.assert_not_called()

@pytest.mark.asyncio
async def test_load_data_batch_validation_skips_invalid(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    data = [
        {"id": 1, "name": "valid", "value": 100},
        {"name": "invalid_no_pk", "value": 200}, # Missing primary key
    ]

    mock_postgres_service.execute.return_value = "INSERT 0 1"

    result = await postgres_loader.load_data_batch(table_name, data, validate=True)

    assert result.status == "success"
    assert result.total_records == 2 # Original total
    assert result.inserted_records == 1 # Only valid record inserted
    assert result.skipped_records == 1 # One record skipped due to validation
    mock_postgres_service.execute.assert_called_once()

@pytest.mark.asyncio
async def test_load_data_batch_exception_handling(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    data = [{"id": 1, "name": "test1", "value": 100}]

    mock_postgres_service.execute.side_effect = Exception("Database error")

    result = await postgres_loader.load_data_batch(table_name, data)

    assert result.status == "failed"
    assert "Database error" in result.error_message
    assert result.total_records == 1
    assert result.inserted_records == 0
    assert result.skipped_records == 1

@pytest.mark.asyncio
async def test_truncate_and_load_success(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    data = [{"id": 1, "name": "new_data", "value": 500}]

    mock_postgres_service.execute.side_effect = ["TRUNCATE TABLE mock_table", "INSERT 0 1"]

    result = await postgres_loader.truncate_and_load(table_name, data)

    assert result.status == "success"
    assert result.total_records == 1
    assert result.inserted_records == 1
    assert result.skipped_records == 0
    assert mock_postgres_service.execute.call_count == 2 # One for truncate, one for insert

@pytest.mark.asyncio
async def test_get_table_stats_success(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    mock_postgres_service.fetchval.side_effect = [
        10, # row_count
        "10 MB" # table_size
    ]

    stats = await postgres_loader.get_table_stats(table_name)

    assert stats["table_name"] == table_name
    assert stats["row_count"] == 10
    assert stats["table_size"] == "10 MB"
    assert stats["column_count"] == 6 # id, name, value, created_at, updated_at, fecha_procesamiento
    assert "primary_key" in stats
    assert "model_class" in stats
    assert mock_postgres_service.fetchval.call_count == 2

@pytest.mark.asyncio
async def test_validate_table_schema_success(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    # Simulate database schema matching the model
    mock_postgres_service.fetch.return_value = [
        {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        {"column_name": "name", "data_type": "character varying", "is_nullable": "YES"},
        {"column_name": "value", "data_type": "integer", "is_nullable": "YES"},
        {"column_name": "created_at", "data_type": "timestamp without time zone", "is_nullable": "YES"},
        {"column_name": "updated_at", "data_type": "timestamp without time zone", "is_nullable": "YES"},
        {"column_name": "fecha_procesamiento", "data_type": "timestamp without time zone", "is_nullable": "YES"},
    ]

    schema_validation = await postgres_loader.validate_table_schema(table_name)

    assert schema_validation["table_name"] == table_name
    assert schema_validation["schema_valid"] is True
    assert schema_validation["status"] == "valid"
    assert schema_validation["missing_in_db"] == []
    assert schema_validation["missing_in_model"] == []
    mock_postgres_service.fetch.assert_called_once()

@pytest.mark.asyncio
async def test_validate_table_schema_mismatch(postgres_loader, mock_postgres_service):
    table_name = "mock_table"
    # Simulate database schema missing a column from the model
    mock_postgres_service.fetch.return_value = [
        {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        {"column_name": "name", "data_type": "character varying", "is_nullable": "YES"},
        # 'value' column is missing in DB
    ]

    schema_validation = await postgres_loader.validate_table_schema(table_name)

    assert schema_validation["table_name"] == table_name
    assert schema_validation["schema_valid"] is False
    assert schema_validation["status"] == "invalid"
    assert "value" in schema_validation["missing_in_db"]
    assert schema_validation["missing_in_model"] == []
    mock_postgres_service.fetch.assert_called_once()

# Restore original mapping after tests
@pytest.fixture(scope="module", autouse=True)
def restore_table_model_mapping():
    yield
    TABLE_MODEL_MAPPING.clear()
    TABLE_MODEL_MAPPING.update(original_table_model_mapping)
