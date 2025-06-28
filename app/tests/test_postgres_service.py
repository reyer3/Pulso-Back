import pytest
import pytest_asyncio
from unittest.mock import patch, AsyncMock

from app.services.postgres_service import PostgresService
from app.core.config import settings

# Set a test DSN or ensure your environment provides one for testing
# For example, by setting POSTGRES_URL in a .env.test file loaded by pytest-dotenv
# or by mocking settings directly if preferred.
TEST_POSTGRES_DSN = settings.POSTGRES_URL or "postgresql://testuser:testpass@testhost/testdb"

@pytest.fixture
def service():
    """Fixture to create a PostgresService instance for testing."""
    # It's good practice to ensure a test-specific DSN is used if possible.
    # If settings.POSTGRES_URL is not set, this will use the default test DSN.
    return PostgresService(dsn=TEST_POSTGRES_DSN)

@pytest_asyncio.fixture
async def mock_asyncpg_connect():
    """Mocks asyncpg.connect and the connection object."""
    with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn

        # Mock connection methods
        mock_conn.fetch = AsyncMock()
        mock_conn.fetchrow = AsyncMock()
        mock_conn.fetchval = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        yield mock_connect, mock_conn

@pytest.mark.asyncio
async def test_postgres_service_init_with_dsn(service):
    assert service.dsn == TEST_POSTGRES_DSN

@pytest.mark.asyncio
async def test_postgres_service_init_no_dsn_raises_error():
    original_url = settings.POSTGRES_URL
    settings.POSTGRES_URL = None # Temporarily unset
    with pytest.raises(ValueError, match="PostgreSQL DSN must be configured"):
        PostgresService()
    settings.POSTGRES_URL = original_url # Restore

@pytest.mark.asyncio
async def test_fetch_successful(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.fetch.return_value = [{"id": 1, "name": "Test"}]

    query = "SELECT * FROM test_table"
    result = await service.fetch(query, 123)

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    mock_conn.fetch.assert_called_once_with(query, 123)
    mock_conn.close.assert_called_once()
    assert result == [{"id": 1, "name": "Test"}]

@pytest.mark.asyncio
async def test_fetchrow_successful(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.fetchrow.return_value = {"id": 1, "name": "Test"}

    query = "SELECT * FROM test_table WHERE id = $1"
    param = 1
    result = await service.fetchrow(query, param)

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    mock_conn.fetchrow.assert_called_once_with(query, param)
    mock_conn.close.assert_called_once()
    assert result == {"id": 1, "name": "Test"}

@pytest.mark.asyncio
async def test_fetchval_successful(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.fetchval.return_value = 123

    query = "SELECT COUNT(*) FROM test_table"
    result = await service.fetchval(query)

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    mock_conn.fetchval.assert_called_once_with(query)
    mock_conn.close.assert_called_once()
    assert result == 123

@pytest.mark.asyncio
async def test_execute_successful(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.execute.return_value = "INSERT 0 1"

    query = "INSERT INTO test_table (name) VALUES ($1)"
    param = "New Value"
    result = await service.execute(query, param)

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    mock_conn.execute.assert_called_once_with(query, param)
    mock_conn.close.assert_called_once()
    assert result == "INSERT 0 1"

@pytest.mark.asyncio
async def test_health_check_healthy(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.fetchval.return_value = 1 # Simulate SELECT 1 returning 1

    is_healthy = await service.health_check()

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    mock_conn.fetchval.assert_called_once_with("SELECT 1")
    mock_conn.close.assert_called_once()
    assert is_healthy is True

@pytest.mark.asyncio
async def test_health_check_unhealthy_value_mismatch(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.fetchval.return_value = 0 # Simulate SELECT 1 returning something else

    is_healthy = await service.health_check()

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    assert is_healthy is False

@pytest.mark.asyncio
async def test_health_check_exception(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect
    mock_conn.fetchval.side_effect = Exception("Connection error")

    is_healthy = await service.health_check()

    mock_connect.assert_called_once_with(dsn=TEST_POSTGRES_DSN)
    assert is_healthy is False # Should catch exception and return False

@pytest.mark.asyncio
async def test_connection_failure_raises_and_logs(service, mock_asyncpg_connect):
    mock_connect, _ = mock_asyncpg_connect
    mock_connect.side_effect = Exception("Failed to connect")

    with patch("app.services.postgres_service.logger.error") as mock_logger_error:
        with pytest.raises(Exception, match="Failed to connect"):
            await service.fetch("SELECT 1")
        mock_logger_error.assert_called_once()
        # Check that the error message contains "Failed to connect to PostgreSQL"
        args, _ = mock_logger_error.call_args
        assert "Failed to connect to PostgreSQL" in args[0]


@pytest.mark.asyncio
async def test_query_method_exception_logs_and_raises(service, mock_asyncpg_connect):
    mock_connect, mock_conn = mock_asyncpg_connect

    # Test for fetch
    mock_conn.fetch.side_effect = Exception("Query execution error for fetch")
    with patch("app.services.postgres_service.logger.error") as mock_logger_error:
        with pytest.raises(Exception, match="Query execution error for fetch"):
            await service.fetch("SELECT * FROM table")
        mock_logger_error.assert_called_once()
        args, _ = mock_logger_error.call_args
        assert "Error executing fetch query" in args[0]
        assert "SELECT * FROM table" in args[0]

    mock_conn.close.assert_called_once() # Ensure close is called even on error
    mock_conn.reset_mock() # Reset for next call
    mock_connect.reset_mock()

    # Test for fetchrow
    mock_conn.fetchrow.side_effect = Exception("Query execution error for fetchrow")
    with patch("app.services.postgres_service.logger.error") as mock_logger_error:
        with pytest.raises(Exception, match="Query execution error for fetchrow"):
            await service.fetchrow("SELECT * FROM table_row")
        mock_logger_error.assert_called_once()
        args, _ = mock_logger_error.call_args
        assert "Error executing fetchrow query" in args[0]
        assert "SELECT * FROM table_row" in args[0]

    mock_conn.close.assert_called_once()
    mock_conn.reset_mock()
    mock_connect.reset_mock()

    # Test for fetchval
    mock_conn.fetchval.side_effect = Exception("Query execution error for fetchval")
    with patch("app.services.postgres_service.logger.error") as mock_logger_error:
        with pytest.raises(Exception, match="Query execution error for fetchval"):
            await service.fetchval("SELECT val FROM table_val")
        mock_logger_error.assert_called_once()
        args, _ = mock_logger_error.call_args
        assert "Error executing fetchval query" in args[0]
        assert "SELECT val FROM table_val" in args[0]

    mock_conn.close.assert_called_once()
    mock_conn.reset_mock()
    mock_connect.reset_mock()

    # Test for execute
    mock_conn.execute.side_effect = Exception("Query execution error for execute")
    with patch("app.services.postgres_service.logger.error") as mock_logger_error:
        with pytest.raises(Exception, match="Query execution error for execute"):
            await service.execute("INSERT SOMETHING")
        mock_logger_error.assert_called_once()
        args, _ = mock_logger_error.call_args
        assert "Error executing execute query" in args[0]
        assert "INSERT SOMETHING" in args[0]

    mock_conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_dsn_conversion_from_sqlalchemy_asyncpg_format(mock_asyncpg_connect):
    """
    Tests that if a DSN like 'postgresql+asyncpg://...' is passed, it's converted
    to 'postgresql://...' for asyncpg.connect.
    """
    mock_connect, _ = mock_asyncpg_connect
    sqlalchemy_dsn = "postgresql+asyncpg://user:pass@host/db"
    expected_asyncpg_dsn = "postgresql://user:pass@host/db"

    with patch("app.services.postgres_service.logger.warning") as mock_logger_warning:
        service_with_conversion = PostgresService(dsn=sqlalchemy_dsn)
        assert service_with_conversion.dsn == expected_asyncpg_dsn
        mock_logger_warning.assert_called_once()
        args, _ = mock_logger_warning.call_args
        assert "DSN starts with postgresql+asyncpg://" in args[0]

    # Ensure it still calls connect with the corrected DSN
    await service_with_conversion.fetch("SELECT 1")
    mock_connect.assert_called_once_with(dsn=expected_asyncpg_dsn)

# It's good practice to also test if existing BigQuery tests need updates,
# but that's outside the scope of this specific file.
# For example, ensure that `app/tests/test_bigquery_repo.py` exists and is comprehensive.
# This task focuses on adding tests for the new `PostgresService`.
# Placeholder for BigQuery tests (assuming they exist elsewhere or will be added):
# from app.repositories.bigquery_repo import BigQueryRepository # etc.
# async def test_bigquery_repo_health_check(): ...
# async def test_bigquery_repo_queries(): ...
