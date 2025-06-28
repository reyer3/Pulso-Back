"""
🔄 Alembic Environment for TimescaleDB
Production-ready migration environment with TimescaleDB support
"""

import asyncio
import os
from logging.config import fileConfig
from typing import Dict, Any

from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine import Connection
from alembic import context

# Import our models for autogenerate support
from app.database.schemas import Base

# =============================================================================
# ALEMBIC CONFIGURATION
# =============================================================================

# This is the Alembic Config object
config = context.config

# Setup logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata for autogenerate support
target_metadata = Base.metadata

# =============================================================================
# DATABASE URL CONFIGURATION
# =============================================================================

def get_database_url() -> str:
    """Get database URL from environment or config"""
    # Try environment variable first (production)
    db_url = os.getenv("DATABASE_URL")
    
    if db_url:
        # Convert psycopg2 URL to asyncpg if needed
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
        return db_url
    
    # Fallback to alembic.ini config (development)
    return config.get_main_option("sqlalchemy.url", "postgresql+asyncpg://user:pass@localhost:5432/pulso_db")


# =============================================================================
# TIMESCALEDB UTILITIES
# =============================================================================

def is_timescaledb_available(connection: Connection) -> bool:
    """Check if TimescaleDB extension is available"""
    try:
        result = connection.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
        return result.fetchone() is not None
    except:
        return False


def execute_timescaledb_operations(connection: Connection, operations: Dict[str, Any]) -> None:
    """Execute TimescaleDB-specific operations (hypertables, retention policies, etc.)"""
    
    if not is_timescaledb_available(connection):
        print("⚠️  TimescaleDB extension not found - skipping time-series optimizations")
        return
    
    print("🚀 Configuring TimescaleDB optimizations...")
    
    # Import TimescaleDB configuration
    from app.database.schemas import TIMESCALE_HYPERTABLES, CONTINUOUS_AGGREGATES
    
    # Create hypertables
    for table_name, config in TIMESCALE_HYPERTABLES.items():
        try:
            # Check if table exists and is not already a hypertable
            result = connection.execute(f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{table_name}' AND table_schema = 'public'
            """)
            
            if result.fetchone():
                # Check if it's already a hypertable
                result = connection.execute(f"""
                    SELECT 1 FROM timescaledb_information.hypertables 
                    WHERE hypertable_name = '{table_name}'
                """)
                
                if not result.fetchone():
                    # Create hypertable
                    sql = f"""
                    SELECT create_hypertable(
                        '{table_name}', 
                        '{config["time_column"]}',
                        chunk_time_interval => INTERVAL '{config["chunk_time_interval"]}'
                    )
                    """
                    connection.execute(sql)
                    print(f"✅ Created hypertable: {table_name}")
                    
                    # Add retention policy if specified
                    if "retention_policy" in config:
                        retention_sql = f"""
                        SELECT add_retention_policy(
                            '{table_name}', 
                            INTERVAL '{config["retention_policy"]}'
                        )
                        """
                        connection.execute(retention_sql)
                        print(f"✅ Added retention policy: {table_name} -> {config['retention_policy']}")
                else:
                    print(f"ℹ️  Hypertable already exists: {table_name}")
                    
        except Exception as e:
            print(f"❌ Error creating hypertable {table_name}: {str(e)}")
    
    # Create continuous aggregates (optional, for performance)
    for agg_name, config in CONTINUOUS_AGGREGATES.items():
        try:
            # Check if continuous aggregate already exists
            result = connection.execute(f"""
                SELECT 1 FROM timescaledb_information.continuous_aggregates 
                WHERE view_name = '{agg_name}'
            """)
            
            if not result.fetchone():
                # Build aggregation SQL
                aggregates_str = ", ".join(config["aggregates"])
                group_by_str = ", ".join(config["group_by"])
                
                sql = f"""
                CREATE MATERIALIZED VIEW {agg_name}
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket(INTERVAL '{config["time_bucket"]}', {TIMESCALE_HYPERTABLES[config["source_table"]]["time_column"]}) AS time_bucket,
                    {group_by_str},
                    {aggregates_str}
                FROM {config["source_table"]}
                GROUP BY time_bucket, {group_by_str}
                """
                
                connection.execute(sql)
                print(f"✅ Created continuous aggregate: {agg_name}")
            else:
                print(f"ℹ️  Continuous aggregate already exists: {agg_name}")
                
        except Exception as e:
            print(f"❌ Error creating continuous aggregate {agg_name}: {str(e)}")


# =============================================================================
# MIGRATION FUNCTIONS
# =============================================================================

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode"""
    url = get_database_url()
    
    # Remove async driver for offline mode
    if "+asyncpg" in url:
        url = url.replace("+asyncpg", "")
    
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations with connection"""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()
        
        # Execute TimescaleDB operations after standard migrations
        if context.is_migration_mode():
            execute_timescaledb_operations(connection, {})


async def run_async_migrations() -> None:
    """Run migrations in async mode for production"""
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_database_url()

    connectable = create_async_engine(
        configuration["sqlalchemy.url"],
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode"""
    asyncio.run(run_async_migrations())


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
