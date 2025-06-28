"""
🗄️ Database connection and session management
This file is being refactored to remove SQLAlchemy's direct async PostgreSQL integration
in favor of a direct asyncpg service.
SQLAlchemy might still be used for other purposes (e.g., migrations with Alembic, or other DB types).
"""

import logging
from typing import AsyncGenerator

# SQLAlchemy core components (potentially for Alembic or other DBs)
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

# Asyncpg related imports will be in app.services.postgres_service.py
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
# from sqlalchemy.orm import sessionmaker

from app.core.config import settings

logger = logging.getLogger(__name__)

# If SQLAlchemy is still needed for *PostgreSQL migrations via Alembic* but not for app runtime,
# a synchronous engine might be configured here for Alembic.
# For runtime, direct asyncpg will be used via PostgresService.

# Base class for models (if using SQLAlchemy ORM with other DBs or for Alembic)
Base = declarative_base()
metadata = MetaData() # Can be used by Alembic

# The following SQLAlchemy async setup for PostgreSQL is being deprecated:
# ASYNC_POSTGRES_URL = settings.POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
# engine = create_async_engine(...)
# AsyncSessionLocal = sessionmaker(...)

# The get_db dependency for SQLAlchemy async sessions is removed.
# If you need a PostgreSQL connection, you'll use the PostgresService.
# async def get_db() -> AsyncGenerator[AsyncSession, None]: ...

# The init_db function for creating tables via SQLAlchemy is removed.
# Database schema management should ideally be handled by migrations (e.g., Alembic).
# async def init_db() -> None: ...

# The close_db function for SQLAlchemy engine is removed.
# Connections via asyncpg will be managed by the PostgresService.
# async def close_db() -> None: ...


# Example: If Alembic needs to generate migrations based on SQLAlchemy models,
# it might require a synchronous engine. This is typically configured in Alembic's env.py
# from sqlalchemy import create_engine
# SQLALCHEMY_DATABASE_URL = settings.POSTGRES_URL # Standard URL for sync Alembic
# alembic_engine = create_engine(SQLALCHEMY_DATABASE_URL)

# For now, this file will primarily hold the Base and metadata if models are defined
# using SQLAlchemy's declarative system, which Alembic can use.

# If models are not using SQLAlchemy (e.g. Pydantic models for BigQuery/asyncpg results),
# then Base and metadata might also be removed if Alembic is not used or configured differently.
# For this refactor, we assume Alembic might still be in use, so Base and metadata are kept.

logger.info("database.py: SQLAlchemy async setup for PostgreSQL has been removed. "
            "Direct asyncpg via PostgresService should be used for runtime PostgreSQL operations. "
            "SQLAlchemy Base and metadata are kept for potential Alembic use.")

# To ensure models are registered with metadata if Base is used by them:
# from app.models import dashboard, evolution, assignment # noqa
# This line would typically be within init_db or similar, or just by virtue of models importing Base.
# If your models in app.models.* import Base from here, they get registered.

# placeholder for any future synchronous engine needed by Alembic, not for runtime
alembic_engine = None
if settings.DATA_SOURCE_TYPE == "postgresql" and settings.POSTGRES_URL:
    # This is a placeholder. Alembic's env.py is the primary place for its engine config.
    # from sqlalchemy import create_engine
    # logger.info("Creating synchronous SQLAlchemy engine for potential Alembic use.")
    # alembic_engine = create_engine(settings.POSTGRES_URL.replace("+asyncpg", "")) # Ensure it's a sync URL
    pass