"""
🗄️ Database connection and session management
This file is being refactored to remove SQLAlchemy's direct async PostgreSQL integration
in favor of a direct asyncpg service.
SQLAlchemy might still be used for other purposes (e.g., migrations with Alembic, or other DB types).
"""

import logging
from typing import AsyncGenerator, Optional

# SQLAlchemy core components (potentially for Alembic or other DBs)
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.config import settings

logger = logging.getLogger(__name__)

# Base class for models (if using SQLAlchemy ORM with other DBs or for Alembic)
Base = declarative_base()
metadata = MetaData() # Can be used by Alembic

# 🔧 TEMPORARY: Keep AsyncEngine for watermarks system
# This will be refactored to use asyncpg directly in the future
_postgres_engine: Optional[AsyncEngine] = None

async def get_postgres_engine() -> AsyncEngine:
    """
    Get PostgreSQL AsyncEngine for watermarks system
    
    TEMPORARY: This function provides backward compatibility while
    the system transitions from SQLAlchemy to pure asyncpg
    """
    global _postgres_engine
    
    if _postgres_engine is None:
        # Convert postgresql:// to postgresql+asyncpg:// for async support
        async_url = settings.POSTGRES_URL.replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        
        _postgres_engine = create_async_engine(
            async_url,
            echo=settings.DEBUG,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        
        logger.info("Created PostgreSQL AsyncEngine for watermarks system")
    
    return _postgres_engine

async def close_postgres_engine() -> None:
    """Close the PostgreSQL engine"""
    global _postgres_engine
    
    if _postgres_engine:
        await _postgres_engine.dispose()
        _postgres_engine = None
        logger.info("Closed PostgreSQL AsyncEngine")

# placeholder for any future synchronous engine needed by Alembic, not for runtime
alembic_engine = None
if settings.DATA_SOURCE_TYPE == "postgresql" and settings.POSTGRES_URL:
    # This is a placeholder. Alembic's env.py is the primary place for its engine config.
    # from sqlalchemy import create_engine
    # logger.info("Creating synchronous SQLAlchemy engine for potential Alembic use.")
    # alembic_engine = create_engine(settings.POSTGRES_URL.replace("+asyncpg", "")) # Ensure it's a sync URL
    pass

logger.info("database.py: PostgreSQL AsyncEngine available for watermarks system. "
            "Direct asyncpg via PostgresService should be used for other runtime PostgreSQL operations. "
            "SQLAlchemy Base and metadata are kept for potential Alembic use.")
