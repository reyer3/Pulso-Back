"""
🔄 Database Migration Manager
Production-ready migration utilities for TimescaleDB

Features:
- Automated migration execution
- Schema validation
- TimescaleDB optimization
- Rollback capabilities
"""

import asyncio
import os
import subprocess
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text, inspect
from alembic.config import Config
from alembic import command
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext

from app.database.schemas import Base, TIMESCALE_HYPERTABLES


class MigrationManager:
    """
    Production migration manager for TimescaleDB
    
    Handles database schema creation, migrations, and TimescaleDB optimizations
    """
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.logger = logging.getLogger(__name__)
        
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
            
        # Ensure async driver
        if not self.database_url.startswith("postgresql+asyncpg://"):
            if self.database_url.startswith("postgresql://"):
                self.database_url = self.database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            elif self.database_url.startswith("postgres://"):
                self.database_url = self.database_url.replace("postgres://", "postgresql+asyncpg://", 1)
        
        self.engine: Optional[AsyncEngine] = None
        self.alembic_cfg = self._get_alembic_config()
    
    def _get_alembic_config(self) -> Config:
        """Get Alembic configuration"""
        # Find alembic.ini in project root
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        alembic_ini_path = os.path.join(project_root, "alembic.ini")
        
        if not os.path.exists(alembic_ini_path):
            raise FileNotFoundError(f"alembic.ini not found at {alembic_ini_path}")
        
        cfg = Config(alembic_ini_path)
        cfg.set_main_option("sqlalchemy.url", self.database_url.replace("+asyncpg", ""))  # Sync URL for Alembic
        return cfg
    
    async def _get_engine(self) -> AsyncEngine:
        """Get async database engine"""
        if self.engine is None:
            self.engine = create_async_engine(self.database_url, echo=False)
        return self.engine
    
    async def check_database_connection(self) -> Dict[str, Any]:
        """Check database connectivity and basic info"""
        try:
            engine = await self._get_engine()
            
            async with engine.connect() as conn:
                # Basic connection test
                result = await conn.execute(text("SELECT 1"))
                result.fetchone()
                
                # Get database info
                db_info = await conn.execute(text("""
                    SELECT 
                        version() as postgres_version,
                        current_database() as database_name,
                        current_user as user_name,
                        inet_server_addr() as server_addr,
                        inet_server_port() as server_port
                """))
                info = dict(db_info.fetchone()._mapping)
                
                # Check TimescaleDB
                timescale_result = await conn.execute(text("""
                    SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'
                """))
                timescale_row = timescale_result.fetchone()
                
                info["timescaledb_version"] = timescale_row[0] if timescale_row else None
                info["timescaledb_available"] = timescale_row is not None
                
                return {
                    "status": "connected",
                    "database_info": info
                }
                
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def check_migration_status(self) -> Dict[str, Any]:
        """Check current migration status"""
        try:
            # Get current revision from database
            engine = await self._get_engine()
            
            async with engine.connect() as conn:
                # Check if alembic_version table exists
                result = await conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'alembic_version'
                    )
                """))
                
                alembic_table_exists = result.fetchone()[0]
                
                if alembic_table_exists:
                    # Get current revision
                    result = await conn.execute(text("SELECT version_num FROM alembic_version"))
                    current_row = result.fetchone()
                    current_revision = current_row[0] if current_row else None
                else:
                    current_revision = None
            
            # Get available revisions from Alembic
            script_dir = ScriptDirectory.from_config(self.alembic_cfg)
            available_revisions = [rev.revision for rev in script_dir.walk_revisions()]
            head_revision = script_dir.get_current_head()
            
            return {
                "current_revision": current_revision,
                "head_revision": head_revision,
                "available_revisions": available_revisions,
                "needs_migration": current_revision != head_revision,
                "alembic_initialized": alembic_table_exists
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "needs_migration": True
            }
    
    def initialize_alembic(self) -> Dict[str, Any]:
        """Initialize Alembic for the project"""
        try:
            # Check if already initialized
            script_dir_path = self.alembic_cfg.get_main_option("script_location")
            if os.path.exists(os.path.join(script_dir_path, "versions")):
                return {
                    "status": "already_initialized",
                    "message": "Alembic is already initialized"
                }
            
            # Initialize Alembic
            command.init(self.alembic_cfg, script_dir_path)
            
            return {
                "status": "initialized",
                "message": "Alembic initialized successfully"
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def create_migration(self, message: str, autogenerate: bool = True) -> Dict[str, Any]:
        """Create a new migration"""
        try:
            if autogenerate:
                command.revision(self.alembic_cfg, message=message, autogenerate=True)
            else:
                command.revision(self.alembic_cfg, message=message)
            
            return {
                "status": "created",
                "message": f"Migration '{message}' created successfully"
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def run_migrations(self, target_revision: str = "head") -> Dict[str, Any]:
        """Run migrations to target revision"""
        try:
            command.upgrade(self.alembic_cfg, target_revision)
            
            return {
                "status": "completed",
                "message": f"Migrations applied to {target_revision}"
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def rollback_migration(self, target_revision: str) -> Dict[str, Any]:
        """Rollback to target revision"""
        try:
            command.downgrade(self.alembic_cfg, target_revision)
            
            return {
                "status": "completed",
                "message": f"Rolled back to {target_revision}"
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def validate_schema(self) -> Dict[str, Any]:
        """Validate that all expected tables exist with correct schema"""
        try:
            engine = await self._get_engine()
            
            async with engine.connect() as conn:
                # Get actual table schemas
                inspector = inspect(engine.sync_engine)
                
                existing_tables = []
                missing_tables = []
                schema_issues = []
                
                # Check each table in our schema
                for table_name, table in Base.metadata.tables.items():
                    if inspector.has_table(table_name):
                        existing_tables.append(table_name)
                        
                        # Check columns
                        actual_columns = {col['name']: col for col in inspector.get_columns(table_name)}
                        expected_columns = {col.name: col for col in table.columns}
                        
                        for col_name, col_obj in expected_columns.items():
                            if col_name not in actual_columns:
                                schema_issues.append(f"Missing column {col_name} in table {table_name}")
                    else:
                        missing_tables.append(table_name)
                
                # Check TimescaleDB hypertables
                timescale_status = {}
                for table_name in TIMESCALE_HYPERTABLES.keys():
                    if table_name in existing_tables:
                        result = await conn.execute(text(f"""
                            SELECT 1 FROM timescaledb_information.hypertables 
                            WHERE hypertable_name = '{table_name}'
                        """))
                        is_hypertable = result.fetchone() is not None
                        timescale_status[table_name] = is_hypertable
                
                return {
                    "status": "completed",
                    "existing_tables": existing_tables,
                    "missing_tables": missing_tables,
                    "schema_issues": schema_issues,
                    "timescale_hypertables": timescale_status,
                    "is_valid": len(missing_tables) == 0 and len(schema_issues) == 0
                }
                
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def setup_fresh_database(self) -> Dict[str, Any]:
        """Setup database from scratch (for new deployments)"""
        try:
            # Check connection
            conn_status = await self.check_database_connection()
            if conn_status["status"] != "connected":
                return {
                    "status": "failed",
                    "error": "Cannot connect to database",
                    "details": conn_status
                }
            
            # Check migration status
            migration_status = await self.check_migration_status()
            
            steps_completed = []
            
            # Run migrations if needed
            if migration_status.get("needs_migration", True):
                migration_result = self.run_migrations()
                if migration_result["status"] != "completed":
                    return {
                        "status": "failed",
                        "error": "Migration failed",
                        "details": migration_result
                    }
                steps_completed.append("migrations_applied")
            
            # Validate schema
            validation_result = await self.validate_schema()
            if validation_result["status"] == "completed":
                steps_completed.append("schema_validated")
            
            return {
                "status": "completed",
                "message": "Database setup completed successfully",
                "steps_completed": steps_completed,
                "validation": validation_result
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics and health info"""
        try:
            engine = await self._get_engine()
            
            async with engine.connect() as conn:
                # Get table sizes
                table_stats = await conn.execute(text("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                """))
                
                tables = []
                for row in table_stats:
                    tables.append({
                        "name": row[1],
                        "size": row[2],
                        "size_bytes": row[3]
                    })
                
                # Get TimescaleDB info if available
                timescale_info = {}
                try:
                    hypertable_stats = await conn.execute(text("""
                        SELECT 
                            hypertable_name,
                            num_chunks,
                            table_size,
                            index_size,
                            total_size
                        FROM timescaledb_information.hypertable_detailed_size
                    """))
                    
                    timescale_info["hypertables"] = []
                    for row in hypertable_stats:
                        timescale_info["hypertables"].append({
                            "name": row[0],
                            "chunks": row[1],
                            "table_size": row[2],
                            "index_size": row[3],
                            "total_size": row[4]
                        })
                except:
                    timescale_info["error"] = "TimescaleDB not available"
                
                return {
                    "status": "completed",
                    "tables": tables,
                    "timescale": timescale_info
                }
                
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def close(self):
        """Close database connections"""
        if self.engine:
            await self.engine.dispose()
            self.engine = None


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def quick_migration_status() -> Dict[str, Any]:
    """Quick check of migration status"""
    manager = MigrationManager()
    try:
        return await manager.check_migration_status()
    finally:
        await manager.close()


async def setup_database() -> Dict[str, Any]:
    """Setup database for new deployment"""
    manager = MigrationManager()
    try:
        return await manager.setup_fresh_database()
    finally:
        await manager.close()


async def validate_database() -> Dict[str, Any]:
    """Validate database schema"""
    manager = MigrationManager()
    try:
        return await manager.validate_schema()
    finally:
        await manager.close()


# =============================================================================
# CLI UTILITIES
# =============================================================================

def run_migration_command(command_name: str, *args) -> int:
    """Run migration command via CLI"""
    try:
        manager = MigrationManager()
        
        if command_name == "status":
            result = asyncio.run(manager.check_migration_status())
            print(f"Migration Status: {result}")
            
        elif command_name == "upgrade":
            target = args[0] if args else "head"
            result = manager.run_migrations(target)
            print(f"Migration Result: {result}")
            
        elif command_name == "create":
            if not args:
                print("Error: Migration message required")
                return 1
            result = manager.create_migration(args[0])
            print(f"Migration Creation: {result}")
            
        elif command_name == "validate":
            result = asyncio.run(manager.validate_schema())
            print(f"Schema Validation: {result}")
            
        elif command_name == "setup":
            result = asyncio.run(manager.setup_fresh_database())
            print(f"Database Setup: {result}")
            
        else:
            print(f"Unknown command: {command_name}")
            print("Available commands: status, upgrade, create, validate, setup")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m app.database.migration_manager <command> [args...]")
        print("Commands: status, upgrade, create <message>, validate, setup")
        sys.exit(1)
    
    command = sys.argv[1]
    args = sys.argv[2:]
    
    exit_code = run_migration_command(command, *args)
    sys.exit(exit_code)
