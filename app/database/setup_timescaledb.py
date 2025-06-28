"""
🗃️ TimescaleDB Setup and Configuration Script
Production-ready setup for Pulso Dashboard TimescaleDB

This script helps configure TimescaleDB for optimal performance with
the Pulso dashboard ETL system.
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy import text
from alembic.config import Config
from alembic import command

from app.database.schemas import TIMESCALE_HYPERTABLES, CONTINUOUS_AGGREGATES
from app.models.database import Base


class TimescaleDBSetup:
    """
    TimescaleDB setup and configuration utility
    
    Features:
    - Check TimescaleDB extension availability
    - Configure hypertables with optimal settings
    - Set up retention policies
    - Create continuous aggregates for performance
    - Validate configuration
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine: Optional[AsyncEngine] = None
        self.logger = logging.getLogger(__name__)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    async def _get_engine(self) -> AsyncEngine:
        """Get or create async database engine"""
        if self.engine is None:
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                pool_pre_ping=True
            )
        return self.engine
    
    async def check_timescaledb_available(self) -> bool:
        """Check if TimescaleDB extension is available and installed"""
        try:
            engine = await self._get_engine()
            async with engine.begin() as conn:
                result = await conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
                )
                return result.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking TimescaleDB availability: {e}")
            return False
    
    async def install_timescaledb_extension(self) -> bool:
        """Install TimescaleDB extension (requires superuser privileges)"""
        try:
            engine = await self._get_engine()
            async with engine.begin() as conn:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
                self.logger.info("✅ TimescaleDB extension installed successfully")
                return True
        except Exception as e:
            self.logger.error(f"❌ Failed to install TimescaleDB extension: {e}")
            self.logger.info("💡 Note: TimescaleDB extension requires superuser privileges")
            return False
    
    async def run_migrations(self) -> bool:
        """Run Alembic migrations to create tables"""
        try:
            # Configure Alembic
            alembic_cfg = Config("app/database/alembic.ini")
            alembic_cfg.set_main_option("script_location", "app/database/migrations")
            alembic_cfg.set_main_option("sqlalchemy.url", self.database_url)
            
            # Run migrations
            self.logger.info("🔄 Running database migrations...")
            command.upgrade(alembic_cfg, "head")
            self.logger.info("✅ Database migrations completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Migration failed: {e}")
            return False
    
    async def configure_hypertables(self) -> Dict[str, bool]:
        """Configure TimescaleDB hypertables with optimal settings"""
        results = {}
        
        if not await self.check_timescaledb_available():
            self.logger.warning("⚠️ TimescaleDB not available - skipping hypertable configuration")
            return results
        
        engine = await self._get_engine()
        
        for table_name, config in TIMESCALE_HYPERTABLES.items():
            try:
                async with engine.begin() as conn:
                    # Create hypertable
                    await conn.execute(text(f"""
                    SELECT create_hypertable(
                        '{table_name}', 
                        '{config['time_column']}',
                        chunk_time_interval => INTERVAL '{config['chunk_time_interval']}',
                        if_not_exists => TRUE
                    )
                    """))
                    
                    self.logger.info(f"✅ Configured hypertable: {table_name}")
                    results[table_name] = True
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to configure hypertable {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    async def configure_retention_policies(self) -> Dict[str, bool]:
        """Configure retention policies for hypertables"""
        results = {}
        
        if not await self.check_timescaledb_available():
            self.logger.warning("⚠️ TimescaleDB not available - skipping retention policies")
            return results
        
        engine = await self._get_engine()
        
        for table_name, config in TIMESCALE_HYPERTABLES.items():
            try:
                async with engine.begin() as conn:
                    # Add retention policy
                    await conn.execute(text(f"""
                    SELECT add_retention_policy(
                        '{table_name}', 
                        INTERVAL '{config['retention_policy']}',
                        if_not_exists => TRUE
                    )
                    """))
                    
                    self.logger.info(f"✅ Added retention policy to {table_name}: {config['retention_policy']}")
                    results[table_name] = True
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to add retention policy to {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    async def configure_continuous_aggregates(self) -> Dict[str, bool]:
        """Configure continuous aggregates for performance optimization"""
        results = {}
        
        if not await self.check_timescaledb_available():
            self.logger.warning("⚠️ TimescaleDB not available - skipping continuous aggregates")
            return results
        
        engine = await self._get_engine()
        
        for cagg_name, config in CONTINUOUS_AGGREGATES.items():
            try:
                async with engine.begin() as conn:
                    # Build aggregation query
                    group_by_clause = ", ".join(config['group_by'])
                    aggregates_clause = ", ".join(config['aggregates'])
                    
                    cagg_sql = f"""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {cagg_name}
                    WITH (timescaledb.continuous) AS
                    SELECT 
                        time_bucket(INTERVAL '{config['time_bucket']}', {config['source_table']}.{TIMESCALE_HYPERTABLES[config['source_table']]['time_column']}) AS time_bucket,
                        {group_by_clause},
                        {aggregates_clause}
                    FROM {config['source_table']}
                    GROUP BY time_bucket, {group_by_clause}
                    """
                    
                    await conn.execute(text(cagg_sql))
                    
                    self.logger.info(f"✅ Created continuous aggregate: {cagg_name}")
                    results[cagg_name] = True
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to create continuous aggregate {cagg_name}: {e}")
                results[cagg_name] = False
        
        return results
    
    async def optimize_database_settings(self) -> bool:
        """Apply TimescaleDB-specific database optimizations"""
        try:
            engine = await self._get_engine()
            
            optimizations = [
                "SET timescaledb.max_background_workers = 4",
                "SET shared_preload_libraries = 'timescaledb'",
                "SET max_worker_processes = 16",
                "SET max_parallel_workers_per_gather = 2",
                "SET work_mem = '256MB'",
                "SET maintenance_work_mem = '512MB'",
                "SET checkpoint_timeout = '10min'",
                "SET checkpoint_completion_target = 0.9",
                "SET wal_buffers = '16MB'",
                "SET default_statistics_target = 500",
            ]
            
            async with engine.begin() as conn:
                for setting in optimizations:
                    try:
                        await conn.execute(text(setting))
                        self.logger.info(f"✅ Applied: {setting}")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Could not apply: {setting} - {e}")
            
            self.logger.info("✅ Database optimizations applied")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to apply database optimizations: {e}")
            return False
    
    async def validate_setup(self) -> Dict[str, Any]:
        """Validate the complete TimescaleDB setup"""
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "timescaledb_available": False,
            "tables_created": {},
            "hypertables_configured": {},
            "retention_policies": {},
            "continuous_aggregates": {},
            "overall_status": "unknown"
        }
        
        try:
            # Check TimescaleDB availability
            validation_results["timescaledb_available"] = await self.check_timescaledb_available()
            
            # Check if tables exist
            engine = await self._get_engine()
            async with engine.begin() as conn:
                for table_name in TIMESCALE_HYPERTABLES.keys():
                    result = await conn.execute(text(f"""
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                    """))
                    validation_results["tables_created"][table_name] = result.fetchone() is not None
            
            # Check hypertables (if TimescaleDB available)
            if validation_results["timescaledb_available"]:
                async with engine.begin() as conn:
                    for table_name in TIMESCALE_HYPERTABLES.keys():
                        result = await conn.execute(text(f"""
                        SELECT 1 FROM timescaledb_information.hypertables 
                        WHERE hypertable_name = '{table_name}'
                        """))
                        validation_results["hypertables_configured"][table_name] = result.fetchone() is not None
            
            # Determine overall status
            all_tables_created = all(validation_results["tables_created"].values())
            
            if validation_results["timescaledb_available"]:
                all_hypertables_configured = all(validation_results["hypertables_configured"].values())
                validation_results["overall_status"] = "optimal" if all_tables_created and all_hypertables_configured else "partial"
            else:
                validation_results["overall_status"] = "basic" if all_tables_created else "incomplete"
            
        except Exception as e:
            self.logger.error(f"❌ Validation failed: {e}")
            validation_results["overall_status"] = "error"
            validation_results["error"] = str(e)
        
        return validation_results
    
    async def full_setup(self) -> Dict[str, Any]:
        """Run complete TimescaleDB setup process"""
        self.logger.info("🚀 Starting full TimescaleDB setup for Pulso Dashboard")
        
        setup_results = {
            "started_at": datetime.now().isoformat(),
            "steps": {}
        }
        
        # Step 1: Install TimescaleDB extension
        self.logger.info("📦 Step 1: Installing TimescaleDB extension...")
        setup_results["steps"]["extension_install"] = await self.install_timescaledb_extension()
        
        # Step 2: Run migrations
        self.logger.info("📋 Step 2: Running database migrations...")
        setup_results["steps"]["migrations"] = await self.run_migrations()
        
        # Step 3: Configure hypertables
        self.logger.info("🕒 Step 3: Configuring TimescaleDB hypertables...")
        setup_results["steps"]["hypertables"] = await self.configure_hypertables()
        
        # Step 4: Configure retention policies
        self.logger.info("🗑️ Step 4: Setting up retention policies...")
        setup_results["steps"]["retention_policies"] = await self.configure_retention_policies()
        
        # Step 5: Configure continuous aggregates
        self.logger.info("📊 Step 5: Creating continuous aggregates...")
        setup_results["steps"]["continuous_aggregates"] = await self.configure_continuous_aggregates()
        
        # Step 6: Validate setup
        self.logger.info("✅ Step 6: Validating setup...")
        setup_results["validation"] = await self.validate_setup()
        
        setup_results["completed_at"] = datetime.now().isoformat()
        
        # Summary
        overall_status = setup_results["validation"]["overall_status"]
        if overall_status == "optimal":
            self.logger.info("🎉 TimescaleDB setup completed successfully with optimal configuration!")
        elif overall_status == "basic":
            self.logger.info("✅ Basic PostgreSQL setup completed successfully")
            self.logger.info("💡 For optimal performance, consider installing TimescaleDB extension")
        else:
            self.logger.warning("⚠️ Setup completed with some issues - check logs for details")
        
        return setup_results
    
    async def cleanup(self):
        """Clean up database connections"""
        if self.engine:
            await self.engine.dispose()


# CLI Interface
async def main():
    """Main CLI interface for TimescaleDB setup"""
    
    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        print("💡 Example: DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/pulso_db")
        sys.exit(1)
    
    # Convert to async URL if needed
    if not database_url.startswith("postgresql+asyncpg://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
    
    setup = TimescaleDBSetup(database_url)
    
    try:
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == "check":
                # Just check TimescaleDB availability
                available = await setup.check_timescaledb_available()
                print(f"TimescaleDB available: {'✅ Yes' if available else '❌ No'}")
                
            elif command == "validate":
                # Validate current setup
                results = await setup.validate_setup()
                print("📋 Setup Validation Results:")
                print(f"Overall Status: {results['overall_status']}")
                print(f"TimescaleDB Available: {results['timescaledb_available']}")
                print(f"Tables Created: {sum(results['tables_created'].values())}/{len(results['tables_created'])}")
                
            elif command == "install":
                # Install TimescaleDB extension only
                success = await setup.install_timescaledb_extension()
                sys.exit(0 if success else 1)
                
            else:
                print(f"Unknown command: {command}")
                print("Available commands: check, validate, install, setup")
                sys.exit(1)
        else:
            # Full setup
            results = await setup.full_setup()
            
            # Print summary
            print("\n" + "="*60)
            print("📊 SETUP SUMMARY")
            print("="*60)
            print(f"Overall Status: {results['validation']['overall_status']}")
            print(f"TimescaleDB Available: {results['validation']['timescaledb_available']}")
            
            if results["validation"]["timescaledb_available"]:
                print("🎯 Ready for production with TimescaleDB optimization!")
            else:
                print("📝 Running on standard PostgreSQL (still production-ready)")
        
    finally:
        await setup.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
