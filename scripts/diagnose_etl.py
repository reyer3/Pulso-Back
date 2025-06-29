"""
🔧 Quick ETL Diagnostics Tool - Debug ETL Issues

This script helps diagnose common ETL pipeline issues
"""

import asyncio
import sys
from typing import Dict, Any

from app.etl.config import ETLConfig
from app.etl.transformers.unified_transformer import get_unified_transformer_registry
from app.etl.watermarks import get_watermark_manager
from app.core.database import get_database


async def diagnose_etl_system() -> Dict[str, Any]:
    """Run comprehensive ETL system diagnostics"""
    
    diagnostics = {
        "status": "running_diagnostics",
        "timestamp": "checking...",
        "issues": [],
        "recommendations": []
    }
    
    print("🔍 Starting ETL System Diagnostics...")
    
    try:
        # 1. Check table configuration
        print("\n1️⃣ Checking ETL Configuration...")
        configured_tables = ETLConfig.list_tables()
        dashboard_tables = ETLConfig.get_dashboard_tables()
        
        print(f"   ✅ {len(configured_tables)} tables configured")
        print(f"   ✅ {len(dashboard_tables)} dashboard tables")
        
        diagnostics["configured_tables"] = configured_tables
        diagnostics["dashboard_tables"] = dashboard_tables
        
        # 2. Check transformers
        print("\n2️⃣ Checking Transformer Registry...")
        try:
            transformer = get_unified_transformer_registry()
            supported_tables = transformer.get_supported_tables()
            transformation_stats = transformer.get_transformation_stats()
            
            print(f"   ✅ Transformer initialized")
            print(f"   ✅ {len(supported_tables)} tables supported by transformers")
            
            # Check which configured tables don't have transformers
            missing_transformers = set(configured_tables) - set(supported_tables)
            if missing_transformers:
                print(f"   ⚠️  Missing transformers for: {list(missing_transformers)}")
                diagnostics["issues"].append(f"Missing transformers for tables: {list(missing_transformers)}")
                diagnostics["recommendations"].append("Check transformer implementations for missing tables")
            
            diagnostics["transformer_status"] = "working"
            diagnostics["supported_tables"] = supported_tables
            diagnostics["transformation_stats"] = transformation_stats
            
        except Exception as e:
            print(f"   ❌ Transformer error: {str(e)}")
            diagnostics["issues"].append(f"Transformer initialization failed: {str(e)}")
            diagnostics["transformer_status"] = "failed"
        
        # 3. Check database connection
        print("\n3️⃣ Checking Database Connection...")
        try:
            db = get_database()
            async with db.get_connection() as conn:
                # Test basic query
                result = await conn.fetchval("SELECT 1")
                print(f"   ✅ Database connection working")
                
                # Check if tables exist
                tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name LIKE 'raw_%'
                ORDER BY table_name;
                """
                existing_tables = await conn.fetch(tables_query)
                existing_table_names = [row['table_name'] for row in existing_tables]
                
                print(f"   ✅ {len(existing_table_names)} raw tables exist in database")
                
                if existing_table_names:
                    print(f"   📋 Existing tables: {existing_table_names}")
                else:
                    print(f"   ⚠️  No raw tables found - migrations may not be applied")
                    diagnostics["issues"].append("No raw tables found in database")
                    diagnostics["recommendations"].append("Run 'yoyo apply' to apply migrations")
                
                diagnostics["database_status"] = "connected"
                diagnostics["existing_tables"] = existing_table_names
                
                # Check for hypertables (TimescaleDB)
                try:
                    hypertables_query = "SELECT hypertable_name FROM timescaledb_information.hypertables;"
                    hypertables = await conn.fetch(hypertables_query)
                    hypertable_names = [row['hypertable_name'] for row in hypertables]
                    
                    print(f"   ✅ {len(hypertable_names)} TimescaleDB hypertables found")
                    if hypertable_names:
                        print(f"   📋 Hypertables: {hypertable_names}")
                    
                    diagnostics["timescaledb_status"] = "working"
                    diagnostics["hypertables"] = hypertable_names
                    
                except Exception as e:
                    print(f"   ⚠️  TimescaleDB check failed: {str(e)}")
                    diagnostics["issues"].append("TimescaleDB extension may not be installed")
                    diagnostics["recommendations"].append("Install TimescaleDB extension")
                    diagnostics["timescaledb_status"] = "failed"
                
        except Exception as e:
            print(f"   ❌ Database connection failed: {str(e)}")
            diagnostics["issues"].append(f"Database connection failed: {str(e)}")
            diagnostics["database_status"] = "failed"
        
        # 4. Check watermark system
        print("\n4️⃣ Checking Watermark System...")
        try:
            watermark_manager = await get_watermark_manager()
            watermarks = await watermark_manager.get_all_watermarks()
            
            print(f"   ✅ Watermark system working")
            print(f"   ✅ {len(watermarks)} watermarks exist")
            
            # Check for recent activity
            recent_watermarks = [w for w in watermarks if w.last_extracted_at]
            if recent_watermarks:
                print(f"   ✅ {len(recent_watermarks)} tables have been extracted before")
            else:
                print(f"   ℹ️  No tables have been extracted yet (first run)")
            
            diagnostics["watermark_status"] = "working"
            diagnostics["total_watermarks"] = len(watermarks)
            diagnostics["tables_with_extractions"] = len(recent_watermarks)
            
        except Exception as e:
            print(f"   ❌ Watermark system error: {str(e)}")
            diagnostics["issues"].append(f"Watermark system failed: {str(e)}")
            diagnostics["watermark_status"] = "failed"
        
        # 5. Summary and recommendations
        print("\n📊 Diagnostic Summary:")
        if diagnostics["issues"]:
            print(f"   ⚠️  {len(diagnostics['issues'])} issues found:")
            for issue in diagnostics["issues"]:
                print(f"      - {issue}")
                
            print(f"\n💡 Recommendations:")
            for rec in diagnostics["recommendations"]:
                print(f"      - {rec}")
        else:
            print(f"   ✅ No major issues detected")
        
        diagnostics["status"] = "completed"
        return diagnostics
        
    except Exception as e:
        print(f"\n❌ Diagnostic failed: {str(e)}")
        diagnostics["status"] = "failed"
        diagnostics["error"] = str(e)
        return diagnostics


async def main():
    """Run diagnostics"""
    try:
        result = await diagnose_etl_system()
        print(f"\n🎯 Diagnostics Result: {result['status']}")
        return 0 if result['status'] == 'completed' else 1
    except Exception as e:
        print(f"❌ Diagnostic script failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
