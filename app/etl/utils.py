"""
🛠️ ETL Utilities
Production utilities for testing, debugging, and maintenance

Features:
- Quick testing of individual components
- Data validation and quality checks
- Performance benchmarking
- Manual recovery tools
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import json
import logging

from app.etl.config import ETLConfig, ExtractionMode
from app.etl.extractors.bigquery_extractor import BigQueryExtractor
from app.etl.loaders.postgres_loader import PostgresLoader
from app.etl.watermarks import get_watermark_manager
from app.etl.pipelines.extraction_pipeline import get_pipeline


class ETLTester:
    """Utility class for testing ETL components"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def test_bigquery_connection(self) -> Dict[str, Any]:
        """Test BigQuery connectivity and permissions"""
        try:
            extractor = BigQueryExtractor()
            
            # Test basic query
            test_result = await extractor.test_query("SELECT 1 as test, CURRENT_TIMESTAMP() as now")
            
            return {
                "status": "success",
                "connection": "OK",
                "permissions": "READ access confirmed",
                "test_query": test_result
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "suggestion": "Check Google Cloud credentials and BigQuery permissions"
            }
    
    async def test_postgres_connection(self) -> Dict[str, Any]:
        """Test PostgreSQL connectivity and permissions"""
        try:
            loader = PostgresLoader()
            
            # Test simple insert
            test_data = [{"test_id": 1, "test_name": "ETL Test", "test_timestamp": datetime.now()}]
            
            result = await loader.load_data_batch(
                table_name="etl_test_table",
                data=test_data,
                primary_key=["test_id"],
                upsert=True,
                create_table=True
            )
            
            return {
                "status": "success",
                "connection": "OK",
                "permissions": "READ/WRITE access confirmed",
                "test_load": result.__dict__
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "suggestion": "Check PostgreSQL connection string and permissions"
            }
    
    async def test_table_extraction(self, table_name: str) -> Dict[str, Any]:
        """Test extraction for a specific table"""
        try:
            extractor = BigQueryExtractor()
            
            # Test the configured query
            config = ETLConfig.get_config(table_name)
            base_query = ETLConfig.get_query(table_name)
            
            # Build query with small date range for testing
            test_date = datetime.now(timezone.utc) - timedelta(days=1)
            incremental_filter = f"{config.incremental_column} >= '{test_date.strftime('%Y-%m-%d')}'"
            test_query = base_query.replace("{incremental_filter}", incremental_filter)
            
            # Test query execution
            test_result = await extractor.test_query(test_query)
            
            return {
                "status": "success",
                "table_name": table_name,
                "query_test": test_result,
                "config": {
                    "primary_key": config.primary_key,
                    "incremental_column": config.incremental_column,
                    "batch_size": config.batch_size
                }
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "table_name": table_name,
                "error": str(e)
            }
    
    async def benchmark_table_extraction(self, table_name: str, days_back: int = 1) -> Dict[str, Any]:
        """Benchmark extraction performance for a table"""
        try:
            pipeline = get_pipeline()
            
            start_time = time.time()
            
            # Force extraction for benchmark
            load_result = await pipeline.extract_and_load_table(
                table_name=table_name,
                mode=ExtractionMode.INCREMENTAL,
                force=True
            )
            
            total_time = time.time() - start_time
            
            # Calculate performance metrics
            records_per_second = load_result.total_records / max(total_time, 0.1)
            
            return {
                "status": "success",
                "table_name": table_name,
                "performance": {
                    "total_records": load_result.total_records,
                    "total_time_seconds": round(total_time, 2),
                    "records_per_second": round(records_per_second, 2),
                    "load_time_seconds": load_result.load_duration_seconds
                },
                "result": load_result.__dict__
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "table_name": table_name,
                "error": str(e)
            }


class ETLValidator:
    """Data quality validation utilities"""
    
    async def validate_table_data(self, table_name: str) -> Dict[str, Any]:
        """Validate data quality for a table"""
        try:
            # Get table configuration
            config = ETLConfig.get_config(table_name)
            
            # Get watermark info
            watermark_manager = await get_watermark_manager()
            watermark = await watermark_manager.get_watermark(table_name)
            
            # Basic validations
            validations = {
                "table_configured": True,
                "has_watermark": watermark is not None,
                "recent_extraction": False,
                "expected_records": False,
                "primary_key_valid": len(config.primary_key) > 0
            }
            
            if watermark:
                # Check if extraction is recent
                hours_since_extraction = (datetime.now(timezone.utc) - watermark.last_extracted_at).total_seconds() / 3600
                validations["recent_extraction"] = hours_since_extraction < (config.refresh_frequency_hours * 2)
                
                # Check if we have expected minimum records
                validations["expected_records"] = watermark.records_extracted >= config.min_expected_records
            
            # Calculate overall health
            passed_validations = sum(1 for v in validations.values() if v)
            health_score = (passed_validations / len(validations)) * 100
            
            return {
                "table_name": table_name,
                "health_score": round(health_score, 2),
                "validations": validations,
                "last_extraction": {
                    "timestamp": watermark.last_extracted_at.isoformat() if watermark else None,
                    "status": watermark.last_extraction_status if watermark else "never",
                    "records": watermark.records_extracted if watermark else 0
                } if watermark else None,
                "recommendations": _get_health_recommendations(validations, config)
            }
            
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e),
                "health_score": 0
            }


class ETLRecovery:
    """Recovery and maintenance utilities"""
    
    async def reset_table_watermark(self, table_name: str, reset_to_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Reset watermark for a table (use with caution)"""
        try:
            watermark_manager = await get_watermark_manager()
            
            if reset_to_date is None:
                # Reset to 7 days ago by default
                reset_to_date = datetime.now(timezone.utc) - timedelta(days=7)
            
            await watermark_manager.reset_watermark(table_name, reset_to_date)
            
            return {
                "status": "success",
                "table_name": table_name,
                "reset_to": reset_to_date.isoformat(),
                "message": f"Watermark reset - next extraction will process data since {reset_to_date}"
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "table_name": table_name,
                "error": str(e)
            }
    
    async def force_full_refresh(self, table_name: str) -> Dict[str, Any]:
        """Force a full refresh of a table"""
        try:
            pipeline = get_pipeline()
            
            load_result = await pipeline.extract_and_load_table(
                table_name=table_name,
                mode=ExtractionMode.FULL_REFRESH,
                force=True
            )
            
            return {
                "status": "success",
                "table_name": table_name,
                "mode": "full_refresh",
                "result": load_result.__dict__
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "table_name": table_name,
                "error": str(e)
            }


def _get_health_recommendations(validations: Dict[str, bool], config) -> List[str]:
    """Get health recommendations based on validation results"""
    recommendations = []
    
    if not validations.get("has_watermark"):
        recommendations.append("Run initial extraction to establish watermark")
    
    if not validations.get("recent_extraction"):
        recommendations.append(f"Extraction overdue - should run every {config.refresh_frequency_hours} hours")
    
    if not validations.get("expected_records"):
        recommendations.append(f"Low record count - expected at least {config.min_expected_records} records")
    
    if not validations.get("primary_key_valid"):
        recommendations.append("Configure valid primary key for UPSERT operations")
    
    if not recommendations:
        recommendations.append("Table appears healthy - no immediate actions needed")
    
    return recommendations


# 🎯 Convenience functions for easy CLI usage

async def quick_test() -> Dict[str, Any]:
    """Quick test of all ETL components"""
    tester = ETLTester()
    
    results = {
        "bigquery": await tester.test_bigquery_connection(),
        "postgres": await tester.test_postgres_connection(),
        "configured_tables": len(ETLConfig.list_tables()),
        "dashboard_tables": ETLConfig.get_dashboard_tables()
    }
    
    return results


async def validate_all_tables() -> Dict[str, Any]:
    """Validate data quality for all configured tables"""
    validator = ETLValidator()
    
    results = {}
    overall_health = []
    
    for table_name in ETLConfig.list_tables():
        result = await validator.validate_table_data(table_name)
        results[table_name] = result
        overall_health.append(result.get("health_score", 0))
    
    # Calculate system-wide health
    system_health = sum(overall_health) / len(overall_health) if overall_health else 0
    
    return {
        "system_health_score": round(system_health, 2),
        "tables": results,
        "summary": {
            "total_tables": len(results),
            "healthy_tables": len([r for r in results.values() if r.get("health_score", 0) >= 80]),
            "degraded_tables": len([r for r in results.values() if 50 <= r.get("health_score", 0) < 80]),
            "unhealthy_tables": len([r for r in results.values() if r.get("health_score", 0) < 50])
        }
    }


async def emergency_recovery() -> Dict[str, Any]:
    """Emergency recovery for all failed extractions"""
    recovery = ETLRecovery()
    pipeline = get_pipeline()
    
    # Get cleanup results
    cleanup_result = await pipeline.cleanup_and_recover()
    
    # Additional recovery if needed
    recovery_results = []
    
    watermark_manager = await get_watermark_manager()
    failed_extractions = await watermark_manager.get_failed_extractions()
    
    for failed in failed_extractions:
        # Try to recover each failed table
        result = await recovery.force_full_refresh(failed.table_name)
        recovery_results.append(result)
    
    return {
        "cleanup": cleanup_result,
        "recovery_attempts": recovery_results,
        "total_recovered": len([r for r in recovery_results if r.get("status") == "success"])
    }


# 🔧 CLI-style functions for manual testing

if __name__ == "__main__":
    import asyncio
    
    async def main():
        print("🚀 ETL Utilities - Quick Test")
        print("=" * 50)
        
        # Quick test
        print("\n📋 Testing Connections...")
        test_results = await quick_test()
        print(json.dumps(test_results, indent=2, default=str))
        
        # Validate tables
        print("\n🔍 Validating Tables...")
        validation_results = await validate_all_tables()
        print(f"System Health: {validation_results['system_health_score']}%")
        print(f"Healthy Tables: {validation_results['summary']['healthy_tables']}")
        print(f"Total Tables: {validation_results['summary']['total_tables']}")
    
    asyncio.run(main())
