"""
🎯 Watermark System for Incremental Extractions - asyncpg Pure
Production-ready tracking of extraction state without SQLAlchemy overhead

Manages watermarks (last extraction timestamps) for each table,
ensuring reliable incremental processing and recovery from failures.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import json
import logging
from dataclasses import dataclass

from shared.database.connection import get_database_manager, execute_query, DatabaseManager


@dataclass
class WatermarkInfo:
    """Information about a table's extraction watermark"""
    table_name: str
    last_extracted_at: datetime
    last_extraction_status: str  # 'success', 'failed', 'running'
    records_extracted: int
    extraction_duration_seconds: float
    error_message: Optional[str] = None
    extraction_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class WatermarkManager:
    """
    Manages extraction watermarks for incremental ETL - asyncpg Pure
    
    Features:
    - Pure asyncpg for maximum performance
    - Atomic watermark updates
    - Recovery from failed extractions
    - Extraction metadata tracking
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    async def get_db(self) -> DatabaseManager:
        """Get database manager instance"""
        if self.db_manager is None:
            self.db_manager = await get_database_manager()
        return self.db_manager
    
    async def ensure_watermark_table(self) -> None:
        """Create watermark table if it doesn't exist"""
        # Check if table exists first
        db = await self.get_db()
        table_exists = await db.table_exists("etl_watermarks")
        
        if table_exists:
            self.logger.info("✅ Watermark table already exists")
            return
        
        # Create table with all constraints and indexes
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS etl_watermarks (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL UNIQUE,
            last_extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            last_extraction_status VARCHAR(20) NOT NULL DEFAULT 'success',
            records_extracted INTEGER DEFAULT 0,
            extraction_duration_seconds FLOAT DEFAULT 0.0,
            error_message TEXT,
            extraction_id VARCHAR(50),
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_etl_watermarks_table_name 
            ON etl_watermarks(table_name);
        CREATE INDEX IF NOT EXISTS idx_etl_watermarks_status 
            ON etl_watermarks(last_extraction_status);
        CREATE INDEX IF NOT EXISTS idx_etl_watermarks_updated 
            ON etl_watermarks(updated_at);
        """
        
        await execute_query(create_table_sql)
        self.logger.info("✅ Watermark table created with indexes")
    
    async def get_watermark(self, table_name: str) -> Optional[WatermarkInfo]:
        """Get current watermark for a table"""
        query = """
        SELECT 
            table_name,
            last_extracted_at,
            last_extraction_status,
            records_extracted,
            extraction_duration_seconds,
            error_message,
            extraction_id,
            created_at,
            updated_at
        FROM etl_watermarks 
        WHERE table_name = $1
        """
        
        row = await execute_query(query, table_name, fetch="one")
        
        if row:
            return WatermarkInfo(**dict(row))
        return None
    
    async def get_last_extraction_time(self, table_name: str) -> Optional[datetime]:
        """Get just the last extraction timestamp for a table"""
        watermark = await self.get_watermark(table_name)
        return watermark.last_extracted_at if watermark else None
    
    async def start_extraction(self, table_name: str, extraction_id: str) -> None:
        """Mark extraction as started (for monitoring)"""
        upsert_sql = """
        INSERT INTO etl_watermarks (
            table_name, 
            last_extracted_at, 
            last_extraction_status,
            extraction_id,
            updated_at
        ) VALUES (
            $1, $2, 'running', $3, CURRENT_TIMESTAMP
        )
        ON CONFLICT (table_name) 
        DO UPDATE SET 
            last_extraction_status = 'running',
            extraction_id = $3,
            updated_at = CURRENT_TIMESTAMP
        """
        
        await execute_query(
            upsert_sql, 
            table_name, 
            datetime.now(timezone.utc), 
            extraction_id
        )
        
        self.logger.info(f"🏁 Started extraction for {table_name} (ID: {extraction_id})")
    
    async def update_watermark(
        self, 
        table_name: str, 
        timestamp: datetime,
        records_extracted: int = 0,
        extraction_duration_seconds: float = 0.0,
        status: str = "success",
        error_message: Optional[str] = None,
        extraction_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Update watermark for successful or failed extraction"""
        
        upsert_sql = """
        INSERT INTO etl_watermarks (
            table_name, 
            last_extracted_at, 
            last_extraction_status,
            records_extracted,
            extraction_duration_seconds,
            error_message,
            extraction_id,
            metadata,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        ON CONFLICT (table_name) 
        DO UPDATE SET 
            last_extracted_at = $2,
            last_extraction_status = $3,
            records_extracted = $4,
            extraction_duration_seconds = $5,
            error_message = $6,
            extraction_id = $7,
            metadata = COALESCE($8, etl_watermarks.metadata),
            updated_at = CURRENT_TIMESTAMP
        """
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        await execute_query(
            upsert_sql,
            table_name,
            timestamp,
            status,
            records_extracted,
            extraction_duration_seconds,
            error_message,
            extraction_id,
            metadata_json
        )
        
        status_emoji = "✅" if status == "success" else "❌" if status == "failed" else "🔄"
        self.logger.info(
            f"{status_emoji} Updated watermark for {table_name}: {timestamp} "
            f"({records_extracted} records, {status})"
        )
    
    async def get_failed_extractions(self) -> List[WatermarkInfo]:
        """Get all tables with failed last extraction"""
        query = """
        SELECT 
            table_name,
            last_extracted_at,
            last_extraction_status,
            records_extracted,
            extraction_duration_seconds,
            error_message,
            extraction_id,
            created_at,
            updated_at
        FROM etl_watermarks 
        WHERE last_extraction_status = 'failed'
        ORDER BY updated_at DESC
        """
        
        rows = await execute_query(query, fetch="all")
        return [WatermarkInfo(**dict(row)) for row in rows]
    
    async def get_running_extractions(self) -> List[WatermarkInfo]:
        """Get all extractions currently marked as running"""
        query = """
        SELECT 
            table_name,
            last_extracted_at,
            last_extraction_status,
            records_extracted,
            extraction_duration_seconds,
            error_message,
            extraction_id,
            created_at,
            updated_at
        FROM etl_watermarks 
        WHERE last_extraction_status = 'running'
        ORDER BY updated_at DESC
        """
        
        rows = await execute_query(query, fetch="all")
        return [WatermarkInfo(**dict(row)) for row in rows]
    
    async def cleanup_stale_extractions(self, timeout_minutes: int = 30) -> int:
        """
        Clean up extractions that have been running too long
        
        Returns number of stale extractions cleaned up
        """
        cleanup_sql = """
        UPDATE etl_watermarks 
        SET 
            last_extraction_status = 'failed',
            error_message = 'Extraction timed out - marked as failed by cleanup',
            updated_at = CURRENT_TIMESTAMP
        WHERE 
            last_extraction_status = 'running'
            AND updated_at < CURRENT_TIMESTAMP - INTERVAL '%s minutes'
        RETURNING table_name
        """
        
        cleaned_tables = await execute_query(
            cleanup_sql % timeout_minutes, 
            fetch="all"
        )
        
        cleaned_count = len(cleaned_tables)
        
        if cleaned_count > 0:
            table_names = [row['table_name'] for row in cleaned_tables]
            self.logger.warning(
                f"🧹 Cleaned up {cleaned_count} stale extractions: {table_names}"
            )
        
        return cleaned_count
    
    async def reset_watermark(self, table_name: str, timestamp: datetime) -> None:
        """Reset watermark to a specific timestamp (for manual recovery)"""
        await self.update_watermark(
            table_name=table_name,
            timestamp=timestamp,
            status="reset",
            records_extracted=0,
            error_message=f"Manually reset to {timestamp}"
        )
        
        self.logger.warning(f"🔄 Reset watermark for {table_name} to {timestamp}")
    
    async def delete_watermark(self, table_name: str) -> None:
        """Delete watermark for a table (careful!)"""
        delete_sql = "DELETE FROM etl_watermarks WHERE table_name = $1"
        
        result = await execute_query(delete_sql, table_name)
        
        # Extract row count from result string "DELETE n"
        row_count = int(result.split()[-1]) if result.startswith("DELETE") else 0
        
        if row_count > 0:
            self.logger.warning(f"🗑️ Deleted watermark for {table_name}")
        else:
            self.logger.info(f"ℹ️ No watermark found for {table_name}")
    
    async def get_all_watermarks(self) -> List[WatermarkInfo]:
        """Get all watermarks for monitoring dashboard"""
        query = """
        SELECT 
            table_name,
            last_extracted_at,
            last_extraction_status,
            records_extracted,
            extraction_duration_seconds,
            error_message,
            extraction_id,
            created_at,
            updated_at
        FROM etl_watermarks 
        ORDER BY table_name
        """
        
        rows = await execute_query(query, fetch="all")
        return [WatermarkInfo(**dict(row)) for row in rows]
    
    async def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary statistics for monitoring"""
        summary_sql = """
        SELECT 
            COUNT(*) as total_tables,
            COUNT(*) FILTER (WHERE last_extraction_status = 'success') as successful_tables,
            COUNT(*) FILTER (WHERE last_extraction_status = 'failed') as failed_tables,
            COUNT(*) FILTER (WHERE last_extraction_status = 'running') as running_tables,
            COALESCE(SUM(records_extracted), 0) as total_records_extracted,
            COALESCE(AVG(extraction_duration_seconds), 0) as avg_extraction_time,
            MAX(updated_at) as last_activity
        FROM etl_watermarks
        """
        
        row = await execute_query(summary_sql, fetch="one")
        
        if row:
            summary = dict(row)
            # Convert to proper types for JSON serialization
            summary['avg_extraction_time'] = float(summary['avg_extraction_time'])
            summary['total_records_extracted'] = int(summary['total_records_extracted'])
            return summary
        
        return {
            "total_tables": 0,
            "successful_tables": 0, 
            "failed_tables": 0,
            "running_tables": 0,
            "total_records_extracted": 0,
            "avg_extraction_time": 0.0,
            "last_activity": None
        }


# 🎯 Global watermark manager instance
_watermark_manager: Optional[WatermarkManager] = None


async def get_watermark_manager() -> WatermarkManager:
    """Get singleton watermark manager instance"""
    global _watermark_manager
    
    if _watermark_manager is None:
        _watermark_manager = WatermarkManager()
        await _watermark_manager.ensure_watermark_table()
    
    return _watermark_manager


# 🚀 Convenience functions for common operations
async def get_last_extracted(table_name: str) -> Optional[datetime]:
    """Quick access to last extraction time"""
    manager = await get_watermark_manager()
    return await manager.get_last_extraction_time(table_name)


async def mark_extraction_success(
    table_name: str, 
    records_count: int, 
    duration_seconds: float,
    extraction_id: Optional[str] = None
) -> None:
    """Mark extraction as successful"""
    manager = await get_watermark_manager()
    await manager.update_watermark(
        table_name=table_name,
        timestamp=datetime.now(timezone.utc),
        records_extracted=records_count,
        extraction_duration_seconds=duration_seconds,
        status="success",
        extraction_id=extraction_id
    )


async def mark_extraction_failed(
    table_name: str, 
    error_message: str,
    extraction_id: Optional[str] = None
) -> None:
    """Mark extraction as failed"""
    manager = await get_watermark_manager()
    await manager.update_watermark(
        table_name=table_name,
        timestamp=datetime.now(timezone.utc),
        status="failed",
        error_message=error_message,
        extraction_id=extraction_id
    )


async def watermark_health_check() -> Dict[str, Any]:
    """Health check for watermark system"""
    try:
        manager = await get_watermark_manager()
        summary = await manager.get_extraction_summary()
        
        return {
            "status": "healthy",
            "watermark_system": "operational",
            "total_tables_tracked": summary["total_tables"],
            "last_activity": summary["last_activity"].isoformat() if summary["last_activity"] else None
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "watermark_system": "failed"
        }
