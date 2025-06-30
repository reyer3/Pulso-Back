"""
🚀 ETL HTTP API Endpoints
Production-ready HTTP interface for incremental extractions

Features:
- Trigger dashboard refresh from frontend
- Monitor extraction status and progress
- Manual table refresh capabilities
- Comprehensive ETL health monitoring
- Cancel running extractions
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from etl import (
    trigger_table_refresh,
    get_etl_status,
    get_pipeline
)
from etl.config import ETLConfig
from etl import get_watermark_manager
from app.core.logging import LoggerMixin
from app.models.base import success_response


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class RefreshRequest(BaseModel):
    """Request model for refresh operations"""
    force: bool = Field(default=False, description="Force refresh even if recently updated")
    tables: Optional[List[str]] = Field(default=None, description="Specific tables to refresh (None = all dashboard tables)")
    max_concurrent: int = Field(default=2, description="Maximum concurrent extractions")


class RefreshResponse(BaseModel):
    """Response model for refresh operations"""
    pipeline_id: str
    status: str
    tables_requested: List[str]
    started_at: datetime
    estimated_duration_minutes: int = Field(default=5)


class ETLStatusResponse(BaseModel):
    """Response model for ETL status"""
    status: str
    last_updated: Optional[datetime]
    total_tables: int
    successful_tables: int
    failed_tables: int
    running_extractions: int
    dashboard_tables: List[str]
    recent_activity: List[Dict[str, Any]]


class TableStatusResponse(BaseModel):
    """Response model for individual table status"""
    table_name: str
    status: str
    last_extracted: Optional[datetime]
    records_count: int
    extraction_duration_seconds: float
    next_scheduled: Optional[datetime]
    error_message: Optional[str]


class CancelResponse(BaseModel):
    """Response model for cancellation operations"""
    cancelled_extractions: int
    cleaned_up_tasks: int
    status: str
    timestamp: datetime


class CampaignCatchUpSummary(BaseModel):
    status: str
    campaigns_total: int
    campaigns_processed: int
    campaigns_successful: int
    campaigns_partial: int
    campaigns_failed: int
    records_loaded_by_table: Dict[str, int]
    duration_seconds: float
    campaigns_per_minute: float

class CampaignCatchUpResponse(BaseModel):
    message: str
    status: str
    details: Optional[CampaignCatchUpSummary] = None

class CampaignCatchUpTriggerResponse(BaseModel):
    message: str
    status: str
    timestamp: datetime
# =============================================================================
# ROUTER SETUP - FIXED: Remove duplicate prefix
# =============================================================================

router = APIRouter(prefix="/etl", tags=["etl"])


class ETLAPI(LoggerMixin):
    """
    ETL API endpoints for production incremental extraction system
    
    This is the main HTTP entrypoint that the frontend calls to:
    - Trigger dashboard data refresh
    - Monitor extraction status
    - Manage individual table extractions
    - Cancel running extractions
    """
    
    # =============================================================================
    # MAIN ETL ENDPOINTS
    # =============================================================================
    
    @staticmethod
    @router.post("/refresh/dashboard", response_model=RefreshResponse)
    async def refresh_dashboard_data(
        request: RefreshRequest,
        background_tasks: BackgroundTasks
    ) -> RefreshResponse:
        """
        🎯 MAIN ENDPOINT: Trigger dashboard data refresh
        
        This is the primary endpoint called by the frontend to refresh
        dashboard data incrementally. Runs in background for better UX.
        
        Args:
            request: Refresh configuration
            background_tasks: FastAPI background tasks
            
        Returns:
            RefreshResponse with pipeline tracking info
        """
        try:
            # Determine tables to refresh
            tables_to_refresh = request.tables or ETLConfig.get_dashboard_tables()
            
            # Generate pipeline tracking info
            pipeline_id = f"dashboard_refresh_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Add background task for actual refresh
            background_tasks.add_task(
                _execute_dashboard_refresh,
                force=request.force,
                tables=tables_to_refresh,
                max_concurrent=request.max_concurrent
            )
            
            return RefreshResponse(
                pipeline_id=pipeline_id,
                status="started",
                tables_requested=tables_to_refresh,
                started_at=datetime.now(),
                estimated_duration_minutes=len(tables_to_refresh) * 2  # Rough estimate
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to start dashboard refresh: {str(e)}"
            )
    
    @staticmethod
    @router.post("/refresh/table/{table_name}")
    async def refresh_single_table(
        table_name: str,
        background_tasks: BackgroundTasks,
        force: bool = Query(default=False, description="Force refresh even if recent")
    ):
        """
        Refresh a specific table
        
        Useful for targeted updates or troubleshooting specific data marts.
        """
        try:
            # Validate table exists in configuration
            if table_name not in ETLConfig.list_tables():
                raise HTTPException(
                    status_code=404,
                    detail=f"Table {table_name} not found in ETL configuration"
                )
            
            # Add background task
            background_tasks.add_task(
                _execute_table_refresh,
                table_name=table_name,
                force=force
            )
            
            return success_response(
                message=f"Refresh started for table {table_name}",
                data={
                    "table_name": table_name,
                    "force": force,
                    "status": "started",
                    "started_at": datetime.now().isoformat()
                }
            )
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to refresh table {table_name}: {str(e)}"
            )
    
    # =============================================================================
    # 🛑 NEW: CANCELLATION AND CONTROL ENDPOINTS
    # =============================================================================
    
    @staticmethod
    @router.post("/cancel", response_model=CancelResponse)
    async def cancel_running_extractions():
        """
        🛑 Cancel all running ETL extractions
        
        Forcefully stops all running extractions and cleans up resources.
        Useful when extractions are stuck or need to be stopped urgently.
        
        Returns:
            CancelResponse with details of what was cancelled
        """
        try:
            pipeline = get_pipeline()
            
            # Cancel running extractions
            cancelled_count = 0
            cleaned_count = 0
            
            # Try to cancel through pipeline if available
            if hasattr(pipeline, 'cancel_all_extractions'):
                result = await pipeline.cancel_all_extractions()
                cancelled_count = result.get('cancelled_extractions', 0)
                cleaned_count = result.get('cleaned_tasks', 0)
            else:
                # Fallback: Cleanup approach
                cleanup_result = await pipeline.cleanup_and_recover()
                cleaned_count = cleanup_result.get('cleaned_tasks', 0)
            
            # Update watermarks to mark cancelled extractions as failed
            watermark_manager = await get_watermark_manager()
            
            # Get all tables with running status and mark them as cancelled
            all_watermarks = await watermark_manager.get_all_watermarks()
            running_tables = [w for w in all_watermarks if w.last_extraction_status in ['running', 'started']]
            
            for watermark in running_tables:
                await watermark_manager.update_watermark(
                    table_name=watermark.table_name,
                    timestamp=datetime.now(timezone.utc),
                    records_extracted=0,
                    status='cancelled',
                    error_message='Manually cancelled via API'
                )
                cancelled_count += 1
            
            return CancelResponse(
                cancelled_extractions=cancelled_count,
                cleaned_up_tasks=cleaned_count,
                status="cancelled" if cancelled_count > 0 else "no_running_extractions",
                timestamp=datetime.now()
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to cancel extractions: {str(e)}"
            )
    
    @staticmethod
    @router.post("/cancel/table/{table_name}")
    async def cancel_table_extraction(table_name: str):
        """
        🛑 Cancel extraction for a specific table
        
        Stops the extraction for a single table if it's currently running.
        
        Args:
            table_name: Name of the table to cancel
            
        Returns:
            Status of the cancellation
        """
        try:
            # Validate table exists
            if table_name not in ETLConfig.list_tables():
                raise HTTPException(
                    status_code=404,
                    detail=f"Table {table_name} not found in ETL configuration"
                )
            
            # Update watermark to mark as cancelled
            watermark_manager = await get_watermark_manager()
            watermark = await watermark_manager.get_watermark(table_name)
            
            if watermark and watermark.last_extraction_status in ['running', 'started']:
                await watermark_manager.update_watermark(
                    table_name=table_name,
                    timestamp=datetime.now(timezone.utc),
                    records_extracted=0,
                    status='cancelled',
                    error_message=f'Manually cancelled via API at {datetime.now()}'
                )
                
                return success_response(
                    message=f"Cancelled extraction for table {table_name}",
                    data={
                        "table_name": table_name,
                        "previous_status": watermark.last_extraction_status,
                        "cancelled_at": datetime.now().isoformat()
                    }
                )
            else:
                return success_response(
                    message=f"Table {table_name} was not running",
                    data={
                        "table_name": table_name,
                        "status": watermark.last_extraction_status if watermark else "never_run"
                    }
                )
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to cancel table extraction: {str(e)}"
            )
    
    @staticmethod
    @router.post("/reset")
    async def reset_etl_system():
        """
        🔄 Reset ETL system state
        
        Clears all watermarks and resets the system to initial state.
        WARNING: This will cause all tables to be fully refreshed on next run.
        
        Returns:
            Reset confirmation
        """
        try:
            watermark_manager = await get_watermark_manager()
            
            # Get all watermarks before reset
            all_watermarks = await watermark_manager.get_all_watermarks()
            reset_count = len(all_watermarks)
            
            # Use a fixed early date to force full refresh on next extraction
            reset_timestamp = datetime(2020, 1, 1, tzinfo=timezone.utc)
            
            # Reset all watermarks to force full refresh
            for watermark in all_watermarks:
                await watermark_manager.reset_watermark(
                    table_name=watermark.table_name,
                    timestamp=reset_timestamp  # ✅ FIXED: Include required timestamp argument
                )
            
            return success_response(
                message="ETL system reset completed",
                data={
                    "reset_tables": reset_count,
                    "reset_at": datetime.now().isoformat(),
                    "reset_timestamp": reset_timestamp.isoformat(),
                    "warning": "All tables will be fully refreshed on next extraction"
                }
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to reset ETL system: {str(e)}"
            )
    
    # =============================================================================
    # STATUS AND MONITORING ENDPOINTS
    # =============================================================================
    
    @staticmethod
    @router.get("/status", response_model=ETLStatusResponse)
    async def get_etl_system_status():
        """
        Get comprehensive ETL system status
        
        Returns current state of all extractions, watermarks, and system health.
        Used by the frontend to show ETL monitoring dashboard.
        """
        try:
            # Get comprehensive status from pipeline
            status_data = await get_etl_status()
            
            # Extract summary information
            summary = status_data.get("summary", {})
            
            # Get recent activity from watermarks
            watermark_manager = await get_watermark_manager()
            all_watermarks = await watermark_manager.get_all_watermarks()
            
            # Sort by last update for recent activity
            recent_activity = []
            for watermark in sorted(all_watermarks, key=lambda w: w.updated_at or datetime.min, reverse=True)[:10]:
                recent_activity.append({
                    "table_name": watermark.table_name,
                    "status": watermark.last_extraction_status,
                    "timestamp": watermark.last_extracted_at.isoformat() if watermark.last_extracted_at else None,
                    "records": watermark.records_extracted,
                    "duration": watermark.extraction_duration_seconds
                })
            
            return ETLStatusResponse(
                status="healthy" if summary.get("failed_tables", 0) == 0 else "degraded",
                last_updated=datetime.now(),
                total_tables=summary.get("total_tables", 0),
                successful_tables=summary.get("successful_tables", 0),
                failed_tables=summary.get("failed_tables", 0),
                running_extractions=summary.get("running_tables", 0),
                dashboard_tables=ETLConfig.get_dashboard_tables(),
                recent_activity=recent_activity
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get ETL status: {str(e)}"
            )
    
    @staticmethod
    @router.get("/status/table/{table_name}", response_model=TableStatusResponse)
    async def get_table_status(table_name: str):
        """
        Get detailed status for a specific table
        
        Provides granular information about extraction history, performance,
        and next scheduled refresh for a single table.
        """
        try:
            # Validate table exists
            if table_name not in ETLConfig.list_tables():
                raise HTTPException(
                    status_code=404,
                    detail=f"Table {table_name} not found in ETL configuration"
                )
            
            # Get watermark info
            watermark_manager = await get_watermark_manager()
            watermark = await watermark_manager.get_watermark(table_name)
            
            # Get table configuration
            config = ETLConfig.get_config(table_name)
            
            # Calculate next scheduled refresh
            next_scheduled = None
            if watermark and watermark.last_extracted_at:
                from datetime import timedelta
                next_scheduled = watermark.last_extracted_at + timedelta(
                    hours=config.refresh_frequency_hours
                )
            
            return TableStatusResponse(
                table_name=table_name,
                status=watermark.last_extraction_status if watermark else "never",
                last_extracted=watermark.last_extracted_at if watermark else None,
                records_count=watermark.records_extracted if watermark else 0,
                extraction_duration_seconds=watermark.extraction_duration_seconds if watermark else 0,
                next_scheduled=next_scheduled,
                error_message=watermark.error_message if watermark else None
            )
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get table status: {str(e)}"
            )

    # =============================================================================
    # 🧠 INTELLIGENT COORDINATOR ENDPOINTS
    # =============================================================================

    @staticmethod
    @router.post(
        "/catch-up/campaigns",
        response_model=CampaignCatchUpTriggerResponse,
        summary="Run Calendar-Driven ETL Catch-up"
    )
    async def trigger_campaign_catch_up(
            background_tasks: BackgroundTasks,
            force_refresh: bool = Query(False, description="Force reload of all campaign data"),
            batch_size: int = Query(5, description="Number of campaigns to process in parallel"),
            max_campaigns: Optional[int] = Query(None, description="Limit total campaigns to process")
    ):
        """
        🧠 Trigger the intelligent, calendar-driven ETL process.

        This is a powerful, long-running process that will:
        1. Read the `raw_calendario` table for campaign windows.
        2. For each campaign, load only the relevant data chunks from BigQuery.
        3. This process runs in the background. Check logs for progress.
        """
        try:
            # Aquí delegamos el trabajo pesado a una tarea en segundo plano.
            # El patrón es idéntico al de `refresh_dashboard_data`.
            background_tasks.add_task(
                _execute_campaign_catch_up,
                force_refresh=force_refresh,
                batch_size=batch_size,
                max_campaigns=max_campaigns
            )

            return CampaignCatchUpTriggerResponse(
                message="Campaign catch-up process started in the background.",
                status="started",
                timestamp=datetime.now()
            )

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to start campaign catch-up coordinator: {str(e)}"
            )
    # =============================================================================
    # CONFIGURATION AND METADATA ENDPOINTS
    # =============================================================================
    
    @staticmethod
    @router.get("/config/tables")
    async def get_configured_tables():
        """
        Get list of all configured tables with their settings
        
        Returns table configuration metadata for frontend display.
        """
        try:
            tables_info = []
            
            for table_name in ETLConfig.list_tables():
                config = ETLConfig.get_config(table_name)
                tables_info.append({
                    "table_name": table_name,
                    "table_type": config.table_type.value,
                    "description": config.description,
                    "primary_key": config.primary_key,
                    "incremental_column": config.incremental_column,
                    "lookback_days": config.lookback_days,
                    "refresh_frequency_hours": config.refresh_frequency_hours,
                    "batch_size": config.batch_size
                })
            
            return success_response(
                data={
                    "total_tables": len(tables_info),
                    "dashboard_tables": ETLConfig.get_dashboard_tables(),
                    "tables": tables_info
                }
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get table configuration: {str(e)}"
            )
    
    @staticmethod
    @router.get("/config/extraction-modes")
    async def get_extraction_modes():
        """Get available extraction modes"""
        from etl.config import ExtractionMode
        
        return success_response(
            data={
                "modes": [
                    {
                        "value": mode.value,
                        "label": mode.value.replace("_", " ").title(),
                        "description": _get_mode_description(mode)
                    }
                    for mode in ExtractionMode
                ]
            }
        )
    
    # =============================================================================
    # OPERATIONAL ENDPOINTS
    # =============================================================================
    
    @staticmethod
    @router.post("/cleanup")
    async def cleanup_failed_extractions():
        """
        Clean up failed/stale extractions and attempt recovery
        
        Useful for maintenance and recovery from system issues.
        """
        try:
            pipeline = get_pipeline()
            cleanup_result = await pipeline.cleanup_and_recover()
            
            return success_response(
                message="Cleanup and recovery completed",
                data=cleanup_result
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to cleanup extractions: {str(e)}"
            )
    
    @staticmethod
    @router.get("/health")
    async def etl_health_check():
        """
        ETL system health check
        
        Quick health check for monitoring systems.
        """
        try:
            # Check if watermark system is accessible
            watermark_manager = await get_watermark_manager()
            summary = await watermark_manager.get_extraction_summary()
            
            # Simple health logic
            failed_count = summary.get("failed_tables", 0)
            total_count = summary.get("total_tables", 0)
            
            if total_count == 0:
                status = "initializing"
            elif failed_count == 0:
                status = "healthy"
            elif failed_count < total_count / 2:
                status = "degraded"
            else:
                status = "unhealthy"
            
            return {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total_tables": total_count,
                    "failed_tables": failed_count,
                    "success_rate": round((total_count - failed_count) / max(total_count, 1) * 100, 2)
                }
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }


# =============================================================================
# BACKGROUND TASK FUNCTIONS
# =============================================================================
# Updated import for the new CampaignCatchUpPipeline
from etl import get_campaign_catchup_pipeline

async def _execute_dashboard_refresh(
    force: bool = False, 
    tables: Optional[List[str]] = None,
    max_concurrent: int = 2
):
    """Background task for dashboard refresh"""
    try:
        pipeline = get_pipeline()
        
        if tables:
            # Refresh specific tables
            from etl.config import ExtractionMode
            result = await pipeline.run_incremental_pipeline(
                table_names=tables,
                mode=ExtractionMode.INCREMENTAL,
                force=force,
                max_concurrent=max_concurrent
            )
        else:
            # Use standard dashboard refresh
            result = await pipeline.run_dashboard_refresh(force=force)
        
        # Log result
        if result.status == "success":
            print(f"✅ Dashboard refresh completed: {len(result.tables_processed)} tables")
        else:
            print(f"❌ Dashboard refresh failed: {result.error_message}")
            
    except Exception as e:
        print(f"❌ Dashboard refresh error: {str(e)}")


async def _execute_table_refresh(table_name: str, force: bool = False):
    """Background task for single table refresh"""
    try:
        result_dict = await trigger_table_refresh(table_name, force)
        
        # Log result
        if result_dict.get("result", {}).get("status") == "success":
            records = result_dict.get("result", {}).get("total_records", 0)
            print(f"✅ Table {table_name} refreshed: {records} records")
        else:
            error = result_dict.get("result", {}).get("error_message", "Unknown error")
            print(f"❌ Table {table_name} refresh failed: {error}")
            
    except Exception as e:
        print(f"❌ Table {table_name} refresh error: {str(e)}")


def _get_mode_description(mode) -> str:
    """Get human-readable description for extraction mode"""
    descriptions = {
        "incremental": "Extract only new or changed data since last extraction",
        "full_refresh": "Extract complete table data (slower but comprehensive)",
        "sliding_window": "Re-process data within a sliding time window"
    }
    return descriptions.get(mode.value, "Unknown mode")


async def _execute_campaign_catch_up(
        force_refresh: bool,
        batch_size: int,
        max_campaigns: Optional[int]
):
    """Background task for calendar-driven campaign catch-up."""
    print("🚀 Starting intelligent campaign catch-up...")
    try:
        # 1. Get the pipeline instance
        pipeline = await get_campaign_catchup_pipeline() # Use the new getter

        # 2. Execute the main logic
        summary = await pipeline.run_all_pending_campaigns( # Call the new method
            force_refresh_all=force_refresh, # Parameter name might change
            batch_size=batch_size,
            max_campaigns=max_campaigns
        )

        # 3. Log the result
        print(f"✅ Campaign catch-up completed. Status: {summary.get('status')}")
        print(f"📊 Summary: {summary}")

    except Exception as e:
        print(f"❌ Campaign catch-up process failed: {str(e)}")

@router.post("/catch-up/campaigns/cancel", status_code=202)
async def cancel_campaign_catch_up():
    """
    🛑 Signals the running campaign catch-up process to stop gracefully.
    The process will finish its current batch and then exit.
    """
    pipeline = await get_campaign_catchup_pipeline() # Use the new getter
    pipeline.cancel_processing() # Call the new cancel method
    return {"message": "Cancellation signal sent. Process will stop after the current batch."}

# =============================================================================
# REGISTER API ROUTES
# =============================================================================

# Create API instance to register methods
api = ETLAPI()

# The router is already configured with the endpoints above
# This will be imported in main.py and included in the FastAPI app
