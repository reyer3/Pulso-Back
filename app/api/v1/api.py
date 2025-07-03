"""
🔌 API V1 Router
Main API router that includes all endpoint modules
"""

from fastapi import APIRouter

from app.api.v1.endpoints.dashboard import router as dashboard_router
from app.api.v1.endpoints.evolution import router as evolution_router
from app.api.v1.endpoints.assignment import router as assignment_router
from app.api.v1.endpoints.operation import router as operation_router
from app.api.v1.endpoints.productivity import router as productivity_router
from app.api.v1.endpoints.etl import router as etl_router
from app.api.v1.endpoints.users import router as users_router

# Create main API router
api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(dashboard_router, tags=["dashboard"])
api_router.include_router(evolution_router, tags=["evolution"])
api_router.include_router(assignment_router, tags=["assignment"])
api_router.include_router(operation_router, tags=["operation"])
api_router.include_router(productivity_router, tags=["productivity"])
api_router.include_router(etl_router, tags=["etl"])
api_router.include_router(users_router, tags=["users"])

# Add health check endpoint at API level
@api_router.get("/health")
async def api_health():
    """
    API-level health check
    """
    return {
        "status": "healthy",
        "api_version": "v1",
        "endpoints": {
            "dashboard": "/dashboard - Dashboard principal con KPIs ejecutivos",
            "evolution": "/evolution - Evolutivos diarios y trending de KPIs", 
            "assignment": "/assignment - Análisis de composición de cartera",
            "operation": "/operation - Análisis operativo diario del call center",
            "productivity": "/productivity - Análisis de productividad de agentes",
            "etl": "/etl - Sistema de extracción incremental de datos",
            "users": "/users - Gestión de usuarios y permisos"
        },
        "features": {
            "database_agnostic": "Preparado para migración BigQuery → PostgreSQL",
            "incremental_etl": "Extracciones incrementales automáticas",
            "caching": "Redis cache integrado para performance",
            "filtering": "Filtros por cartera, servicio, fechas",
            "real_time": "Datos con refresh bajo demanda",
            "agent_tracking": "Monitoreo de productividad por agente",
            "user_management": "Sistema completo de gestión de usuarios"
        },
        "openapi_docs": "/docs"
    }


@api_router.get("/info")
async def api_info():
    """
    API information and capabilities
    """
    return {
        "name": "Pulso-Back API",
        "version": "1.0.0",
        "description": "API + ETL Backend para Dashboard Cobranzas Telefónica",
        "architecture": {
            "pattern": "API-First with Repository Pattern + Incremental ETL",
            "database": "BigQuery → PostgreSQL (incremental migration)",
            "cache": "Redis",
            "processing": "Python pandas + numpy",
            "etl": "Production-ready incremental extraction system",
            "auth": "JWT-based authentication with role-based access control"
        },
        "endpoints": {
            "/api/v1/dashboard": {
                "description": "Dashboard principal con métricas ejecutivas",
                "methods": ["GET", "POST"],
                "features": ["filtros", "agregaciones", "cache"]
            },
            "/api/v1/evolution": {
                "description": "Evolutivos diarios de KPIs por día de gestión",
                "methods": ["GET", "POST"],
                "features": ["date_range", "metrics_selection", "cartera_filter"]
            },
            "/api/v1/assignment": {
                "description": "Análisis de composición y asignación de carteras",
                "methods": ["GET", "POST"],
                "features": ["period_comparison", "composition_breakdown", "executive_kpis"]
            },
            "/api/v1/operation": {
                "description": "Análisis operativo diario del call center",
                "methods": ["GET"],
                "features": ["hourly_breakdown", "channel_comparison", "queue_performance"]
            },
            "/api/v1/productivity": {
                "description": "Análisis de productividad de agentes y performance",
                "methods": ["GET", "POST"],
                "features": ["agent_ranking", "daily_trends", "hourly_patterns", "performance_heatmap"]
            },
            "/api/v1/etl": {
                "description": "Sistema de extracción incremental de datos",
                "methods": ["GET", "POST"],
                "features": ["dashboard_refresh", "table_monitoring", "watermark_tracking", "background_processing"]
            },
            "/api/v1/users": {
                "description": "Gestión completa de usuarios y permisos",
                "methods": ["GET", "POST", "PUT", "DELETE"],
                "features": [
                    "crud_operations", "role_management", "permission_control",
                    "bulk_operations", "user_statistics", "profile_management",
                    "password_security", "audit_logging", "export_functionality"
                ]
            }
        },
        "data_sources": {
            "asignaciones": "Cuentas asignadas por cartera",
            "tran_deuda": "Transacciones de deuda evolutiva", 
            "gestiones_bot": "Gestiones automatizadas del voicebot",
            "gestiones_humano": "Gestiones manuales del call center",
            "pagos": "Transacciones de recupero y pagos",
            "users": "Sistema de usuarios y permisos"
        },
        "etl_capabilities": {
            "extraction_modes": ["incremental", "full_refresh", "sliding_window"],
            "supported_tables": ["dashboard_data", "evolution_data", "assignment_data", "operation_data", "productivity_data"],
            "monitoring": "Comprehensive watermark tracking and status monitoring",
            "recovery": "Automatic cleanup and recovery from failed extractions",
            "concurrency": "Configurable concurrent processing limits"
        },
        "user_management": {
            "authentication": "JWT-based with bcrypt password hashing",
            "authorization": "Role-based access control (RBAC)",
            "roles": ["admin", "manager", "analyst", "viewer"],
            "features": [
                "User CRUD operations",
                "Profile self-management",
                "Role and permission management",
                "Password security validation",
                "Bulk user operations",
                "User statistics and analytics",
                "Audit logging",
                "Data export functionality"
            ],
            "security": {
                "password_requirements": "Minimum 8 chars, uppercase, lowercase, number",
                "soft_delete": "Users are deactivated, not permanently deleted",
                "session_management": "JWT token-based sessions",
                "permission_checks": "Granular permission validation"
            }
        },
        "refresh_schedule": "On-demand via API trigger (incremental)",
        "target_users": "Equipo de cobranzas Telefónica",
        "frontend": "React TypeScript dashboard (Pulso-Dash)",
        "integration": {
            "status": "Fully integrated with Pulso-Dash frontend + ETL system + User Management",
            "authentication": "JWT + API key support",
            "cors": "Configured for cross-origin requests",
            "documentation": "OpenAPI/Swagger at /docs",
            "etl_triggering": "HTTP endpoints for triggering data refreshes",
            "user_management": "Complete user administration system"
        }
    }
