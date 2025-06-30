"""
🔐 Authentication Module Initialization
Central module for authentication and authorization system

Features:
- Router registration
- Database migrations
- Default data seeding
- Security middleware
- Configuration validation
"""

from fastapi import FastAPI
from app.auth.routes import auth_router
from app.auth.user_routes import users_router
from app.auth.role_routes import roles_router, permissions_router
from app.auth.config import get_auth_settings
from app.auth.middleware import setup_security_middleware
from app.auth.database import setup_auth_database

def setup_auth_module(app: FastAPI):
    """
    Setup authentication module for FastAPI application
    
    Args:
        app: FastAPI application instance
    """
    # Validate configuration
    auth_settings = get_auth_settings()
    
    # Setup security middleware
    setup_security_middleware(app)
    
    # Register authentication routers
    app.include_router(auth_router)
    app.include_router(users_router)
    app.include_router(roles_router)
    app.include_router(permissions_router)
    
    # Setup authentication database
    @app.on_event("startup")
    async def setup_auth_on_startup():
        await setup_auth_database()

# Export main components
__all__ = [
    "setup_auth_module",
    "auth_router",
    "users_router", 
    "roles_router",
    "permissions_router"
]
