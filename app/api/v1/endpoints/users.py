# app/api/v1/endpoints/users.py
"""
👥 Users API Endpoints - REST API para gestión de usuarios
Endpoints completos para el mantenedor de usuarios del frontend
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.services.user_service import (
    UserService,
    UserCreate,
    UserUpdate,
    UserResponse,
    UserListResponse,
    UserStats,
    get_user_service
)
from app.auth.dependencies import get_current_user, require_permission
from app.core.logging import LoggerMixin


# =============================================================================
# ROUTER SETUP
# =============================================================================

router = APIRouter(prefix="/users", tags=["users"])


# =============================================================================
# USER MANAGEMENT ENDPOINTS
# =============================================================================

@router.post(
    "/",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create new user",
    description="Create a new user with role-based validation"
)
async def create_user(
    user_data: UserCreate,
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.create"))
):
    """
    Crear un nuevo usuario
    
    **Requires:** `user.create` permission
    
    **Validations:**
    - Email unique validation
    - Strong password requirements
    - Valid role assignment
    """
    try:
        new_user = await user_service.create_user(
            user_data=user_data,
            created_by=current_user.id
        )
        return new_user
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )


@router.get(
    "/",
    response_model=UserListResponse,
    summary="Get users list",
    description="Get paginated list of users with optional filtering"
)
async def get_users(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    role: Optional[str] = Query(None, description="Filter by role"),
    search: Optional[str] = Query(None, description="Search by name or email"),
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.read"))
):
    """
    Obtener lista paginada de usuarios
    
    **Requires:** `user.read` permission
    
    **Filters:**
    - `role`: Filter by user role (admin, manager, analyst, viewer)
    - `search`: Search in first_name, last_name, or email
    """
    try:
        users = await user_service.get_users(
            page=page,
            per_page=per_page,
            role_filter=role,
            search=search
        )
        return users
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve users"
        )


@router.get(
    "/{user_id}",
    response_model=UserResponse,
    summary="Get user by ID",
    description="Get detailed information of a specific user"
)
async def get_user(
    user_id: str,
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.read"))
):
    """
    Obtener usuario por ID
    
    **Requires:** `user.read` permission
    """
    try:
        user = await user_service.get_user(user_id)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with ID {user_id} not found"
            )
        
        return user
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve user"
        )


@router.put(
    "/{user_id}",
    response_model=UserResponse,
    summary="Update user",
    description="Update user information with validation"
)
async def update_user(
    user_id: str,
    user_data: UserUpdate,
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.update"))
):
    """
    Actualizar usuario
    
    **Requires:** `user.update` permission
    
    **Validations:**
    - Role assignment validation
    - User existence validation
    """
    try:
        updated_user = await user_service.update_user(
            user_id=user_id,
            update_data=user_data,
            updated_by=current_user.id
        )
        
        return updated_user
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )


@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete user",
    description="Soft delete user (deactivate)"
)
async def delete_user(
    user_id: str,
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.delete"))
):
    """
    Eliminar usuario (soft delete)
    
    **Requires:** `user.delete` permission
    
    **Note:** This is a soft delete - user is deactivated, not permanently removed
    """
    try:
        success = await user_service.delete_user(
            user_id=user_id,
            deleted_by=current_user.id
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to delete user"
            )
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )


# =============================================================================
# USER PROFILE ENDPOINTS
# =============================================================================

@router.get(
    "/me/profile",
    response_model=UserResponse,
    summary="Get current user profile",
    description="Get profile information of the authenticated user"
)
async def get_my_profile(
    current_user: UserResponse = Depends(get_current_user)
):
    """
    Obtener perfil del usuario actual
    
    **Note:** No additional permissions required - users can always view their own profile
    """
    return current_user


@router.put(
    "/me/profile",
    response_model=UserResponse,
    summary="Update current user profile",
    description="Update profile information of the authenticated user"
)
async def update_my_profile(
    profile_data: UserUpdate,
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
):
    """
    Actualizar perfil del usuario actual
    
    **Note:** Users can update their own profile (excluding role and admin fields)
    """
    try:
        # Restrict what users can update about themselves
        allowed_fields = {'first_name', 'last_name'}
        update_dict = profile_data.dict(exclude_unset=True)
        
        # Remove restricted fields if user is not admin
        if current_user.role != 'admin':
            update_dict = {k: v for k, v in update_dict.items() if k in allowed_fields}
        
        if not update_dict:
            return current_user  # Nothing to update
        
        # Create limited update object
        limited_update = UserUpdate(**update_dict)
        
        updated_user = await user_service.update_user(
            user_id=current_user.id,
            update_data=limited_update,
            updated_by=current_user.id
        )
        
        return updated_user
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update profile"
        )


# =============================================================================
# STATISTICS AND ANALYTICS ENDPOINTS
# =============================================================================

@router.get(
    "/statistics",
    response_model=UserStats,
    summary="Get user statistics",
    description="Get comprehensive user statistics and analytics"
)
async def get_user_statistics(
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.read"))
):
    """
    Obtener estadísticas de usuarios
    
    **Requires:** `user.read` permission
    
    **Returns:**
    - Total users count
    - Verified users count
    - Active users (7 and 30 days)
    - New users (30 days)
    - Users by role distribution
    """
    try:
        stats = await user_service.get_user_statistics()
        return stats
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve user statistics"
        )


@router.get(
    "/recent",
    response_model=List[UserResponse],
    summary="Get recent users",
    description="Get list of recently registered users"
)
async def get_recent_users(
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.read"))
):
    """
    Obtener usuarios registrados recientemente
    
    **Requires:** `user.read` permission
    """
    try:
        recent_users = await user_service.get_recent_users(days=days)
        return recent_users
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve recent users"
        )


# =============================================================================
# BULK OPERATIONS ENDPOINTS
# =============================================================================

@router.post(
    "/bulk-update",
    summary="Bulk update users",
    description="Update multiple users in a single operation"
)
async def bulk_update_users(
    updates: List[dict],
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.update"))
):
    """
    Actualización masiva de usuarios
    
    **Requires:** `user.update` permission
    
    **Format:**
    ```json
    [
        {"id": "user_id_1", "role": "analyst", "is_active": true},
        {"id": "user_id_2", "first_name": "New Name"}
    ]
    ```
    """
    try:
        updated_count = await user_service.bulk_update_users(
            updates=updates,
            updated_by=current_user.id
        )
        
        return {
            "message": f"Successfully updated {updated_count} users",
            "updated_count": updated_count,
            "total_attempted": len(updates)
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to perform bulk update"
        )


@router.get(
    "/export",
    summary="Export users",
    description="Export all users data for download"
)
async def export_users(
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.read"))
):
    """
    Exportar datos de usuarios
    
    **Requires:** `user.read` permission
    
    **Returns:** CSV-ready data for user export
    """
    try:
        export_data = await user_service.export_users()
        
        return {
            "data": export_data,
            "total_records": len(export_data),
            "export_timestamp": "2025-07-03T03:30:00Z",
            "exported_by": current_user.email
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export users"
        )


# =============================================================================
# ROLE MANAGEMENT ENDPOINTS
# =============================================================================

@router.get(
    "/roles",
    summary="Get available roles",
    description="Get list of available user roles and their permissions"
)
async def get_available_roles(
    current_user: UserResponse = Depends(get_current_user),
    _: None = Depends(require_permission("user.read"))
):
    """
    Obtener roles disponibles
    
    **Requires:** `user.read` permission
    """
    roles = [
        {
            "name": "admin",
            "display_name": "Administrador",
            "description": "Acceso completo al sistema",
            "permissions": ["*"]
        },
        {
            "name": "manager",
            "display_name": "Gerente",
            "description": "Gestión de usuarios y acceso a reportes",
            "permissions": ["user.read", "user.create", "user.update", "dashboard.read", "reports.read", "reports.export"]
        },
        {
            "name": "analyst",
            "display_name": "Analista",
            "description": "Acceso a dashboard y reportes",
            "permissions": ["dashboard.read", "reports.read", "reports.export"]
        },
        {
            "name": "viewer",
            "display_name": "Visualizador",
            "description": "Solo lectura del dashboard",
            "permissions": ["dashboard.read"]
        }
    ]
    
    return {
        "roles": roles,
        "total": len(roles)
    }


@router.get(
    "/roles/{role}/users",
    response_model=List[UserResponse],
    summary="Get users by role",
    description="Get all users with a specific role"
)
async def get_users_by_role(
    role: str,
    current_user: UserResponse = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    _: None = Depends(require_permission("user.read"))
):
    """
    Obtener usuarios por rol
    
    **Requires:** `user.read` permission
    """
    try:
        # Validate role
        valid_roles = ['admin', 'manager', 'analyst', 'viewer']
        if role not in valid_roles:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}"
            )
        
        users = await user_service.get_users(
            page=1,
            per_page=1000,  # Get all users for role
            role_filter=role
        )
        
        return users.users
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve users by role"
        )


# =============================================================================
# HEALTH CHECK ENDPOINT
# =============================================================================

@router.get(
    "/health",
    summary="User service health check",
    description="Check health of user service and database connection"
)
async def users_health_check(
    user_service: UserService = Depends(get_user_service)
):
    """
    Health check del servicio de usuarios
    """
    try:
        # Test database connection
        health_status = await user_service.user_repo.health_check()
        
        return {
            "status": "healthy" if health_status else "unhealthy",
            "service": "users",
            "database_connection": health_status,
            "timestamp": "2025-07-03T03:30:00Z"
        }
    
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "service": "users",
                "error": str(e),
                "timestamp": "2025-07-03T03:30:00Z"
            }
        )
