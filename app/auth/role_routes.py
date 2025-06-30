"""
🔐 Role and Permission Management API Endpoints
FastAPI routes for CRUD operations on roles and permissions with system protection

Endpoints:
- GET /api/v1/roles - List roles
- POST /api/v1/roles - Create new role
- GET /api/v1/roles/{role_id} - Get role details
- PUT /api/v1/roles/{role_id} - Update role
- DELETE /api/v1/roles/{role_id} - Delete role
- GET /api/v1/permissions - List all permissions

Features:
- System role protection
- Permission assignment
- CSRF protection for mutations
- Comprehensive validation
- Audit logging
"""

from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, validator
import uuid

from app.auth.dependencies import (
    require_permissions_and_csrf,
    require_permissions,
    require_admin,
    AuthContext
)
from app.auth.services import SecurityService
from app.core.logging import LoggerMixin
from app.database.connection import get_database_manager

# Pydantic models for roles
class RoleCreateRequest(BaseModel):
    name: str
    display_name: str
    description: Optional[str] = None
    permission_ids: List[str] = []

    @validator('name')
    def validate_name(cls, v):
        if not v or len(v.strip()) < 2:
            raise ValueError("Role name must be at least 2 characters long")
        # Ensure lowercase and no spaces for system consistency
        clean_name = v.strip().lower().replace(' ', '_')
        if not clean_name.replace('_', '').isalnum():
            raise ValueError("Role name can only contain letters, numbers, and underscores")
        return clean_name

    @validator('display_name')
    def validate_display_name(cls, v):
        if not v or len(v.strip()) < 2:
            raise ValueError("Display name must be at least 2 characters long")
        return v.strip()

class RoleUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    permission_ids: Optional[List[str]] = None

    @validator('display_name')
    def validate_display_name(cls, v):
        if v is not None and (not v or len(v.strip()) < 2):
            raise ValueError("Display name must be at least 2 characters long")
        return v.strip() if v else v

class PermissionResponse(BaseModel):
    id: str
    name: str
    resource: str
    action: str
    description: Optional[str] = None
    is_system: bool

class RoleResponse(BaseModel):
    id: str
    name: str
    display_name: str
    description: Optional[str] = None
    is_system: bool
    is_active: bool
    permissions: List[PermissionResponse]
    user_count: int
    created_at: datetime
    updated_at: datetime

class RoleListResponse(BaseModel):
    roles: List[RoleResponse]
    total: int

class RoleDeleteResponse(BaseModel):
    message: str
    success: bool = True

class PermissionListResponse(BaseModel):
    permissions: List[PermissionResponse]
    total: int

# Routers
roles_router = APIRouter(prefix="/api/v1/roles", tags=["Role Management"])
permissions_router = APIRouter(prefix="/api/v1/permissions", tags=["Permission Management"])


class RolesAPI(LoggerMixin):
    """Role management API implementation"""
    
    def __init__(self):
        super().__init__()
        self.security_service = SecurityService()


# Global API instance
roles_api = RolesAPI()


@roles_router.get("", response_model=RoleListResponse)
async def list_roles(
    include_inactive: bool = Query(default=False, description="Include inactive roles"),
    auth: AuthContext = Depends(require_permissions("role.read")),
    db=Depends(get_database_manager)
):
    """
    List all roles with their permissions
    
    Requires: role.read permission
    """
    try:
        # Query roles with permission count
        where_clause = "WHERE r.is_active = true" if not include_inactive else ""
        
        roles_query = f"""
            SELECT 
                r.id, r.name, r.display_name, r.description, r.is_system, r.is_active,
                r.created_at, r.updated_at,
                COUNT(DISTINCT u.id) as user_count
            FROM roles r
            LEFT JOIN users u ON r.id = u.role_id AND u.status = 'active'
            {where_clause}
            GROUP BY r.id, r.name, r.display_name, r.description, r.is_system, r.is_active, r.created_at, r.updated_at
            ORDER BY r.is_system DESC, r.name ASC
        """
        
        roles_rows = await db.execute_query(roles_query, fetch="all")
        
        # Get permissions for each role
        roles = []
        for role_row in roles_rows:
            permissions_query = """
                SELECT p.id, p.name, p.resource, p.action, p.description, p.is_system
                FROM permissions p
                JOIN role_permissions rp ON p.id = rp.permission_id
                WHERE rp.role_id = $1 AND p.is_active = true
                ORDER BY p.resource, p.action
            """
            
            permissions_rows = await db.execute_query(permissions_query, [role_row['id']], fetch="all")
            
            permissions = [
                PermissionResponse(
                    id=str(perm['id']),
                    name=perm['name'],
                    resource=perm['resource'],
                    action=perm['action'],
                    description=perm['description'],
                    is_system=perm['is_system']
                )
                for perm in permissions_rows
            ]
            
            roles.append(RoleResponse(
                id=str(role_row['id']),
                name=role_row['name'],
                display_name=role_row['display_name'],
                description=role_row['description'],
                is_system=role_row['is_system'],
                is_active=role_row['is_active'],
                permissions=permissions,
                user_count=role_row['user_count'],
                created_at=role_row['created_at'],
                updated_at=role_row['updated_at']
            ))
        
        # Audit log
        await roles_api.security_service.create_audit_log(
            db=db,
            user_id=auth.user.id,
            event_type="role_list",
            event_category="role_mgmt",
            event_description=f"User {auth.user.email} listed {len(roles)} roles",
            result="success",
            target_type="role_list"
        )
        
        return RoleListResponse(
            roles=roles,
            total=len(roles)
        )
        
    except Exception as e:
        roles_api.logger.error(f"Error listing roles: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving roles"
        )


@roles_router.post("", response_model=RoleResponse)
async def create_role(
    role_data: RoleCreateRequest,
    auth: AuthContext = Depends(require_permissions_and_csrf("role.create")),
    db=Depends(get_database_manager)
):
    """
    Create new role
    
    Requires: role.create permission + CSRF token
    """
    try:
        # Check if role name already exists
        name_query = "SELECT id FROM roles WHERE name = $1"
        existing_role = await db.execute_query(name_query, [role_data.name], fetch="one")
        
        if existing_role:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Role name already exists"
            )
        
        # Validate permissions exist
        if role_data.permission_ids:
            permission_uuids = []
            for perm_id in role_data.permission_ids:
                try:
                    permission_uuids.append(uuid.UUID(perm_id))
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid permission ID format: {perm_id}"
                    )
            
            # Check all permissions exist and are active
            perm_check_query = """
                SELECT id FROM permissions 
                WHERE id = ANY($1) AND is_active = true
            """
            existing_perms = await db.execute_query(perm_check_query, [permission_uuids], fetch="all")
            
            if len(existing_perms) != len(permission_uuids):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="One or more permission IDs are invalid or inactive"
                )
        
        # Create role
        role_id = uuid.uuid4()
        now = datetime.now()
        
        create_query = """
            INSERT INTO roles (
                id, name, display_name, description, is_system, is_active,
                created_at, updated_at, created_by
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )
        """
        
        await db.execute_query(create_query, [
            role_id, role_data.name, role_data.display_name, role_data.description,
            False, True, now, now, auth.user.id
        ])
        
        # Assign permissions
        if role_data.permission_ids:
            for perm_id in permission_uuids:
                perm_assign_query = """
                    INSERT INTO role_permissions (role_id, permission_id)
                    VALUES ($1, $2)
                    ON CONFLICT (role_id, permission_id) DO NOTHING
                """
                await db.execute_query(perm_assign_query, [role_id, perm_id])
        
        # Retrieve created role with permissions
        created_role = await _get_role_by_id(db, role_id)
        
        # Audit log
        await roles_api.security_service.create_audit_log(
            db=db,
            user_id=auth.user.id,
            event_type="role_created",
            event_category="role_mgmt",
            event_description=f"User {auth.user.email} created role {role_data.name}",
            result="success",
            target_type="role",
            target_id=str(role_id),
            target_details=f"Name: {role_data.name}, Permissions: {len(role_data.permission_ids)}"
        )
        
        roles_api.logger.info(f"Role {role_data.name} created by {auth.user.email}")
        
        return created_role
        
    except HTTPException:
        raise
    except Exception as e:
        roles_api.logger.error(f"Error creating role: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating role"
        )


@roles_router.get("/{role_id}", response_model=RoleResponse)
async def get_role(
    role_id: str,
    auth: AuthContext = Depends(require_permissions("role.read")),
    db=Depends(get_database_manager)
):
    """
    Get role details by ID
    
    Requires: role.read permission
    """
    try:
        # Validate UUID
        try:
            role_uuid = uuid.UUID(role_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid role ID format"
            )
        
        role = await _get_role_by_id(db, role_uuid)
        
        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Role not found"
            )
        
        # Audit log
        await roles_api.security_service.create_audit_log(
            db=db,
            user_id=auth.user.id,
            event_type="role_viewed",
            event_category="role_mgmt",
            event_description=f"User {auth.user.email} viewed role {role.name}",
            result="success",
            target_type="role",
            target_id=str(role_uuid)
        )
        
        return role
        
    except HTTPException:
        raise
    except Exception as e:
        roles_api.logger.error(f"Error getting role {role_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving role"
        )


@roles_router.put("/{role_id}", response_model=RoleResponse)
async def update_role(
    role_id: str,
    role_data: RoleUpdateRequest,
    auth: AuthContext = Depends(require_permissions_and_csrf("role.update")),
    db=Depends(get_database_manager)
):
    """
    Update role
    
    Requires: role.update permission + CSRF token
    Note: System roles can only be updated by superadmin
    """
    try:
        # Validate UUID
        try:
            role_uuid = uuid.UUID(role_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid role ID format"
            )
        
        # Check if role exists
        check_query = "SELECT id, name, is_system FROM roles WHERE id = $1"
        existing_role = await db.execute_query(check_query, [role_uuid], fetch="one")
        
        if not existing_role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Role not found"
            )
        
        # Protect system roles
        if existing_role['is_system'] and not auth.has_role('superadmin'):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only superadmin can modify system roles"
            )
        
        # Build update query dynamically
        update_fields = []
        params = []
        param_count = 0
        
        if role_data.display_name:
            update_fields.append(f"display_name = ${param_count + 1}")
            params.append(role_data.display_name)
            param_count += 1
        
        if role_data.description is not None:
            update_fields.append(f"description = ${param_count + 1}")
            params.append(role_data.description)
            param_count += 1
        
        if update_fields:
            # Add updated_at and updated_by
            update_fields.extend([
                f"updated_at = ${param_count + 1}",
                f"updated_by = ${param_count + 2}"
            ])
            params.extend([datetime.now(), auth.user.id])
            param_count += 2
            
            # Add WHERE clause
            params.append(role_uuid)
            
            update_query = f"""
                UPDATE roles 
                SET {', '.join(update_fields)}
                WHERE id = ${param_count + 1}
            """
            
            await db.execute_query(update_query, params)
        
        # Update permissions if provided
        if role_data.permission_ids is not None:
            # Validate permissions
            permission_uuids = []
            for perm_id in role_data.permission_ids:
                try:
                    permission_uuids.append(uuid.UUID(perm_id))
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid permission ID format: {perm_id}"
                    )
            
            if permission_uuids:
                # Check all permissions exist and are active
                perm_check_query = """
                    SELECT id FROM permissions 
                    WHERE id = ANY($1) AND is_active = true
                """
                existing_perms = await db.execute_query(perm_check_query, [permission_uuids], fetch="all")
                
                if len(existing_perms) != len(permission_uuids):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="One or more permission IDs are invalid or inactive"
                    )
            
            # Remove all existing permissions
            delete_perms_query = "DELETE FROM role_permissions WHERE role_id = $1"
            await db.execute_query(delete_perms_query, [role_uuid])
            
            # Add new permissions
            for perm_id in permission_uuids:
                perm_assign_query = """
                    INSERT INTO role_permissions (role_id, permission_id)
                    VALUES ($1, $2)
                """
                await db.execute_query(perm_assign_query, [role_uuid, perm_id])
        
        # Retrieve updated role
        updated_role = await _get_role_by_id(db, role_uuid)
        
        # Audit log
        changes = []
        if role_data.display_name: changes.append(f"display_name: {role_data.display_name}")
        if role_data.description is not None: changes.append(f"description: {role_data.description or '[CLEARED]'}")
        if role_data.permission_ids is not None: changes.append(f"permissions: {len(role_data.permission_ids)} assigned")
        
        await roles_api.security_service.create_audit_log(
            db=db,
            user_id=auth.user.id,
            event_type="role_updated",
            event_category="role_mgmt",
            event_description=f"User {auth.user.email} updated role {existing_role['name']}",
            result="success",
            target_type="role",
            target_id=str(role_uuid),
            target_details=f"Changes: {', '.join(changes)}"
        )
        
        roles_api.logger.info(f"Role {existing_role['name']} updated by {auth.user.email}")
        
        return updated_role
        
    except HTTPException:
        raise
    except Exception as e:
        roles_api.logger.error(f"Error updating role {role_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating role"
        )


@roles_router.delete("/{role_id}", response_model=RoleDeleteResponse)
async def delete_role(
    role_id: str,
    auth: AuthContext = Depends(require_permissions_and_csrf("role.delete")),
    db=Depends(get_database_manager)
):
    """
    Delete role
    
    Requires: role.delete permission + CSRF token
    Note: System roles cannot be deleted
    """
    try:
        # Validate UUID
        try:
            role_uuid = uuid.UUID(role_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid role ID format"
            )
        
        # Check if role exists
        check_query = "SELECT id, name, is_system FROM roles WHERE id = $1"
        existing_role = await db.execute_query(check_query, [role_uuid], fetch="one")
        
        if not existing_role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Role not found"
            )
        
        # Protect system roles
        if existing_role['is_system']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="System roles cannot be deleted"
            )
        
        # Check if role is assigned to any users
        users_query = "SELECT COUNT(*) as user_count FROM users WHERE role_id = $1 AND status = 'active'"
        user_count_result = await db.execute_query(users_query, [role_uuid], fetch="one")
        
        if user_count_result['user_count'] > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot delete role that is assigned to {user_count_result['user_count']} active users"
            )
        
        # Delete role (cascade will handle role_permissions)
        delete_query = "DELETE FROM roles WHERE id = $1"
        await db.execute_query(delete_query, [role_uuid])
        
        # Audit log
        await roles_api.security_service.create_audit_log(
            db=db,
            user_id=auth.user.id,
            event_type="role_deleted",
            event_category="role_mgmt",
            event_description=f"User {auth.user.email} deleted role {existing_role['name']}",
            result="success",
            target_type="role",
            target_id=str(role_uuid),
            target_details=f"Deleted role: {existing_role['name']}"
        )
        
        roles_api.logger.info(f"Role {existing_role['name']} deleted by {auth.user.email}")
        
        return RoleDeleteResponse(
            message=f"Role {existing_role['name']} deleted successfully",
            success=True
        )
        
    except HTTPException:
        raise
    except Exception as e:
        roles_api.logger.error(f"Error deleting role {role_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting role"
        )


@permissions_router.get("", response_model=PermissionListResponse)
async def list_permissions(
    resource: Optional[str] = Query(default=None, description="Filter by resource"),
    include_system: bool = Query(default=True, description="Include system permissions"),
    auth: AuthContext = Depends(require_permissions("permission.read")),
    db=Depends(get_database_manager)
):
    """
    List all permissions
    
    Requires: permission.read permission
    """
    try:
        # Build query
        where_conditions = ["p.is_active = true"]
        params = []
        param_count = 0
        
        if resource:
            where_conditions.append(f"p.resource = ${param_count + 1}")
            params.append(resource)
            param_count += 1
        
        if not include_system:
            where_conditions.append("p.is_system = false")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT id, name, resource, action, description, is_system
            FROM permissions p
            {where_clause}
            ORDER BY resource, action
        """
        
        rows = await db.execute_query(query, params, fetch="all")
        
        permissions = [
            PermissionResponse(
                id=str(row['id']),
                name=row['name'],
                resource=row['resource'],
                action=row['action'],
                description=row['description'],
                is_system=row['is_system']
            )
            for row in rows
        ]
        
        # Audit log
        await roles_api.security_service.create_audit_log(
            db=db,
            user_id=auth.user.id,
            event_type="permission_list",
            event_category="role_mgmt",
            event_description=f"User {auth.user.email} listed {len(permissions)} permissions",
            result="success",
            target_type="permission_list"
        )
        
        return PermissionListResponse(
            permissions=permissions,
            total=len(permissions)
        )
        
    except Exception as e:
        roles_api.logger.error(f"Error listing permissions: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving permissions"
        )


# Helper function
async def _get_role_by_id(db, role_id: uuid.UUID) -> Optional[RoleResponse]:
    """Get role with permissions by ID"""
    # Get role details
    role_query = """
        SELECT 
            r.id, r.name, r.display_name, r.description, r.is_system, r.is_active,
            r.created_at, r.updated_at,
            COUNT(DISTINCT u.id) as user_count
        FROM roles r
        LEFT JOIN users u ON r.id = u.role_id AND u.status = 'active'
        WHERE r.id = $1
        GROUP BY r.id, r.name, r.display_name, r.description, r.is_system, r.is_active, r.created_at, r.updated_at
    """
    
    role_row = await db.execute_query(role_query, [role_id], fetch="one")
    
    if not role_row:
        return None
    
    # Get permissions
    permissions_query = """
        SELECT p.id, p.name, p.resource, p.action, p.description, p.is_system
        FROM permissions p
        JOIN role_permissions rp ON p.id = rp.permission_id
        WHERE rp.role_id = $1 AND p.is_active = true
        ORDER BY p.resource, p.action
    """
    
    permissions_rows = await db.execute_query(permissions_query, [role_id], fetch="all")
    
    permissions = [
        PermissionResponse(
            id=str(perm['id']),
            name=perm['name'],
            resource=perm['resource'],
            action=perm['action'],
            description=perm['description'],
            is_system=perm['is_system']
        )
        for perm in permissions_rows
    ]
    
    return RoleResponse(
        id=str(role_row['id']),
        name=role_row['name'],
        display_name=role_row['display_name'],
        description=role_row['description'],
        is_system=role_row['is_system'],
        is_active=role_row['is_active'],
        permissions=permissions,
        user_count=role_row['user_count'],
        created_at=role_row['created_at'],
        updated_at=role_row['updated_at']
    )
