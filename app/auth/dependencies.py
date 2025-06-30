"""
🔐 Authentication and Authorization Dependencies
FastAPI dependencies for JWT authentication, CSRF protection, and permission checking

Features:
- JWT token validation and user extraction
- CSRF token verification with double-submit pattern
- Role-based access control (RBAC)
- Permission-based authorization
- Rate limiting and security headers
- Request context and audit logging
"""

from typing import Optional, List, Callable, Any
from datetime import datetime, timezone
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.auth.models import User, Role, Permission, CSRFToken
from app.auth.security import get_security_service, SecurityService
from app.auth.config import get_auth_settings, get_client_ip, get_user_agent
from app.database.connection import get_database_manager
from app.core.logging import LoggerMixin

# HTTP Bearer token security scheme
security_scheme = HTTPBearer(auto_error=False)

class AuthenticationError(HTTPException):
    """Custom authentication error"""
    def __init__(self, detail: str = "Authentication failed"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"}
        )

class AuthorizationError(HTTPException):
    """Custom authorization error"""
    def __init__(self, detail: str = "Insufficient permissions"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail
        )

class CSRFError(HTTPException):
    """Custom CSRF error"""
    def __init__(self, detail: str = "CSRF token validation failed"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail
        )


class AuthContext(LoggerMixin):
    """
    Authentication context for request processing
    
    Contains authenticated user information and security context
    """
    
    def __init__(
        self,
        user: User,
        token_payload: dict,
        request: Request,
        security_service: SecurityService
    ):
        super().__init__()
        self.user = user
        self.token_payload = token_payload
        self.request = request
        self.security_service = security_service
        self.ip_address = get_client_ip(request)
        self.user_agent = get_user_agent(request)
        self.request_id = getattr(request.state, 'request_id', None)
    
    @property
    def user_id(self) -> str:
        """Get current user ID"""
        return str(self.user.id)
    
    @property
    def user_email(self) -> str:
        """Get current user email"""
        return self.user.email
    
    @property
    def user_role(self) -> str:
        """Get current user role name"""
        return self.user.role.name if self.user.role else None
    
    @property
    def is_admin(self) -> bool:
        """Check if user has admin role"""
        return self.user_role in ['superadmin', 'admin']
    
    @property
    def is_superadmin(self) -> bool:
        """Check if user has superadmin role"""
        return self.user_role == 'superadmin'
    
    def has_permission(self, permission_name: str) -> bool:
        """
        Check if user has specific permission
        
        Args:
            permission_name: Permission name (e.g., 'user.read')
            
        Returns:
            bool: True if user has permission
        """
        if not self.user.role or not self.user.role.permissions:
            return False
        
        for permission in self.user.role.permissions:
            if permission.name == permission_name and permission.is_active:
                return True
        
        return False
    
    def has_any_permission(self, permission_names: List[str]) -> bool:
        """
        Check if user has any of the specified permissions
        
        Args:
            permission_names: List of permission names
            
        Returns:
            bool: True if user has at least one permission
        """
        return any(self.has_permission(perm) for perm in permission_names)
    
    def has_all_permissions(self, permission_names: List[str]) -> bool:
        """
        Check if user has all specified permissions
        
        Args:
            permission_names: List of permission names
            
        Returns:
            bool: True if user has all permissions
        """
        return all(self.has_permission(perm) for perm in permission_names)
    
    async def log_access_event(self, resource: str, action: str, result: str = "success") -> None:
        """
        Log access event for audit trail
        
        Args:
            resource: Resource being accessed
            action: Action being performed
            result: Access result
        """
        await self.security_service.log_security_event(
            event_type="resource_access",
            event_description=f"User accessed {resource} with action {action}",
            user_id=self.user_id,
            result=result,
            ip_address=self.ip_address,
            user_agent=self.user_agent,
            target_type=resource,
            target_id=action,
            request_id=self.request_id
        )


# =============================================================================
# AUTHENTICATION DEPENDENCIES
# =============================================================================

async def get_current_user_optional(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme),
    security_service: SecurityService = Depends(get_security_service)
) -> Optional[AuthContext]:
    """
    Get current authenticated user (optional - returns None if not authenticated)
    
    Args:
        request: FastAPI request object
        credentials: HTTP Bearer credentials
        security_service: Security service instance
        
    Returns:
        Optional[AuthContext]: Auth context if authenticated, None otherwise
    """
    if not credentials:
        return None
    
    # Verify JWT token
    token_payload = security_service.verify_access_token(credentials.credentials)
    if not token_payload:
        return None
    
    # Get user from database
    user_id = token_payload.get("sub")
    if not user_id:
        return None
    
    db = await get_database_manager()
    async with db.get_session() as session:
        stmt = select(User).where(
            User.id == user_id,
            User.status == 'active'
        ).options(
            selectinload(User.role).selectinload(Role.permissions)
        )
        
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
        
        if not user or not user.is_active:
            return None
        
        # Update last login tracking
        user.last_login_at = datetime.now(timezone.utc)
        user.last_login_ip = get_client_ip(request)
        await session.commit()
        
        return AuthContext(user, token_payload, request, security_service)


async def get_current_user(
    auth_context: Optional[AuthContext] = Depends(get_current_user_optional)
) -> AuthContext:
    """
    Get current authenticated user (required - raises exception if not authenticated)
    
    Args:
        auth_context: Optional auth context from get_current_user_optional
        
    Returns:
        AuthContext: Authenticated user context
        
    Raises:
        AuthenticationError: If user is not authenticated
    """
    if not auth_context:
        raise AuthenticationError("Authentication required")
    
    return auth_context


async def get_active_user(
    auth_context: AuthContext = Depends(get_current_user)
) -> AuthContext:
    """
    Get current active user (ensures user account is active and not locked)
    
    Args:
        auth_context: Auth context from get_current_user
        
    Returns:
        AuthContext: Active user context
        
    Raises:
        AuthenticationError: If user account is inactive or locked
    """
    if not auth_context.user.is_active:
        raise AuthenticationError("Account is inactive")
    
    if auth_context.user.is_locked:
        raise AuthenticationError("Account is temporarily locked")
    
    return auth_context


# =============================================================================
# CSRF PROTECTION DEPENDENCIES
# =============================================================================

async def verify_csrf_token(
    request: Request,
    auth_context: AuthContext = Depends(get_current_user),
    security_service: SecurityService = Depends(get_security_service)
) -> None:
    """
    Verify CSRF token using double-submit pattern
    
    Args:
        request: FastAPI request object
        auth_context: Authenticated user context
        security_service: Security service instance
        
    Raises:
        CSRFError: If CSRF validation fails
    """
    # Only check CSRF for state-changing methods
    if request.method not in ["POST", "PUT", "PATCH", "DELETE"]:
        return
    
    # Get CSRF token from header
    csrf_header = request.headers.get("X-CSRF-Token")
    if not csrf_header:
        await security_service.log_security_event(
            event_type="csrf_violation",
            event_description="Missing CSRF token in header",
            user_id=auth_context.user_id,
            result="failure",
            ip_address=auth_context.ip_address,
            request_id=auth_context.request_id
        )
        raise CSRFError("CSRF token required in X-CSRF-Token header")
    
    # Get CSRF token from cookie
    csrf_cookie = request.cookies.get("csrf_token")
    if not csrf_cookie:
        await security_service.log_security_event(
            event_type="csrf_violation",
            event_description="Missing CSRF token in cookie",
            user_id=auth_context.user_id,
            result="failure",
            ip_address=auth_context.ip_address,
            request_id=auth_context.request_id
        )
        raise CSRFError("CSRF cookie not found")
    
    # Verify tokens match (double-submit pattern)
    if not security_service.verify_csrf_token(csrf_header, security_service.hash_csrf_token(csrf_cookie)):
        await security_service.log_security_event(
            event_type="csrf_violation",
            event_description="CSRF token mismatch",
            user_id=auth_context.user_id,
            result="failure",
            ip_address=auth_context.ip_address,
            request_id=auth_context.request_id
        )
        raise CSRFError("CSRF token validation failed")
    
    # Verify token in database and mark as used
    db = await get_database_manager()
    async with db.get_session() as session:
        token_hash = security_service.hash_csrf_token(csrf_cookie)
        stmt = select(CSRFToken).where(
            CSRFToken.token_hash == token_hash,
            CSRFToken.user_id == auth_context.user.id,
            CSRFToken.is_used == False
        )
        
        result = await session.execute(stmt)
        csrf_token = result.scalar_one_or_none()
        
        if not csrf_token or not csrf_token.is_valid:
            await security_service.log_security_event(
                event_type="csrf_violation",
                event_description="Invalid or expired CSRF token",
                user_id=auth_context.user_id,
                result="failure",
                ip_address=auth_context.ip_address,
                request_id=auth_context.request_id
            )
            raise CSRFError("CSRF token is invalid or expired")
        
        # Mark token as used
        csrf_token.is_used = True
        csrf_token.used_at = datetime.now(timezone.utc)
        await session.commit()


# =============================================================================
# AUTHORIZATION DEPENDENCIES
# =============================================================================

def require_permissions(*permission_names: str) -> Callable:
    """
    Dependency factory for requiring specific permissions
    
    Args:
        *permission_names: Required permission names
        
    Returns:
        Callable: FastAPI dependency function
    """
    async def permission_dependency(
        auth_context: AuthContext = Depends(get_active_user)
    ) -> AuthContext:
        missing_permissions = []
        for permission in permission_names:
            if not auth_context.has_permission(permission):
                missing_permissions.append(permission)
        
        if missing_permissions:
            await auth_context.security_service.log_security_event(
                event_type="authorization_failure",
                event_description=f"Insufficient permissions: {', '.join(missing_permissions)}",
                user_id=auth_context.user_id,
                result="failure",
                ip_address=auth_context.ip_address,
                request_id=auth_context.request_id
            )
            raise AuthorizationError(f"Missing required permissions: {', '.join(missing_permissions)}")
        
        return auth_context
    
    return permission_dependency


def require_any_permission(*permission_names: str) -> Callable:
    """
    Dependency factory for requiring any of the specified permissions
    
    Args:
        *permission_names: Any of these permission names
        
    Returns:
        Callable: FastAPI dependency function
    """
    async def permission_dependency(
        auth_context: AuthContext = Depends(get_active_user)
    ) -> AuthContext:
        if not auth_context.has_any_permission(list(permission_names)):
            await auth_context.security_service.log_security_event(
                event_type="authorization_failure",
                event_description=f"No required permissions from: {', '.join(permission_names)}",
                user_id=auth_context.user_id,
                result="failure",
                ip_address=auth_context.ip_address,
                request_id=auth_context.request_id
            )
            raise AuthorizationError(f"Requires any of: {', '.join(permission_names)}")
        
        return auth_context
    
    return permission_dependency


def require_role(*role_names: str) -> Callable:
    """
    Dependency factory for requiring specific roles
    
    Args:
        *role_names: Required role names
        
    Returns:
        Callable: FastAPI dependency function
    """
    async def role_dependency(
        auth_context: AuthContext = Depends(get_active_user)
    ) -> AuthContext:
        if auth_context.user_role not in role_names:
            await auth_context.security_service.log_security_event(
                event_type="authorization_failure",
                event_description=f"Insufficient role. Required: {', '.join(role_names)}, has: {auth_context.user_role}",
                user_id=auth_context.user_id,
                result="failure",
                ip_address=auth_context.ip_address,
                request_id=auth_context.request_id
            )
            raise AuthorizationError(f"Requires role: {' or '.join(role_names)}")
        
        return auth_context
    
    return role_dependency


def require_admin() -> Callable:
    """
    Dependency for requiring admin role
    
    Returns:
        Callable: FastAPI dependency function
    """
    return require_role("admin", "superadmin")


def require_superadmin() -> Callable:
    """
    Dependency for requiring superadmin role
    
    Returns:
        Callable: FastAPI dependency function
    """
    return require_role("superadmin")


# =============================================================================
# COMBINED SECURITY DEPENDENCIES
# =============================================================================

def require_auth_and_csrf() -> Callable:
    """
    Dependency for requiring both authentication and CSRF protection
    
    Returns:
        Callable: FastAPI dependency function
    """
    async def combined_dependency(
        auth_context: AuthContext = Depends(get_active_user),
        _: None = Depends(verify_csrf_token)
    ) -> AuthContext:
        return auth_context
    
    return combined_dependency


def require_permissions_and_csrf(*permission_names: str) -> Callable:
    """
    Dependency for requiring permissions and CSRF protection
    
    Args:
        *permission_names: Required permission names
        
    Returns:
        Callable: FastAPI dependency function
    """
    async def combined_dependency(
        auth_context: AuthContext = Depends(require_permissions(*permission_names)),
        _: None = Depends(verify_csrf_token)
    ) -> AuthContext:
        return auth_context
    
    return combined_dependency


# =============================================================================
# UTILITY DEPENDENCIES
# =============================================================================

async def get_database_session() -> AsyncSession:
    """
    Get database session for authentication operations
    
    Returns:
        AsyncSession: Database session
    """
    db = await get_database_manager()
    async with db.get_session() as session:
        yield session


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Exceptions
    'AuthenticationError',
    'AuthorizationError', 
    'CSRFError',
    
    # Core dependencies
    'get_current_user_optional',
    'get_current_user',
    'get_active_user',
    'verify_csrf_token',
    
    # Authorization dependencies
    'require_permissions',
    'require_any_permission', 
    'require_role',
    'require_admin',
    'require_superadmin',
    
    # Combined dependencies
    'require_auth_and_csrf',
    'require_permissions_and_csrf',
    
    # Context
    'AuthContext',
    
    # Utilities
    'get_database_session'
]
