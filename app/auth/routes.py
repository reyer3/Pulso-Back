"""
🔐 Authentication API Endpoints
FastAPI routes for JWT authentication, CSRF protection, and session management

Endpoints:
- POST /api/v1/auth/login - User authentication
- POST /api/v1/auth/refresh - Token refresh
- POST /api/v1/auth/logout - User logout
- GET /api/v1/auth/me - Current user info
- GET /api/v1/auth/csrf-token - CSRF token generation

Features:
- Secure cookie handling
- Rate limiting protection
- Comprehensive audit logging
- Device/session tracking
- Error handling with proper HTTP status codes
"""

from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPBearer
from pydantic import BaseModel, EmailStr
import uuid

from app.auth.dependencies import get_current_user_optional, verify_csrf_token, AuthContext
from app.auth.services import SecurityService
from app.auth.config import get_auth_settings
from app.auth.exceptions import (
    AuthenticationError, 
    InvalidCredentialsError, 
    AccountLockedError,
    TokenExpiredError,
    InvalidTokenError
)
from app.core.logging import LoggerMixin
from app.database.connection import get_database_manager

# Pydantic models for request/response
class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    remember_me: bool = False

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int
    user: dict
    csrf_token: str

class RefreshTokenRequest(BaseModel):
    refresh_token: str

class RefreshTokenResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int
    csrf_token: str

class UserResponse(BaseModel):
    id: str
    email: str
    first_name: str
    last_name: str
    full_name: str
    role: dict
    permissions: list
    last_login_at: Optional[datetime] = None
    is_email_verified: bool

class CSRFTokenResponse(BaseModel):
    csrf_token: str
    expires_in: int

class LogoutResponse(BaseModel):
    message: str
    success: bool = True

# Authentication router
auth_router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])

# Security dependencies
bearer_scheme = HTTPBearer(auto_error=False)


class AuthAPI(LoggerMixin):
    """Authentication API implementation"""
    
    def __init__(self):
        super().__init__()
        self.security_service = SecurityService()
        self.auth_settings = get_auth_settings()


# Global API instance
auth_api = AuthAPI()


@auth_router.post("/login", response_model=LoginResponse)
async def login(
    request: Request,
    response: Response,
    login_data: LoginRequest,
    db=Depends(get_database_manager)
):
    """
    Authenticate user and return JWT tokens
    
    Features:
    - Email/password authentication
    - Account lockout protection
    - Device/session tracking
    - Secure cookie handling
    - Audit logging
    """
    client_ip = auth_api.security_service.get_client_ip(request)
    user_agent = auth_api.security_service.get_user_agent(request)
    request_id = str(uuid.uuid4())
    
    try:
        # Authenticate user
        user = await auth_api.security_service.authenticate_user(
            db=db,
            email=login_data.email,
            password=login_data.password,
            client_ip=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        # Generate tokens
        access_token = await auth_api.security_service.create_access_token(
            data={"sub": str(user.id), "email": user.email}
        )
        
        refresh_token = await auth_api.security_service.create_refresh_token(
            db=db,
            user_id=user.id,
            device_info=user_agent,
            ip_address=client_ip
        )
        
        # Generate CSRF token
        csrf_token = await auth_api.security_service.create_csrf_token(
            db=db,
            user_id=user.id,
            session_id=request_id,
            ip_address=client_ip
        )
        
        # Set secure cookies
        if login_data.remember_me:
            # Set refresh token in HTTP-only cookie
            response.set_cookie(
                key="refresh_token",
                value=refresh_token,
                max_age=int(auth_api.auth_settings.jwt_refresh_token_expire_timedelta.total_seconds()),
                httponly=True,
                secure=auth_api.auth_settings.session_cookie_secure,
                samesite=auth_api.auth_settings.session_cookie_samesite
            )
        
        # Set CSRF token in cookie
        response.set_cookie(
            key="csrf_token",
            value=csrf_token,
            max_age=int(auth_api.auth_settings.csrf_token_expire_timedelta.total_seconds()),
            httponly=True,
            secure=auth_api.auth_settings.session_cookie_secure,
            samesite=auth_api.auth_settings.session_cookie_samesite
        )
        
        # Update last login
        await auth_api.security_service.update_last_login(
            db=db,
            user_id=user.id,
            ip_address=client_ip
        )
        
        # Audit log
        await auth_api.security_service.create_audit_log(
            db=db,
            user_id=user.id,
            event_type="login_success",
            event_category="auth",
            event_description=f"User {user.email} logged in successfully",
            result="success",
            ip_address=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        auth_api.logger.info(f"User {user.email} logged in successfully from {client_ip}")
        
        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token if not login_data.remember_me else None,
            token_type="bearer",
            expires_in=int(auth_api.auth_settings.jwt_access_token_expire_timedelta.total_seconds()),
            user={
                "id": str(user.id),
                "email": user.email,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "full_name": user.full_name,
                "role": {
                    "id": str(user.role.id),
                    "name": user.role.name,
                    "display_name": user.role.display_name
                } if user.role else None
            },
            csrf_token=csrf_token
        )
        
    except AccountLockedError as e:
        # Audit log for locked account attempt
        await auth_api.security_service.create_audit_log(
            db=db,
            user_id=None,
            event_type="login_blocked_locked",
            event_category="security",
            event_description=f"Login attempt on locked account: {login_data.email}",
            result="failure",
            error_message=str(e),
            ip_address=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        auth_api.logger.warning(f"Login attempt on locked account {login_data.email} from {client_ip}")
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail="Account is temporarily locked due to too many failed login attempts"
        )
        
    except InvalidCredentialsError as e:
        # Audit log for invalid credentials
        await auth_api.security_service.create_audit_log(
            db=db,
            user_id=None,
            event_type="login_failed_credentials",
            event_category="security", 
            event_description=f"Invalid login attempt for email: {login_data.email}",
            result="failure",
            error_message="Invalid credentials",
            ip_address=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        auth_api.logger.warning(f"Invalid login attempt for {login_data.email} from {client_ip}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
        
    except Exception as e:
        # Audit log for general error
        await auth_api.security_service.create_audit_log(
            db=db,
            user_id=None,
            event_type="login_error",
            event_category="auth",
            event_description=f"Login error for email: {login_data.email}",
            result="error",
            error_message=str(e),
            ip_address=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        auth_api.logger.error(f"Login error for {login_data.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during authentication"
        )


@auth_router.post("/refresh", response_model=RefreshTokenResponse)
async def refresh_token(
    request: Request,
    response: Response,
    refresh_data: RefreshTokenRequest = None,
    db=Depends(get_database_manager)
):
    """
    Refresh access token using refresh token
    
    Features:
    - Token rotation for security
    - Cookie-based refresh token support
    - Device validation
    - Audit logging
    """
    client_ip = auth_api.security_service.get_client_ip(request)
    user_agent = auth_api.security_service.get_user_agent(request)
    request_id = str(uuid.uuid4())
    
    # Get refresh token from request body or cookie
    refresh_token = None
    if refresh_data and refresh_data.refresh_token:
        refresh_token = refresh_data.refresh_token
    else:
        refresh_token = request.cookies.get("refresh_token")
    
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token is required"
        )
    
    try:
        # Validate and refresh token
        result = await auth_api.security_service.refresh_access_token(
            db=db,
            refresh_token=refresh_token,
            client_ip=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        # Generate new CSRF token
        csrf_token = await auth_api.security_service.create_csrf_token(
            db=db,
            user_id=result["user_id"],
            session_id=request_id,
            ip_address=client_ip
        )
        
        # Update cookies if token was rotated
        if result.get("new_refresh_token"):
            response.set_cookie(
                key="refresh_token",
                value=result["new_refresh_token"],
                max_age=int(auth_api.auth_settings.jwt_refresh_token_expire_timedelta.total_seconds()),
                httponly=True,
                secure=auth_api.auth_settings.session_cookie_secure,
                samesite=auth_api.auth_settings.session_cookie_samesite
            )
        
        # Update CSRF token cookie
        response.set_cookie(
            key="csrf_token",
            value=csrf_token,
            max_age=int(auth_api.auth_settings.csrf_token_expire_timedelta.total_seconds()),
            httponly=True,
            secure=auth_api.auth_settings.session_cookie_secure,
            samesite=auth_api.auth_settings.session_cookie_samesite
        )
        
        return RefreshTokenResponse(
            access_token=result["access_token"],
            refresh_token=result.get("new_refresh_token"),
            token_type="bearer",
            expires_in=int(auth_api.auth_settings.jwt_access_token_expire_timedelta.total_seconds()),
            csrf_token=csrf_token
        )
        
    except (TokenExpiredError, InvalidTokenError) as e:
        auth_api.logger.warning(f"Invalid refresh token attempt from {client_ip}: {str(e)}")
        
        # Clear invalid cookies
        response.delete_cookie("refresh_token")
        response.delete_cookie("csrf_token")
        
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token"
        )
        
    except Exception as e:
        auth_api.logger.error(f"Token refresh error from {client_ip}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during token refresh"
        )


@auth_router.post("/logout", response_model=LogoutResponse)
async def logout(
    request: Request,
    response: Response,
    auth: AuthContext = Depends(get_current_user_optional),
    db=Depends(get_database_manager)
):
    """
    Logout user and revoke tokens
    
    Features:
    - Token revocation
    - Cookie cleanup
    - Session termination
    - Audit logging
    """
    client_ip = auth_api.security_service.get_client_ip(request)
    user_agent = auth_api.security_service.get_user_agent(request)
    request_id = str(uuid.uuid4())
    
    user_id = auth.user.id if auth and auth.user else None
    user_email = auth.user.email if auth and auth.user else "unknown"
    
    try:
        # Get refresh token from cookie
        refresh_token = request.cookies.get("refresh_token")
        
        if refresh_token:
            # Revoke refresh token
            await auth_api.security_service.revoke_refresh_token(
                db=db,
                refresh_token=refresh_token,
                reason="logout"
            )
        
        # Clear cookies
        response.delete_cookie("refresh_token")
        response.delete_cookie("csrf_token")
        
        # Audit log
        await auth_api.security_service.create_audit_log(
            db=db,
            user_id=user_id,
            event_type="logout",
            event_category="auth",
            event_description=f"User {user_email} logged out",
            result="success",
            ip_address=client_ip,
            user_agent=user_agent,
            request_id=request_id
        )
        
        auth_api.logger.info(f"User {user_email} logged out from {client_ip}")
        
        return LogoutResponse(
            message="Successfully logged out",
            success=True
        )
        
    except Exception as e:
        auth_api.logger.error(f"Logout error for user {user_email}: {str(e)}", exc_info=True)
        
        # Still clear cookies even if there's an error
        response.delete_cookie("refresh_token")
        response.delete_cookie("csrf_token")
        
        return LogoutResponse(
            message="Logged out with warnings",
            success=True
        )


@auth_router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    auth: AuthContext = Depends(get_current_user)
):
    """
    Get current authenticated user information
    
    Features:
    - Full user profile
    - Role and permissions
    - Last login tracking
    """
    try:
        return UserResponse(
            id=str(auth.user.id),
            email=auth.user.email,
            first_name=auth.user.first_name,
            last_name=auth.user.last_name,
            full_name=auth.user.full_name,
            role={
                "id": str(auth.user.role.id),
                "name": auth.user.role.name,
                "display_name": auth.user.role.display_name,
                "description": auth.user.role.description
            } if auth.user.role else None,
            permissions=[
                {
                    "id": str(perm.id),
                    "name": perm.name,
                    "resource": perm.resource,
                    "action": perm.action,
                    "description": perm.description
                }
                for perm in auth.user.role.permissions if auth.user.role
            ],
            last_login_at=auth.user.last_login_at,
            is_email_verified=auth.user.is_email_verified
        )
        
    except Exception as e:
        auth_api.logger.error(f"Error getting user info for {auth.user.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving user information"
        )


@auth_router.get("/csrf-token", response_model=CSRFTokenResponse)
async def get_csrf_token(
    request: Request,
    response: Response,
    auth: AuthContext = Depends(get_current_user_optional),
    db=Depends(get_database_manager)
):
    """
    Generate CSRF token for form protection
    
    Features:
    - Double-submit pattern
    - Session binding
    - Automatic cookie setting
    """
    client_ip = auth_api.security_service.get_client_ip(request)
    user_id = auth.user.id if auth and auth.user else None
    request_id = str(uuid.uuid4())
    
    try:
        # Generate CSRF token
        csrf_token = await auth_api.security_service.create_csrf_token(
            db=db,
            user_id=user_id,
            session_id=request_id,
            ip_address=client_ip
        )
        
        # Set CSRF token in cookie
        response.set_cookie(
            key="csrf_token",
            value=csrf_token,
            max_age=int(auth_api.auth_settings.csrf_token_expire_timedelta.total_seconds()),
            httponly=True,
            secure=auth_api.auth_settings.session_cookie_secure,
            samesite=auth_api.auth_settings.session_cookie_samesite
        )
        
        return CSRFTokenResponse(
            csrf_token=csrf_token,
            expires_in=int(auth_api.auth_settings.csrf_token_expire_timedelta.total_seconds())
        )
        
    except Exception as e:
        auth_api.logger.error(f"Error generating CSRF token: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error generating CSRF token"
        )


# Import dependencies for proper loading
from app.auth.dependencies import get_current_user
