"""
🔐 JWT Token Management and Security Services
Comprehensive JWT handling with refresh tokens, CSRF protection, and security features

Features:
- Access and refresh token generation/validation
- Token rotation and revocation
- CSRF token management with double-submit pattern
- Password hashing with bcrypt
- Security event logging
- Rate limiting and account lockout
"""

import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple
from jose import JWTError, jwt
import bcrypt
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload

from app.auth.models import User, RefreshToken, CSRFToken, AuditLog, Role, Permission
from app.auth.config import get_auth_settings, get_client_ip, get_user_agent, mask_sensitive_data
from app.database.connection import get_database_manager
from app.core.logging import LoggerMixin


class SecurityService(LoggerMixin):
    """
    Core security service for authentication and authorization
    
    Handles:
    - Password hashing and verification
    - JWT token operations
    - CSRF protection
    - Security auditing
    - Account security
    """
    
    def __init__(self):
        super().__init__()
        self.settings = get_auth_settings()
    
    # =============================================================================
    # PASSWORD MANAGEMENT
    # =============================================================================
    
    def hash_password(self, password: str) -> str:
        """
        Hash password using bcrypt with configurable rounds
        
        Args:
            password: Plain text password
            
        Returns:
            str: Hashed password
        """
        salt = bcrypt.gensalt(rounds=self.settings.bcrypt_rounds)
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    
    def verify_password(self, password: str, hashed_password: str) -> bool:
        """
        Verify password against hash
        
        Args:
            password: Plain text password
            hashed_password: Stored password hash
            
        Returns:
            bool: True if password is correct
        """
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))
        except Exception as e:
            self.logger.error(f"Password verification error: {e}")
            return False
    
    # =============================================================================
    # JWT TOKEN MANAGEMENT
    # =============================================================================
    
    def create_access_token(self, user_id: str, additional_claims: Dict[str, Any] = None) -> str:
        """
        Create JWT access token
        
        Args:
            user_id: User identifier
            additional_claims: Extra claims to include
            
        Returns:
            str: JWT access token
        """
        now = datetime.now(timezone.utc)
        expires_at = now + self.settings.jwt_access_token_expire_timedelta
        
        claims = {
            "sub": str(user_id),
            "iat": now,
            "exp": expires_at,
            "type": "access",
            "jti": secrets.token_hex(16)  # JWT ID for tracking
        }
        
        if additional_claims:
            claims.update(additional_claims)
        
        token = jwt.encode(claims, self.settings.jwt_secret_key, algorithm=self.settings.jwt_algorithm)
        return token
    
    def create_refresh_token(self, user_id: str) -> Tuple[str, str]:
        """
        Create refresh token and return both raw token and hash
        
        Args:
            user_id: User identifier
            
        Returns:
            tuple: (raw_token, token_hash)
        """
        # Generate a cryptographically secure random token
        raw_token = secrets.token_urlsafe(64)
        
        # Hash the token for storage
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        
        return raw_token, token_hash
    
    def verify_access_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Verify and decode JWT access token
        
        Args:
            token: JWT token to verify
            
        Returns:
            Optional[Dict]: Token payload if valid, None otherwise
        """
        try:
            payload = jwt.decode(
                token,
                self.settings.jwt_secret_key,
                algorithms=[self.settings.jwt_algorithm]
            )
            
            # Verify token type
            if payload.get("type") != "access":
                return None
            
            return payload
            
        except JWTError as e:
            self.logger.debug(f"JWT verification failed: {e}")
            return None
    
    def extract_token_from_header(self, authorization_header: str) -> Optional[str]:
        """
        Extract JWT token from Authorization header
        
        Args:
            authorization_header: Authorization header value
            
        Returns:
            Optional[str]: JWT token if valid format
        """
        if not authorization_header:
            return None
        
        parts = authorization_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        
        return parts[1]
    
    # =============================================================================
    # CSRF TOKEN MANAGEMENT
    # =============================================================================
    
    def generate_csrf_token(self) -> str:
        """
        Generate CSRF token for double-submit pattern
        
        Returns:
            str: CSRF token
        """
        return secrets.token_urlsafe(32)
    
    def hash_csrf_token(self, token: str) -> str:
        """
        Hash CSRF token for storage
        
        Args:
            token: Raw CSRF token
            
        Returns:
            str: Hashed token
        """
        return hashlib.sha256(token.encode()).hexdigest()
    
    def verify_csrf_token(self, token: str, token_hash: str) -> bool:
        """
        Verify CSRF token against stored hash
        
        Args:
            token: Raw CSRF token from request
            token_hash: Stored token hash
            
        Returns:
            bool: True if token is valid
        """
        calculated_hash = self.hash_csrf_token(token)
        return secrets.compare_digest(calculated_hash, token_hash)
    
    # =============================================================================
    # ACCOUNT SECURITY
    # =============================================================================
    
    async def check_account_lockout(self, user: User) -> bool:
        """
        Check if user account is locked due to failed login attempts
        
        Args:
            user: User object to check
            
        Returns:
            bool: True if account is locked
        """
        if user.locked_until is None:
            return False
        
        now = datetime.now(timezone.utc)
        if now >= user.locked_until:
            # Lockout period expired, reset
            await self.reset_failed_login_attempts(user)
            return False
        
        return True
    
    async def increment_failed_login_attempts(self, user: User, session: AsyncSession) -> bool:
        """
        Increment failed login attempts and lock account if threshold reached
        
        Args:
            user: User object
            session: Database session
            
        Returns:
            bool: True if account was locked
        """
        user.failed_login_attempts += 1
        
        if user.failed_login_attempts >= self.settings.rate_limit_login_attempts:
            # Lock the account
            user.locked_until = datetime.now(timezone.utc) + self.settings.account_lockout_timedelta
            await session.commit()
            
            self.logger.warning(
                f"Account locked for user {mask_sensitive_data(user.email)} "
                f"after {user.failed_login_attempts} failed attempts"
            )
            return True
        
        await session.commit()
        return False
    
    async def reset_failed_login_attempts(self, user: User, session: AsyncSession = None) -> None:
        """
        Reset failed login attempts and unlock account
        
        Args:
            user: User object
            session: Database session (optional)
        """
        if session is None:
            db = await get_database_manager()
            async with db.get_session() as session:
                await self._do_reset_failed_attempts(user, session)
        else:
            await self._do_reset_failed_attempts(user, session)
    
    async def _do_reset_failed_attempts(self, user: User, session: AsyncSession) -> None:
        """Internal method to reset failed attempts"""
        user.failed_login_attempts = 0
        user.locked_until = None
        await session.commit()
    
    # =============================================================================
    # TOKEN DATABASE OPERATIONS
    # =============================================================================
    
    async def store_refresh_token(
        self,
        user_id: str,
        token_hash: str,
        expires_at: datetime,
        device_info: str = None,
        ip_address: str = None
    ) -> RefreshToken:
        """
        Store refresh token in database
        
        Args:
            user_id: User identifier
            token_hash: Hashed token
            expires_at: Token expiration
            device_info: Device information
            ip_address: Client IP address
            
        Returns:
            RefreshToken: Created token record
        """
        db = await get_database_manager()
        async with db.get_session() as session:
            refresh_token = RefreshToken(
                token_hash=token_hash,
                user_id=user_id,
                expires_at=expires_at,
                device_info=device_info,
                ip_address=ip_address
            )
            
            session.add(refresh_token)
            await session.commit()
            await session.refresh(refresh_token)
            
            return refresh_token
    
    async def get_refresh_token(self, token_hash: str) -> Optional[RefreshToken]:
        """
        Get refresh token by hash
        
        Args:
            token_hash: Hashed token to find
            
        Returns:
            Optional[RefreshToken]: Token record if found
        """
        db = await get_database_manager()
        async with db.get_session() as session:
            stmt = select(RefreshToken).where(
                RefreshToken.token_hash == token_hash,
                RefreshToken.is_revoked == False
            ).options(selectinload(RefreshToken.user))
            
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
    
    async def revoke_refresh_token(self, token_hash: str, reason: str = "logout") -> bool:
        """
        Revoke refresh token
        
        Args:
            token_hash: Token hash to revoke
            reason: Revocation reason
            
        Returns:
            bool: True if token was revoked
        """
        db = await get_database_manager()
        async with db.get_session() as session:
            stmt = update(RefreshToken).where(
                RefreshToken.token_hash == token_hash
            ).values(
                is_revoked=True,
                revoked_at=datetime.now(timezone.utc),
                revoked_reason=reason
            )
            
            result = await session.execute(stmt)
            await session.commit()
            
            return result.rowcount > 0
    
    async def revoke_all_user_tokens(self, user_id: str, reason: str = "security") -> int:
        """
        Revoke all refresh tokens for a user
        
        Args:
            user_id: User identifier
            reason: Revocation reason
            
        Returns:
            int: Number of tokens revoked
        """
        db = await get_database_manager()
        async with db.get_session() as session:
            stmt = update(RefreshToken).where(
                RefreshToken.user_id == user_id,
                RefreshToken.is_revoked == False
            ).values(
                is_revoked=True,
                revoked_at=datetime.now(timezone.utc),
                revoked_reason=reason
            )
            
            result = await session.execute(stmt)
            await session.commit()
            
            return result.rowcount
    
    async def cleanup_expired_tokens(self) -> int:
        """
        Clean up expired tokens from database
        
        Returns:
            int: Number of tokens cleaned up
        """
        db = await get_database_manager()
        async with db.get_session() as session:
            now = datetime.now(timezone.utc)
            
            # Delete expired refresh tokens
            refresh_stmt = delete(RefreshToken).where(
                RefreshToken.expires_at < now
            )
            
            # Delete expired CSRF tokens
            csrf_stmt = delete(CSRFToken).where(
                CSRFToken.expires_at < now
            )
            
            refresh_result = await session.execute(refresh_stmt)
            csrf_result = await session.execute(csrf_stmt)
            await session.commit()
            
            total_cleaned = refresh_result.rowcount + csrf_result.rowcount
            
            if total_cleaned > 0:
                self.logger.info(f"Cleaned up {total_cleaned} expired tokens")
            
            return total_cleaned
    
    # =============================================================================
    # AUDIT LOGGING
    # =============================================================================
    
    async def log_security_event(
        self,
        event_type: str,
        event_description: str,
        user_id: str = None,
        result: str = "success",
        error_message: str = None,
        ip_address: str = None,
        user_agent: str = None,
        target_type: str = None,
        target_id: str = None,
        additional_details: str = None,
        request_id: str = None
    ) -> None:
        """
        Log security event for audit trail
        
        Args:
            event_type: Type of event (login, logout, user_created, etc.)
            event_description: Human-readable description
            user_id: User involved in event
            result: Event result (success, failure, error)
            error_message: Error details if applicable
            ip_address: Client IP address
            user_agent: Client user agent
            target_type: Type of target resource
            target_id: ID of target resource
            additional_details: Additional context (JSON)
            request_id: Request correlation ID
        """
        try:
            db = await get_database_manager()
            async with db.get_session() as session:
                audit_log = AuditLog(
                    user_id=user_id,
                    event_type=event_type,
                    event_category=self._determine_event_category(event_type),
                    event_description=event_description,
                    result=result,
                    error_message=error_message,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    target_type=target_type,
                    target_id=target_id,
                    target_details=additional_details,
                    request_id=request_id
                )
                
                session.add(audit_log)
                await session.commit()
                
        except Exception as e:
            self.logger.error(f"Failed to log security event: {e}")
    
    def _determine_event_category(self, event_type: str) -> str:
        """Determine event category from event type"""
        auth_events = ['login', 'logout', 'token_refresh', 'password_change']
        user_mgmt_events = ['user_created', 'user_updated', 'user_deleted', 'user_locked']
        role_mgmt_events = ['role_created', 'role_updated', 'role_deleted', 'role_assigned']
        security_events = ['account_locked', 'suspicious_login', 'csrf_violation']
        
        if event_type in auth_events:
            return 'auth'
        elif event_type in user_mgmt_events:
            return 'user_mgmt'
        elif event_type in role_mgmt_events:
            return 'role_mgmt'
        elif event_type in security_events:
            return 'security'
        else:
            return 'general'


# Global service instance
_security_service: Optional[SecurityService] = None


def get_security_service() -> SecurityService:
    """Get singleton security service instance"""
    global _security_service
    if _security_service is None:
        _security_service = SecurityService()
    return _security_service
