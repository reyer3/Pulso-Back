"""
🔐 Authentication and Authorization Models
Complete user management system with roles and permissions

Features:
- User model with secure password hashing
- Role-based access control (RBAC)
- Granular permissions system  
- JWT refresh token management
- CSRF token validation
- Audit logging for security events
"""

from datetime import datetime, timezone
from typing import Optional, List
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, Table, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()

# Many-to-many association table for roles and permissions
role_permissions = Table(
    'role_permissions',
    Base.metadata,
    Column('role_id', UUID(as_uuid=True), ForeignKey('roles.id', ondelete='CASCADE')),
    Column('permission_id', UUID(as_uuid=True), ForeignKey('permissions.id', ondelete='CASCADE')),
    UniqueConstraint('role_id', 'permission_id', name='unique_role_permission')
)


class User(Base):
    """
    User model with comprehensive authentication features
    
    Supports:
    - Secure password storage with bcrypt
    - Role-based authorization
    - Account status management
    - Profile information
    - Audit tracking
    """
    __tablename__ = 'users'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    dni: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, unique=True)
    phone: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    
    # Account status and security
    status: Mapped[str] = mapped_column(String(20), nullable=False, default='active')  # active, inactive, suspended
    is_email_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    email_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Role relationship
    role_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey('roles.id'), nullable=False)
    role: Mapped["Role"] = relationship("Role", back_populates="users")
    
    # Security tracking
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_login_ip: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)  # IPv6 support
    failed_login_attempts: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    password_changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Audit fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    updated_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    
    # Relationships
    refresh_tokens: Mapped[List["RefreshToken"]] = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")
    audit_logs: Mapped[List["AuditLog"]] = relationship("AuditLog", foreign_keys="AuditLog.user_id", back_populates="user")
    
    @property
    def full_name(self) -> str:
        """Get user's full name"""
        return f"{self.first_name} {self.last_name}".strip()
    
    @property
    def is_locked(self) -> bool:
        """Check if account is currently locked"""
        if self.locked_until is None:
            return False
        return datetime.now(timezone.utc) < self.locked_until
    
    @property
    def is_active(self) -> bool:
        """Check if user account is active and not locked"""
        return self.status == 'active' and not self.is_locked
    
    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, role={self.role.name if self.role else 'None'})>"


class Role(Base):
    """
    Role model for hierarchical access control
    
    Features:
    - System vs custom roles
    - Descriptive metadata
    - Permission assignment
    - Audit tracking
    """
    __tablename__ = 'roles'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)  # e.g., 'superadmin', 'analyst'
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., 'Super Administrator'
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # System role protection
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)  # Prevent deletion of critical roles
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Audit fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    updated_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    
    # Relationships
    users: Mapped[List[User]] = relationship("User", back_populates="role")
    permissions: Mapped[List["Permission"]] = relationship("Permission", secondary=role_permissions, back_populates="roles")
    
    def __repr__(self):
        return f"<Role(id={self.id}, name={self.name}, display_name={self.display_name})>"


class Permission(Base):
    """
    Permission model for granular access control
    
    Format: resource.action (e.g., 'user.read', 'dashboard.view', 'reports.export')
    """
    __tablename__ = 'permissions'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)  # e.g., 'user.read'
    resource: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g., 'user', 'dashboard'
    action: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g., 'read', 'write', 'delete'
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # System permission protection
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Audit fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Relationships
    roles: Mapped[List[Role]] = relationship("Role", secondary=role_permissions, back_populates="permissions")
    
    def __repr__(self):
        return f"<Permission(id={self.id}, name={self.name}, resource={self.resource}, action={self.action})>"


class RefreshToken(Base):
    """
    JWT Refresh Token model for secure token management
    
    Features:
    - Token rotation and revocation
    - Device/session tracking
    - Expiration management
    - Security audit trail
    """
    __tablename__ = 'refresh_tokens'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_hash: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Token metadata
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_revoked: Mapped[bool] = mapped_column(Boolean, default=False)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_reason: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # logout, expired, security_breach
    
    # Device/session tracking
    device_info: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # User agent, device details
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    
    # Audit fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    user: Mapped[User] = relationship("User", back_populates="refresh_tokens")
    
    @property
    def is_expired(self) -> bool:
        """Check if token is expired"""
        return datetime.now(timezone.utc) >= self.expires_at
    
    @property
    def is_valid(self) -> bool:
        """Check if token is valid (not expired and not revoked)"""
        return not self.is_expired and not self.is_revoked
    
    def __repr__(self):
        return f"<RefreshToken(id={self.id}, user_id={self.user_id}, expires_at={self.expires_at}, is_revoked={self.is_revoked})>"


class CSRFToken(Base):
    """
    CSRF Token model for Cross-Site Request Forgery protection
    
    Features:
    - Double submit cookie pattern
    - Token rotation
    - Expiration management
    - Session binding
    """
    __tablename__ = 'csrf_tokens'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_hash: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=True)
    
    # Token metadata
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_used: Mapped[bool] = mapped_column(Boolean, default=False)
    used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Session tracking
    session_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    
    # Audit fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    @property
    def is_expired(self) -> bool:
        """Check if token is expired"""
        return datetime.now(timezone.utc) >= self.expires_at
    
    @property
    def is_valid(self) -> bool:
        """Check if token is valid (not expired and not used)"""
        return not self.is_expired and not self.is_used
    
    def __repr__(self):
        return f"<CSRFToken(id={self.id}, user_id={self.user_id}, expires_at={self.expires_at})>"


class AuditLog(Base):
    """
    Audit log model for security and compliance tracking
    
    Features:
    - Comprehensive event logging
    - User action tracking
    - IP and device information
    - Security event correlation
    """
    __tablename__ = 'audit_logs'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='SET NULL'), nullable=True)
    
    # Event details
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)  # login, logout, user_created, role_assigned, etc.
    event_category: Mapped[str] = mapped_column(String(30), nullable=False)  # auth, user_mgmt, role_mgmt, security
    event_description: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Result and status
    result: Mapped[str] = mapped_column(String(20), nullable=False)  # success, failure, error
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Context information
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    request_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Target resource (what was affected)
    target_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # user, role, permission
    target_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    target_details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON with additional context
    
    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    
    # Relationships
    user: Mapped[Optional[User]] = relationship("User", foreign_keys=[user_id], back_populates="audit_logs")
    
    def __repr__(self):
        return f"<AuditLog(id={self.id}, event_type={self.event_type}, result={self.result}, created_at={self.created_at})>"


# Indexes for performance optimization
from sqlalchemy import Index

# User indexes
Index('idx_users_email_status', User.email, User.status)
Index('idx_users_role_status', User.role_id, User.status)
Index('idx_users_last_login', User.last_login_at)

# Role indexes
Index('idx_roles_name_active', Role.name, Role.is_active)

# Permission indexes
Index('idx_permissions_resource_action', Permission.resource, Permission.action)
Index('idx_permissions_name_active', Permission.name, Permission.is_active)

# Refresh token indexes
Index('idx_refresh_tokens_user_valid', RefreshToken.user_id, RefreshToken.is_revoked, RefreshToken.expires_at)

# CSRF token indexes
Index('idx_csrf_tokens_user_valid', CSRFToken.user_id, CSRFToken.expires_at)

# Audit log indexes
Index('idx_audit_logs_user_event', AuditLog.user_id, AuditLog.event_type, AuditLog.created_at)
Index('idx_audit_logs_event_category', AuditLog.event_category, AuditLog.created_at)
Index('idx_audit_logs_ip_created', AuditLog.ip_address, AuditLog.created_at)
