# app/services/user_service.py
"""
👥 UserService - Servicio de gestión de usuarios
Lógica de negocio para operaciones de usuarios, validaciones y seguridad
"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

import bcrypt
from pydantic import BaseModel, EmailStr, Field, validator

from app.repositories.user_repo import UserRepository
from app.repositories.cache_repo import CacheRepository
from shared.core.config import settings


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class UserCreate(BaseModel):
    """Modelo para crear usuario"""
    email: EmailStr
    password: str = Field(..., min_length=8)
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    role: str = Field(default="viewer")

    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one digit')
        return v

    @validator('role')
    def validate_role(cls, v):
        allowed_roles = ['admin', 'manager', 'analyst', 'viewer']
        if v not in allowed_roles:
            raise ValueError(f'Role must be one of: {", ".join(allowed_roles)}')
        return v


class UserUpdate(BaseModel):
    """Modelo para actualizar usuario"""
    first_name: Optional[str] = Field(None, min_length=1, max_length=50)
    last_name: Optional[str] = Field(None, min_length=1, max_length=50)
    role: Optional[str] = None
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None

    @validator('role')
    def validate_role(cls, v):
        if v is not None:
            allowed_roles = ['admin', 'manager', 'analyst', 'viewer']
            if v not in allowed_roles:
                raise ValueError(f'Role must be one of: {", ".join(allowed_roles)}')
        return v


class UserResponse(BaseModel):
    """Modelo de respuesta de usuario (sin datos sensibles)"""
    id: str
    email: str
    first_name: str
    last_name: str
    role: str
    is_active: bool
    is_verified: bool
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


class UserListResponse(BaseModel):
    """Respuesta paginada de usuarios"""
    users: List[UserResponse]
    total: int
    page: int
    per_page: int
    total_pages: int


class UserStats(BaseModel):
    """Estadísticas de usuarios"""
    total_users: int
    verified_users: int
    active_last_30_days: int
    active_last_7_days: int
    new_last_30_days: int
    users_by_role: List[Dict[str, Any]]


# =============================================================================
# USER SERVICE
# =============================================================================

class UserService:
    """
    Servicio de gestión de usuarios
    
    Maneja toda la lógica de negocio relacionada con usuarios:
    - Creación y validación
    - Autenticación y autorización
    - Gestión de roles y permisos
    - Estadísticas y reportes
    """

    def __init__(self, user_repo: UserRepository, cache_repo: Optional[CacheRepository] = None):
        self.user_repo = user_repo
        self.cache_repo = cache_repo
        self.cache_ttl = 300  # 5 minutes

    # =============================================================================
    # USER CRUD OPERATIONS
    # =============================================================================

    async def create_user(self, user_data: UserCreate, created_by: Optional[str] = None) -> UserResponse:
        """
        Crear un nuevo usuario con validaciones completas
        """
        # Verificar que el email no existe
        if await self.user_repo.check_email_exists(user_data.email):
            raise ValueError(f"Email {user_data.email} already exists")

        # Hash de password
        password_hash = self._hash_password(user_data.password)

        # Preparar datos para crear
        create_data = {
            'email': user_data.email,
            'password_hash': password_hash,
            'first_name': user_data.first_name,
            'last_name': user_data.last_name,
            'role': user_data.role,
            'is_active': True,
            'is_verified': False  # Requiere verificación por email
        }

        # Crear usuario
        user_record = await self.user_repo.create_user(create_data)
        
        # Limpiar cache
        await self._invalidate_user_cache()

        # Log de auditoría
        await self._log_user_action(
            user_id=user_record['id'],
            action='user_created',
            performed_by=created_by,
            details={'email': user_data.email, 'role': user_data.role}
        )

        return UserResponse(**user_record)

    async def get_user(self, user_id: str) -> Optional[UserResponse]:
        """
        Obtener usuario por ID
        """
        user_record = await self.user_repo.get_user_by_id(user_id)
        return UserResponse(**user_record) if user_record else None

    async def get_users(
        self,
        page: int = 1,
        per_page: int = 20,
        role_filter: Optional[str] = None,
        search: Optional[str] = None
    ) -> UserListResponse:
        """
        Obtener lista paginada de usuarios con filtros
        """
        # Calcular offset
        skip = (page - 1) * per_page

        # Obtener usuarios
        users_data = await self.user_repo.get_all_users(
            skip=skip,
            limit=per_page,
            role_filter=role_filter,
            search=search
        )

        # Contar total (simplificado - en producción usar query separado)
        total_users = len(await self.user_repo.get_all_users(skip=0, limit=1000))
        total_pages = (total_users + per_page - 1) // per_page

        users = [UserResponse(**user) for user in users_data]

        return UserListResponse(
            users=users,
            total=total_users,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )

    async def update_user(
        self,
        user_id: str,
        update_data: UserUpdate,
        updated_by: Optional[str] = None
    ) -> Optional[UserResponse]:
        """
        Actualizar usuario con validaciones
        """
        # Verificar que el usuario existe
        existing_user = await self.user_repo.get_user_by_id(user_id)
        if not existing_user:
            raise ValueError(f"User with ID {user_id} not found")

        # Preparar datos para actualizar (solo campos no None)
        update_dict = update_data.dict(exclude_unset=True)

        if not update_dict:
            return UserResponse(**existing_user)  # Nothing to update

        # Actualizar usuario
        updated_user = await self.user_repo.update_user(user_id, update_dict)
        
        if not updated_user:
            raise ValueError("Failed to update user")

        # Limpiar cache
        await self._invalidate_user_cache(user_id)

        # Log de auditoría
        await self._log_user_action(
            user_id=user_id,
            action='user_updated',
            performed_by=updated_by,
            details=update_dict
        )

        return UserResponse(**updated_user)

    async def delete_user(self, user_id: str, deleted_by: Optional[str] = None) -> bool:
        """
        Eliminación suave de usuario
        """
        # Verificar que el usuario existe
        existing_user = await self.user_repo.get_user_by_id(user_id)
        if not existing_user:
            raise ValueError(f"User with ID {user_id} not found")

        # No permitir auto-eliminación
        if deleted_by == user_id:
            raise ValueError("Users cannot delete themselves")

        # Soft delete
        success = await self.user_repo.soft_delete_user(user_id)
        
        if success:
            # Limpiar cache
            await self._invalidate_user_cache(user_id)

            # Log de auditoría
            await self._log_user_action(
                user_id=user_id,
                action='user_deleted',
                performed_by=deleted_by,
                details={'email': existing_user['email']}
            )

        return success

    # =============================================================================
    # AUTHENTICATION & AUTHORIZATION
    # =============================================================================

    async def authenticate_user(self, email: str, password: str) -> Optional[UserResponse]:
        """
        Autenticar usuario con email y password
        """
        # Obtener usuario con hash de password
        user_record = await self.user_repo.get_user_by_email(email)
        
        if not user_record:
            return None

        # Verificar password
        if not self._verify_password(password, user_record['password_hash']):
            return None

        # Verificar que el usuario esté activo
        if not user_record['is_active']:
            raise ValueError("User account is deactivated")

        # Actualizar último login
        await self.user_repo.update_last_login(user_record['id'])

        # Log de auditoría
        await self._log_user_action(
            user_id=user_record['id'],
            action='user_login',
            details={'email': email}
        )

        # Remover password_hash antes de devolver
        user_record.pop('password_hash', None)
        return UserResponse(**user_record)

    async def change_password(
        self,
        user_id: str,
        current_password: str,
        new_password: str
    ) -> bool:
        """
        Cambiar password de usuario
        """
        # Obtener usuario con hash actual
        user_record = await self.user_repo.get_user_by_email_for_auth(user_id)
        
        if not user_record:
            raise ValueError("User not found")

        # Verificar password actual
        if not self._verify_password(current_password, user_record['password_hash']):
            raise ValueError("Current password is incorrect")

        # Validar nuevo password
        user_create = UserCreate(
            email=user_record['email'],
            password=new_password,
            first_name=user_record['first_name'],
            last_name=user_record['last_name']
        )  # Esto validará el password

        # Hash del nuevo password
        new_password_hash = self._hash_password(new_password)

        # Actualizar password
        await self.user_repo.update_user(user_id, {'password_hash': new_password_hash})

        # Log de auditoría
        await self._log_user_action(
            user_id=user_id,
            action='password_changed'
        )

        return True

    def has_permission(self, user: UserResponse, permission: str) -> bool:
        """
        Verificar si un usuario tiene un permiso específico
        """
        role_permissions = {
            'admin': ['*'],  # All permissions
            'manager': [
                'user.read', 'user.create', 'user.update',
                'dashboard.read', 'reports.read', 'reports.export'
            ],
            'analyst': [
                'dashboard.read', 'reports.read', 'reports.export'
            ],
            'viewer': [
                'dashboard.read'
            ]
        }

        user_permissions = role_permissions.get(user.role, [])
        
        # Admin has all permissions
        if '*' in user_permissions:
            return True
            
        return permission in user_permissions

    # =============================================================================
    # STATISTICS AND ANALYTICS
    # =============================================================================

    async def get_user_statistics(self) -> UserStats:
        """
        Obtener estadísticas generales de usuarios
        """
        cache_key = "user_stats"
        
        # Try cache first
        if self.cache_repo:
            cached_stats = await self.cache_repo.get_from_cache(cache_key)
            if cached_stats:
                return UserStats(**cached_stats)

        # Get stats from database
        activity_stats = await self.user_repo.get_user_activity_stats()
        role_stats = await self.user_repo.get_user_count_by_role()

        stats = UserStats(
            total_users=activity_stats.get('total_users', 0),
            verified_users=activity_stats.get('verified_users', 0),
            active_last_30_days=activity_stats.get('active_last_30_days', 0),
            active_last_7_days=activity_stats.get('active_last_7_days', 0),
            new_last_30_days=activity_stats.get('new_last_30_days', 0),
            users_by_role=role_stats
        )

        # Cache results
        if self.cache_repo:
            await self.cache_repo.set_to_cache(cache_key, stats.dict(), self.cache_ttl)

        return stats

    async def get_recent_users(self, days: int = 30) -> List[UserResponse]:
        """
        Obtener usuarios registrados recientemente
        """
        recent_users = await self.user_repo.get_recent_users(days)
        return [UserResponse(**user) for user in recent_users]

    # =============================================================================
    # BULK OPERATIONS
    # =============================================================================

    async def bulk_update_users(
        self,
        updates: List[Dict[str, Any]],
        updated_by: Optional[str] = None
    ) -> int:
        """
        Actualización masiva de usuarios
        """
        # Validar cada actualización
        validated_updates = []
        for update in updates:
            user_id = update.get('id')
            if not user_id:
                continue
                
            # Validar datos de actualización
            try:
                update_data = UserUpdate(**{k: v for k, v in update.items() if k != 'id'})
                validated_updates.append({
                    'id': user_id,
                    **update_data.dict(exclude_unset=True)
                })
            except Exception as e:
                # Log error but continue with other updates
                print(f"Validation error for user {user_id}: {e}")
                continue

        # Ejecutar actualizaciones
        updated_count = await self.user_repo.bulk_update_users(validated_updates)

        # Limpiar cache
        await self._invalidate_user_cache()

        # Log de auditoría
        await self._log_user_action(
            action='bulk_update',
            performed_by=updated_by,
            details={'updated_count': updated_count, 'total_attempted': len(updates)}
        )

        return updated_count

    async def export_users(self) -> List[Dict[str, Any]]:
        """
        Exportar todos los usuarios para descarga
        """
        users = await self.user_repo.get_users_for_export()
        
        # Transform for export (remove sensitive data, format dates)
        export_data = []
        for user in users:
            export_data.append({
                'ID': user['id'],
                'Email': user['email'],
                'Nombre': user['first_name'],
                'Apellido': user['last_name'],
                'Nombre Completo': f"{user['first_name']} {user['last_name']}",
                'Rol': user['role'],
                'Activo': 'Sí' if user['is_active'] else 'No',
                'Verificado': 'Sí' if user['is_verified'] else 'No',
                'Último Login': user['last_login_at'].strftime('%Y-%m-%d %H:%M:%S') if user['last_login_at'] else 'Nunca',
                'Fecha Creación': user['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'Última Actualización': user['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
            })

        return export_data

    # =============================================================================
    # PRIVATE METHODS
    # =============================================================================

    def _hash_password(self, password: str) -> str:
        """Hash password using bcrypt"""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash"""
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

    async def _invalidate_user_cache(self, user_id: Optional[str] = None):
        """Invalidate user-related cache entries"""
        if not self.cache_repo:
            return

        # Invalidate general cache patterns
        patterns = ['user_stats', 'users_*']
        
        if user_id:
            patterns.append(f'user_{user_id}')

        for pattern in patterns:
            await self.cache_repo.invalidate_cache(pattern)

    async def _log_user_action(
        self,
        action: str,
        user_id: Optional[str] = None,
        performed_by: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log user actions for auditing (implementar según necesidades)"""
        # TODO: Implementar logging de auditoría
        # Esto podría escribir a una tabla de audit_logs o enviar a un servicio de logging
        log_entry = {
            'timestamp': datetime.utcnow(),
            'action': action,
            'user_id': user_id,
            'performed_by': performed_by,
            'details': details or {}
        }
        print(f"AUDIT LOG: {log_entry}")  # Placeholder - reemplazar con logging real


# =============================================================================
# FACTORY FUNCTION FOR DEPENDENCY INJECTION
# =============================================================================

async def get_user_service() -> UserService:
    """Factory function to get UserService instance"""
    user_repo = UserRepository()
    await user_repo.connect()
    
    # Optional: Add cache repository
    # cache_repo = CacheRepository()
    # await cache_repo.connect()
    
    return UserService(user_repo=user_repo, cache_repo=None)
