# app/repositories/user_repo.py
"""
👥 User Repository - Repositorio especializado para gestión de usuarios
Maneja todas las operaciones CRUD de usuarios con PostgreSQL
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from app.repositories.postgres_repo import PostgresRepository
from shared.core.config import settings


class UserRepository(PostgresRepository):
    """
    Repositorio especializado para operaciones de usuarios
    Extiende PostgresRepository para operaciones específicas de usuarios
    """

    def __init__(self):
        super().__init__()
        self.table_name = "users"

    # =============================================================================
    # CRUD OPERATIONS
    # =============================================================================

    async def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crear un nuevo usuario
        """
        user_id = str(uuid4())
        now = datetime.utcnow()

        query = f"""
        INSERT INTO {self.table_name} (
            id, email, password_hash, first_name, last_name, 
            role, is_active, is_verified, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        ) RETURNING *
        """

        params = {
            'id': user_id,
            'email': user_data['email'],
            'password_hash': user_data['password_hash'],
            'first_name': user_data.get('first_name', ''),
            'last_name': user_data.get('last_name', ''),
            'role': user_data.get('role', 'viewer'),
            'is_active': user_data.get('is_active', True),
            'is_verified': user_data.get('is_verified', False),
            'created_at': now,
            'updated_at': now
        }

        result = await self.execute_single(query, params)
        return result

    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtener usuario por ID
        """
        query = f"""
        SELECT 
            id, email, first_name, last_name, role, 
            is_active, is_verified, last_login_at, 
            created_at, updated_at
        FROM {self.table_name} 
        WHERE id = $1 AND is_active = true
        """
        
        params = {'user_id': user_id}
        return await self.execute_single(query, params)

    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Obtener usuario por email (incluye password_hash para autenticación)
        """
        query = f"""
        SELECT 
            id, email, password_hash, first_name, last_name, 
            role, is_active, is_verified, last_login_at,
            created_at, updated_at
        FROM {self.table_name} 
        WHERE email = $1 AND is_active = true
        """
        
        params = {'email': email}
        return await self.execute_single(query, params)

    async def get_all_users(
        self, 
        skip: int = 0, 
        limit: int = 100,
        role_filter: Optional[str] = None,
        search: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Obtener lista de usuarios con filtros y paginación
        """
        where_conditions = ["is_active = true"]
        params = {}
        param_count = 0

        # Filter by role
        if role_filter:
            param_count += 1
            where_conditions.append(f"role = ${param_count}")
            params[f'role'] = role_filter

        # Search by name or email
        if search:
            param_count += 1
            where_conditions.append(f"""
                (first_name ILIKE ${param_count} OR 
                 last_name ILIKE ${param_count} OR 
                 email ILIKE ${param_count})
            """)
            params[f'search'] = f"%{search}%"

        where_clause = " AND ".join(where_conditions)
        
        # Add pagination parameters
        param_count += 1
        params[f'limit'] = limit
        param_count += 1
        params[f'offset'] = skip

        query = f"""
        SELECT 
            id, email, first_name, last_name, role, 
            is_active, is_verified, last_login_at,
            created_at, updated_at
        FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT ${param_count-1} OFFSET ${param_count}
        """

        return await self.execute_query(query, params)

    async def update_user(self, user_id: str, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Actualizar datos de usuario
        """
        # Build dynamic SET clause
        set_clauses = []
        params = {'updated_at': datetime.utcnow()}
        param_count = 1

        for field, value in update_data.items():
            if field not in ['id', 'created_at']:  # Skip immutable fields
                param_count += 1
                set_clauses.append(f"{field} = ${param_count}")
                params[field] = value

        if not set_clauses:
            return None  # Nothing to update

        set_clause = ", ".join(set_clauses)
        param_count += 1
        params['user_id'] = user_id

        query = f"""
        UPDATE {self.table_name}
        SET {set_clause}, updated_at = $1
        WHERE id = ${param_count} AND is_active = true
        RETURNING id, email, first_name, last_name, role, 
                  is_active, is_verified, last_login_at,
                  created_at, updated_at
        """

        return await self.execute_single(query, params)

    async def soft_delete_user(self, user_id: str) -> bool:
        """
        Eliminación suave (soft delete) de usuario
        """
        query = f"""
        UPDATE {self.table_name}
        SET is_active = false, updated_at = $1
        WHERE id = $2 AND is_active = true
        """

        params = {
            'updated_at': datetime.utcnow(),
            'user_id': user_id
        }

        try:
            await self.execute_query(query, params)
            return True
        except Exception:
            return False

    async def update_last_login(self, user_id: str) -> bool:
        """
        Actualizar timestamp de último login
        """
        query = f"""
        UPDATE {self.table_name}
        SET last_login_at = $1, updated_at = $1
        WHERE id = $2 AND is_active = true
        """

        params = {
            'timestamp': datetime.utcnow(),
            'user_id': user_id
        }

        try:
            await self.execute_query(query, params)
            return True
        except Exception:
            return False

    # =============================================================================
    # STATISTICS AND ANALYTICS
    # =============================================================================

    async def get_user_count_by_role(self) -> List[Dict[str, Any]]:
        """
        Obtener conteo de usuarios por rol
        """
        query = f"""
        SELECT 
            role,
            COUNT(*) as count
        FROM {self.table_name}
        WHERE is_active = true
        GROUP BY role
        ORDER BY count DESC
        """

        return await self.execute_query(query)

    async def get_recent_users(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Obtener usuarios registrados recientemente
        """
        query = f"""
        SELECT 
            id, email, first_name, last_name, role,
            created_at, updated_at
        FROM {self.table_name}
        WHERE is_active = true 
            AND created_at >= NOW() - INTERVAL '{days} days'
        ORDER BY created_at DESC
        """

        return await self.execute_query(query)

    async def get_user_activity_stats(self) -> Dict[str, Any]:
        """
        Obtener estadísticas de actividad de usuarios
        """
        query = f"""
        SELECT 
            COUNT(*) as total_users,
            COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_users,
            COUNT(CASE WHEN last_login_at >= NOW() - INTERVAL '30 days' THEN 1 END) as active_last_30_days,
            COUNT(CASE WHEN last_login_at >= NOW() - INTERVAL '7 days' THEN 1 END) as active_last_7_days,
            COUNT(CASE WHEN created_at >= NOW() - INTERVAL '30 days' THEN 1 END) as new_last_30_days
        FROM {self.table_name}
        WHERE is_active = true
        """

        result = await self.execute_single(query)
        return result or {}

    # =============================================================================
    # PERMISSIONS AND ROLES
    # =============================================================================

    async def get_users_by_role(self, role: str) -> List[Dict[str, Any]]:
        """
        Obtener todos los usuarios de un rol específico
        """
        query = f"""
        SELECT 
            id, email, first_name, last_name, role,
            is_active, is_verified, last_login_at,
            created_at, updated_at
        FROM {self.table_name}
        WHERE role = $1 AND is_active = true
        ORDER BY last_name, first_name
        """

        params = {'role': role}
        return await self.execute_query(query, params)

    async def check_email_exists(self, email: str, exclude_user_id: Optional[str] = None) -> bool:
        """
        Verificar si un email ya existe (para validación de unicidad)
        """
        where_clause = "email = $1 AND is_active = true"
        params = {'email': email}

        if exclude_user_id:
            where_clause += " AND id != $2"
            params['exclude_id'] = exclude_user_id

        query = f"""
        SELECT COUNT(*) as count 
        FROM {self.table_name} 
        WHERE {where_clause}
        """

        result = await self.execute_scalar(query, params)
        return result > 0

    # =============================================================================
    # BULK OPERATIONS
    # =============================================================================

    async def bulk_update_users(self, user_updates: List[Dict[str, Any]]) -> int:
        """
        Actualización masiva de usuarios
        """
        updated_count = 0
        
        for update in user_updates:
            user_id = update.pop('id')
            result = await self.update_user(user_id, update)
            if result:
                updated_count += 1

        return updated_count

    async def get_users_for_export(self) -> List[Dict[str, Any]]:
        """
        Obtener todos los usuarios para exportación
        """
        query = f"""
        SELECT 
            id, email, first_name, last_name, role,
            is_active, is_verified, last_login_at,
            created_at, updated_at
        FROM {self.table_name}
        WHERE is_active = true
        ORDER BY created_at DESC
        """

        return await self.execute_query(query)
