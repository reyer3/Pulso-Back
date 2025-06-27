# --- START OF FILE app/core/dependencies.py ---
"""
💧 Sistema Central de Inyección de Dependencias
Provee recursos como sesiones de base de datos, servicios y adaptadores a los endpoints de la API.
Este archivo es la única fuente de la verdad para crear y proveer dependencias complejas.
"""
from typing import Generator, Annotated
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis

# --- Importamos las piezas básicas de nuestros módulos core ---
from app.core.database import get_db as get_db_session  # Renombramos para claridad
from app.core.cache import cache as redis_cache_manager
from app.repositories.data_adapters import DataSourceFactory, DataSourceAdapter
from app.services.dashboard_service_v2 import DashboardServiceV2
from app.services.cache_service import CacheService

# --- DEPENDENCIAS DE NIVEL 1: Conexiones y Clientes Crudos ---

async def get_redis_client() -> redis.Redis:
    """
    Provee una instancia del cliente de Redis.
    Asegura que el pool de conexiones esté inicializado.
    """
    if not redis_cache_manager.redis:
        await redis_cache_manager.init_redis()
    return redis_cache_manager.redis

def get_data_adapter() -> DataSourceAdapter:
    """
    Obtiene el adaptador de fuente de datos configurado desde la fábrica.
    """
    return DataSourceFactory.create_adapter()


# --- DEPENDENCIAS DE NIVEL 2: Servicios Ensamblados ---

# Usamos Annotated para una sintaxis más limpia y un mejor chequeo de tipos.
# Es la forma moderna y recomendada de usar Depends.
DBSession = Annotated[AsyncSession, Depends(get_db_session)]
RedisClient = Annotated[redis.Redis, Depends(get_redis_client)]
DataAdapter = Annotated[DataSourceAdapter, Depends(get_data_adapter)]

def get_cache_service(
    client: RedisClient
) -> CacheService:
    """
    Crea y retorna una instancia de CacheService.
    Depende de un cliente de Redis.
    """
    return CacheService(redis_client=client)

def get_dashboard_service(
    adapter: DataAdapter
) -> DashboardServiceV2:
    """
    Crea y retorna una instancia de DashboardServiceV2.
    Depende de un adaptador de datos.
    """
    return DashboardServiceV2(adapter)

# Alias para mantener la compatibilidad con el código existente si es necesario.
# Puedes usar get_dashboard_service o get_dashboard_service_v2, ambos funcionarán.
get_dashboard_service_v2 = get_dashboard_service