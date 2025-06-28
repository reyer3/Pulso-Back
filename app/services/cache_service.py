# --- START OF FILE app/services/cache_service.py ---
"""
📦 Servicio de Caching
Encapsula la lógica para interactuar con el caché de Redis.
Construye claves, serializa/deserializa datos y gestiona el TTL.
"""
import json
from typing import Any, Optional

import redis.asyncio as redis

from app.core.logging import LoggerMixin
from app.core.middleware import track_cache_hit, track_cache_miss


class CacheService(LoggerMixin):
    """
    Servicio para gestionar las operaciones de caché de la aplicación.
    """

    def __init__(self, redis_client: redis.Redis):
        """
        Inicializa el servicio con un cliente de Redis asíncrono.

        Args:
            redis_client: Una instancia del cliente de redis.asyncio.
        """
        self.redis = redis_client

    @staticmethod
    def _generate_cache_key(prefix: str, **kwargs: Any) -> str:
        """
        Genera una clave de caché consistente a partir de un prefijo y parámetros.
        """
        key_parts = [prefix]
        # Ordenamos los kwargs para que la clave sea siempre la misma independientemente del orden de los argumentos
        for key, value in sorted(kwargs.items()):
            if value is None:
                continue
            # Serializa listas y diccionarios a JSON para crear una representación de string consistente
            if isinstance(value, (list, dict)):
                value_str = json.dumps(value, sort_keys=True, default=str)
            else:
                value_str = str(value)
            key_parts.append(f"{key}:{value_str}")

        return ":".join(key_parts)

    async def get(self, key: str) -> Optional[Any]:
        """
        Obtiene un valor del caché. Deserializa desde JSON.
        """
        try:
            value = await self.redis.get(key)
            if value:
                track_cache_hit("redis")
                self.logger.debug(f"Cache HIT para la clave: {key}")
                return json.loads(value)

            track_cache_miss("redis")
            self.logger.debug(f"Cache MISS para la clave: {key}")
            return None
        except Exception as e:
            self.logger.error(f"Error al obtener del caché para la clave {key}: {e}")
            track_cache_miss("redis")  # Contar como miss si hay un error
            return None

    async def set(self, key: str, value: Any, expire_in: int) -> bool:
        """
        Almacena un valor en el caché con un TTL (Time-To-Live) en segundos.
        Serializa a JSON.
        """
        if expire_in <= 0:
            self.logger.warning(
                f"Intento de establecer una clave de caché '{key}' con TTL no positivo. No se guardará.")
            return False

        try:
            # Serializamos el valor a un string JSON. `default=str` maneja tipos no serializables como datetime.
            json_value = json.dumps(value, default=str)
            await self.redis.setex(key, expire_in, json_value)
            self.logger.debug(f"Cache SET para la clave: {key}, TTL: {expire_in}s")
            return True
        except Exception as e:
            self.logger.error(f"Error al guardar en caché para la clave {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Elimina una clave específica del caché.
        """
        try:
            result = await self.redis.delete(key)
            if result > 0:
                self.logger.info(f"Clave de caché eliminada: {key}")
            return result > 0
        except Exception as e:
            self.logger.error(f"Error al eliminar la clave de caché {key}: {e}")
            return False

    async def clear_by_pattern(self, pattern: str) -> int:
        """
        Elimina todas las claves que coincidan con un patrón (ej: 'assignment:*').
        ¡Usar con precaución en producción!
        """
        try:
            keys = await self.redis.keys(pattern)
            if not keys:
                return 0

            deleted_count = await self.redis.delete(*keys)
            self.logger.info(f"Se eliminaron {deleted_count} claves de caché con el patrón: {pattern}")
            return deleted_count
        except Exception as e:
            self.logger.error(f"Error al limpiar el caché con el patrón {pattern}: {e}")
            return 0

