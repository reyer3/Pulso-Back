"""
🎯 Simple Watermarks Module - Standalone Version

Watermarks simplificados para ETL incremental.
Este módulo puede ser importado independientemente del ETL principal.

Características:
- Solo rastrea última fecha extraída por tabla
- Sin estados complejos (running, failed, etc.)
- Funciones simples para get/set watermarks
- Compatible con etl/simple_incremental_etl.py

Autor: Ricky para Pulso-Back
"""

from datetime import datetime, timezone
from typing import Optional, Dict
import logging

from shared.database.connection import execute_query


async def ensure_watermark_table() -> None:
    """
    Crear tabla de watermarks simplificada si no existe
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS etl_watermarks_simple (
        table_name VARCHAR(100) PRIMARY KEY,
        last_extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_etl_watermarks_simple_updated 
        ON etl_watermarks_simple(updated_at);
    """
    
    await execute_query(create_table_sql)
    logging.info("✅ Simple watermark table ready")


async def get_last_extracted_date(table_name: str) -> Optional[datetime]:
    """
    Obtener última fecha extraída para una tabla
    
    Args:
        table_name: Nombre de la tabla
        
    Returns:
        datetime: Última fecha extraída, None si es primera extracción
    """
    query = """
    SELECT last_extracted_at 
    FROM etl_watermarks_simple 
    WHERE table_name = $1
    """
    
    row = await execute_query(query, table_name, fetch="one")
    
    if row:
        last_date = row['last_extracted_at']
        logging.info(f"📅 {table_name}: última extracción {last_date}")
        return last_date
    else:
        logging.info(f"🆕 {table_name}: primera extracción (sin watermark)")
        return None


async def update_watermark(table_name: str, extracted_until: datetime) -> None:
    """
    Actualizar watermark después de extracción exitosa
    
    Args:
        table_name: Nombre de la tabla
        extracted_until: Fecha hasta la cual se extrajeron datos
    """
    upsert_sql = """
    INSERT INTO etl_watermarks_simple (table_name, last_extracted_at, updated_at) 
    VALUES ($1, $2, CURRENT_TIMESTAMP)
    ON CONFLICT (table_name) 
    DO UPDATE SET 
        last_extracted_at = $2,
        updated_at = CURRENT_TIMESTAMP
    """
    
    await execute_query(upsert_sql, table_name, extracted_until)
    logging.info(f"✅ Watermark updated: {table_name} -> {extracted_until}")


async def get_all_watermarks() -> Dict[str, datetime]:
    """
    Obtener todos los watermarks para monitoreo
    
    Returns:
        Dict con table_name -> última fecha extraída
    """
    query = """
    SELECT table_name, last_extracted_at 
    FROM etl_watermarks_simple 
    ORDER BY table_name
    """
    
    rows = await execute_query(query, fetch="all")
    return {row['table_name']: row['last_extracted_at'] for row in rows}


async def reset_watermark(table_name: str, reset_to_date: datetime) -> None:
    """
    Reset manual de watermark (para re-extraer datos)
    
    Args:
        table_name: Nombre de la tabla
        reset_to_date: Fecha a la cual resetear el watermark
    """
    await update_watermark(table_name, reset_to_date)
    logging.warning(f"🔄 Watermark reset: {table_name} -> {reset_to_date}")


async def delete_watermark(table_name: str) -> None:
    """
    Eliminar watermark (forzar extracción completa en próxima ejecución)
    
    Args:
        table_name: Nombre de la tabla
    """
    delete_sql = "DELETE FROM etl_watermarks_simple WHERE table_name = $1"
    await execute_query(delete_sql, table_name)
    logging.warning(f"🗑️ Watermark deleted: {table_name}")


async def watermark_health_check() -> Dict[str, any]:
    """
    Health check básico del sistema de watermarks
    
    Returns:
        Dict con estado del sistema
    """
    try:
        # Verificar que la tabla existe y es accesible
        watermarks = await get_all_watermarks()
        
        return {
            "status": "healthy",
            "watermark_system": "operational", 
            "total_tables_tracked": len(watermarks),
            "tables": list(watermarks.keys())
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "watermark_system": "failed"
        }


# ===============================================
# CLASE WRAPPER PARA COMPATIBILIDAD (OPCIONAL)
# ===============================================

class SimpleWatermarkManager:
    """
    Wrapper class para compatibilidad con código existente.
    
    Uso recomendado: usar las funciones directas arriba.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def ensure_watermark_table(self) -> None:
        """Wrapper para ensure_watermark_table()"""
        await ensure_watermark_table()
    
    async def get_last_extracted_date(self, table_name: str) -> Optional[datetime]:
        """Wrapper para get_last_extracted_date()"""
        return await get_last_extracted_date(table_name)
    
    async def update_watermark(self, table_name: str, extracted_until: datetime) -> None:
        """Wrapper para update_watermark()"""
        await update_watermark(table_name, extracted_until)
    
    async def get_all_watermarks(self) -> Dict[str, datetime]:
        """Wrapper para get_all_watermarks()"""
        return await get_all_watermarks()
    
    async def reset_watermark(self, table_name: str, reset_to_date: datetime) -> None:
        """Wrapper para reset_watermark()"""
        await reset_watermark(table_name, reset_to_date)
    
    async def delete_watermark(self, table_name: str) -> None:
        """Wrapper para delete_watermark()"""
        await delete_watermark(table_name)


# Singleton para compatibilidad
_watermark_manager: Optional[SimpleWatermarkManager] = None


async def get_watermark_manager() -> SimpleWatermarkManager:
    """
    Get singleton watermark manager para compatibilidad
    
    NOTA: Para código nuevo, usar las funciones directas es más simple.
    """
    global _watermark_manager
    
    if _watermark_manager is None:
        _watermark_manager = SimpleWatermarkManager()
        await ensure_watermark_table()
    
    return _watermark_manager


# ===============================================
# FUNCIONES DE CONVENIENCIA PARA IMPORTACIÓN
# ===============================================

# Estas funciones permiten importar desde otros módulos sin async
# Ejemplo: from etl.watermarks import get_last_extracted

async def get_last_extracted(table_name: str) -> Optional[datetime]:
    """Función de conveniencia para get_last_extracted_date"""
    return await get_last_extracted_date(table_name)


async def update_extraction_watermark(table_name: str, extracted_until: datetime) -> None:
    """Función de conveniencia para update_watermark"""
    await update_watermark(table_name, extracted_until)


# ===============================================
# EJEMPLO DE USO
# ===============================================

async def example_usage():
    """
    Ejemplo de cómo usar los watermarks simplificados
    """
    # Asegurar que la tabla existe
    await ensure_watermark_table()
    
    # Obtener último watermark
    last_date = await get_last_extracted_date("asignaciones")
    
    if last_date:
        print(f"Última extracción: {last_date}")
    else:
        print("Primera extracción")
    
    # Simular extracción exitosa
    new_date = datetime.now(timezone.utc)
    await update_watermark("asignaciones", new_date)
    
    # Ver todos los watermarks
    all_watermarks = await get_all_watermarks()
    print(f"Watermarks: {all_watermarks}")


if __name__ == "__main__":
    import asyncio
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Ejecutar ejemplo
    asyncio.run(example_usage())
