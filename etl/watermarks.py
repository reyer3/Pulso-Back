"""
🎯 Simple Watermarks Module - OPTIMIZED VERSION

Watermarks simplificados para ETL incremental con optimizaciones menores.
Mantiene simplicidad pero agrega funcionalidades útiles sin overhead.

Optimizaciones añadidas:
- Batch fetch de watermarks (útil para monitoreo)
- Validación de fechas (evita retrocesos accidentales) 
- Mejor logging y debugging
- Función de status para health checks

Autor: Ricky para Pulso-Back
"""

from datetime import datetime, timezone
from typing import Optional, Dict, List
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
        logging.debug(f"📅 {table_name}: watermark found {last_date}")
        return last_date
    else:
        logging.info(f"🆕 {table_name}: no watermark (first extraction)")
        return None


async def get_all_watermarks() -> Dict[str, datetime]:
    """
    🆕 OPTIMIZED: Obtener todos los watermarks en una sola query
    Útil para monitoreo y para evitar múltiples round-trips a la DB
    
    Returns:
        Dict con table_name -> última fecha extraída
    """
    query = """
    SELECT table_name, last_extracted_at, updated_at
    FROM etl_watermarks_simple 
    ORDER BY table_name
    """
    
    rows = await execute_query(query, fetch="all")
    
    result = {row['table_name']: row['last_extracted_at'] for row in rows}
    
    logging.debug(f"📊 Fetched {len(result)} watermarks in batch")
    return result


async def get_multiple_watermarks(table_names: List[str]) -> Dict[str, Optional[datetime]]:
    """
    🆕 OPTIMIZED: Obtener watermarks de múltiples tablas en una query
    Útil cuando se procesan varias tablas y se quieren todos los watermarks de una vez
    
    Args:
        table_names: Lista de nombres de tablas
        
    Returns:
        Dict con table_name -> fecha (None si no existe watermark)
    """
    if not table_names:
        return {}
    
    # Crear placeholders para la query IN
    placeholders = ','.join([f'${i+1}' for i in range(len(table_names))])
    
    query = f"""
    SELECT table_name, last_extracted_at 
    FROM etl_watermarks_simple 
    WHERE table_name IN ({placeholders})
    """
    
    rows = await execute_query(query, *table_names, fetch="all")
    
    # Crear resultado con None para tablas sin watermark
    result = {name: None for name in table_names}
    for row in rows:
        result[row['table_name']] = row['last_extracted_at']
    
    logging.debug(f"📊 Batch fetched watermarks for {len(table_names)} tables")
    return result


async def update_watermark(
    table_name: str, 
    extracted_until: datetime, 
    validate_progression: bool = True
) -> None:
    """
    🆕 OPTIMIZED: Actualizar watermark con validación opcional
    
    Args:
        table_name: Nombre de la tabla
        extracted_until: Fecha hasta la cual se extrajeron datos
        validate_progression: Si True, valida que la fecha no retroceda
    """
    # Validación opcional para evitar retrocesos accidentales
    if validate_progression:
        current_watermark = await get_last_extracted_date(table_name)
        if current_watermark and extracted_until < current_watermark:
            logging.warning(
                f"⚠️ {table_name}: Watermark would go backwards! "
                f"Current: {current_watermark}, New: {extracted_until}"
            )
            # No actualizar si va hacia atrás, a menos que sea intencional
            return
    
    upsert_sql = """
    INSERT INTO etl_watermarks_simple (table_name, last_extracted_at, updated_at) 
    VALUES ($1, $2, CURRENT_TIMESTAMP)
    ON CONFLICT (table_name) 
    DO UPDATE SET 
        last_extracted_at = $2,
        updated_at = CURRENT_TIMESTAMP
    """
    
    await execute_query(upsert_sql, table_name, extracted_until)
    logging.info(f"✅ Watermark updated: {table_name} → {extracted_until}")


async def reset_watermark(table_name: str, reset_to_date: datetime) -> None:
    """
    Reset manual de watermark (sin validación, fuerza el cambio)
    
    Args:
        table_name: Nombre de la tabla
        reset_to_date: Fecha a la cual resetear el watermark
    """
    await update_watermark(table_name, reset_to_date, validate_progression=False)
    logging.warning(f"🔄 Watermark reset: {table_name} → {reset_to_date}")


async def delete_watermark(table_name: str) -> None:
    """
    Eliminar watermark (forzar extracción completa en próxima ejecución)
    
    Args:
        table_name: Nombre de la tabla
    """
    delete_sql = "DELETE FROM etl_watermarks_simple WHERE table_name = $1"
    await execute_query(delete_sql, table_name)
    logging.warning(f"🗑️ Watermark deleted: {table_name}")


async def get_watermark_status() -> Dict[str, any]:
    """
    🆕 OPTIMIZED: Status completo del sistema de watermarks para monitoreo
    
    Returns:
        Dict con estadísticas y estado del sistema
    """
    try:
        query = """
        SELECT 
            COUNT(*) as total_tables,
            MIN(last_extracted_at) as oldest_extraction,
            MAX(last_extracted_at) as newest_extraction,
            MAX(updated_at) as last_update,
            COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '24 hours') as updated_last_24h
        FROM etl_watermarks_simple
        """
        
        row = await execute_query(query, fetch="one")
        
        if row and row['total_tables'] > 0:
            return {
                "status": "healthy",
                "total_tables": row['total_tables'],
                "oldest_extraction": row['oldest_extraction'].isoformat() if row['oldest_extraction'] else None,
                "newest_extraction": row['newest_extraction'].isoformat() if row['newest_extraction'] else None, 
                "last_update": row['last_update'].isoformat() if row['last_update'] else None,
                "updated_last_24h": row['updated_last_24h'],
                "system": "operational"
            }
        else:
            return {
                "status": "empty",
                "total_tables": 0,
                "message": "No watermarks found",
                "system": "operational"
            }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "system": "failed"
        }


async def cleanup_orphaned_watermarks(valid_table_names: List[str]) -> int:
    """
    🆕 MAINTENANCE: Limpiar watermarks de tablas que ya no existen en la configuración
    
    Args:
        valid_table_names: Lista de nombres de tablas válidas según configuración
        
    Returns:
        Número de watermarks eliminados
    """
    if not valid_table_names:
        logging.warning("No valid table names provided for cleanup")
        return 0
    
    placeholders = ','.join([f'${i+1}' for i in range(len(valid_table_names))])
    
    cleanup_sql = f"""
    DELETE FROM etl_watermarks_simple 
    WHERE table_name NOT IN ({placeholders})
    RETURNING table_name
    """
    
    deleted_rows = await execute_query(cleanup_sql, *valid_table_names, fetch="all")
    deleted_count = len(deleted_rows)
    
    if deleted_count > 0:
        deleted_tables = [row['table_name'] for row in deleted_rows]
        logging.info(f"🧹 Cleaned up {deleted_count} orphaned watermarks: {deleted_tables}")
    
    return deleted_count


# ===============================================
# FUNCIONES DE CONVENIENCIA PARA COMPATIBILIDAD
# ===============================================

async def get_last_extracted(table_name: str) -> Optional[datetime]:
    """Función de conveniencia para compatibilidad"""
    return await get_last_extracted_date(table_name)


async def update_extraction_watermark(table_name: str, extracted_until: datetime) -> None:
    """Función de conveniencia para compatibilidad"""
    await update_watermark(table_name, extracted_until)


async def watermark_health_check() -> Dict[str, any]:
    """Función de conveniencia para compatibilidad"""
    return await get_watermark_status()


# ===============================================
# CLASE WRAPPER PARA COMPATIBILIDAD (OPCIONAL)
# ===============================================

class SimpleWatermarkManager:
    """
    Wrapper class para compatibilidad con código existente.
    Incluye las nuevas optimizaciones.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def ensure_watermark_table(self) -> None:
        """Wrapper para ensure_watermark_table()"""
        await ensure_watermark_table()
    
    async def get_last_extracted_date(self, table_name: str) -> Optional[datetime]:
        """Wrapper para get_last_extracted_date()"""
        return await get_last_extracted_date(table_name)
    
    async def get_all_watermarks(self) -> Dict[str, datetime]:
        """Wrapper para get_all_watermarks()"""
        return await get_all_watermarks()
    
    async def get_multiple_watermarks(self, table_names: List[str]) -> Dict[str, Optional[datetime]]:
        """🆕 Batch fetch de watermarks"""
        return await get_multiple_watermarks(table_names)
    
    async def update_watermark(self, table_name: str, extracted_until: datetime) -> None:
        """Wrapper para update_watermark()"""
        await update_watermark(table_name, extracted_until)
    
    async def reset_watermark(self, table_name: str, reset_to_date: datetime) -> None:
        """Wrapper para reset_watermark()"""
        await reset_watermark(table_name, reset_to_date)
    
    async def delete_watermark(self, table_name: str) -> None:
        """Wrapper para delete_watermark()"""
        await delete_watermark(table_name)
    
    async def get_status(self) -> Dict[str, any]:
        """🆕 Get watermark system status"""
        return await get_watermark_status()


# Singleton para compatibilidad
_watermark_manager: Optional[SimpleWatermarkManager] = None


async def get_watermark_manager() -> SimpleWatermarkManager:
    """
    Get singleton watermark manager para compatibilidad
    """
    global _watermark_manager
    
    if _watermark_manager is None:
        _watermark_manager = SimpleWatermarkManager()
        await ensure_watermark_table()
    
    return _watermark_manager


# ===============================================
# EJEMPLO DE USO CON NUEVAS OPTIMIZACIONES
# ===============================================

async def example_optimized_usage():
    """
    Ejemplo de cómo usar las nuevas optimizaciones
    """
    from etl.config import ETLConfig
    
    # Asegurar que la tabla existe
    await ensure_watermark_table()
    
    # 🆕 OPTIMIZED: Obtener watermarks de todas las tablas configuradas en batch
    table_names = ETLConfig.get_raw_source_tables()
    watermarks = await get_multiple_watermarks(table_names)
    
    print("📊 Current watermarks:")
    for table, watermark in watermarks.items():
        if watermark:
            print(f"  {table}: {watermark}")
        else:
            print(f"  {table}: NO WATERMARK (first extraction)")
    
    # 🆕 OPTIMIZED: Status del sistema
    status = await get_watermark_status()
    print(f"\n📈 System status: {status}")
    
    # 🆕 MAINTENANCE: Limpiar watermarks huérfanos
    cleaned = await cleanup_orphaned_watermarks(table_names)
    print(f"\n🧹 Cleaned {cleaned} orphaned watermarks")


if __name__ == "__main__":
    import asyncio
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Ejecutar ejemplo optimizado
    asyncio.run(example_optimized_usage())
