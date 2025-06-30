#!/usr/bin/env python3
# etl/debug_campaigns.py

"""
🔍 SCRIPT DE DIAGNÓSTICO: Verificar estructura de campañas

Este script ayuda a diagnosticar problemas con la tabla calendario
y muestra la estructura exacta de los datos.
"""

import asyncio
import sys
from etl.dependencies import etl_dependencies
from shared.core.logging import get_logger

logger = get_logger(__name__)


def safe_extract_row_data(row, description="row"):
    """Helper para extraer datos de manera segura"""
    logger.debug(f"Processing {description}:")
    logger.debug(f"  Type: {type(row)}")
    logger.debug(f"  Content: {row}")
    
    if isinstance(row, (list, tuple)):
        logger.debug(f"  Length: {len(row)}")
        return row
    elif isinstance(row, str):
        logger.debug(f"  String length: {len(row)}")
        # Si es un string, podría ser un valor único
        return [row]
    elif isinstance(row, dict):
        logger.debug(f"  Dict keys: {list(row.keys())}")
        return row
    else:
        logger.debug(f"  Unknown type, converting to list")
        return [row]


async def debug_campaigns():
    """Diagnóstico completo de la tabla calendario"""
    
    try:
        # Inicializar recursos
        logger.info("🔧 Initializing resources...")
        await etl_dependencies.init_resources()
        
        db = etl_dependencies._db_manager
        
        # 1. Verificar si existe la tabla
        logger.info("🔍 Step 1: Checking if calendario table exists...")
        
        table_check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'raw_P3fV4dWNeMkN5RJMhV8e' 
            AND table_name = 'calendario'
        );
        """
        
        exists_result = await db.execute_query(table_check_query)
        logger.debug(f"Exists result: {exists_result}")
        logger.debug(f"Exists result type: {type(exists_result)}")
        
        if exists_result:
            first_row = safe_extract_row_data(exists_result[0], "exists check")
            table_exists = first_row[0] if len(first_row) > 0 else False
        else:
            table_exists = False
        
        logger.info(f"Table exists: {table_exists}")
        
        if not table_exists:
            logger.error("❌ Table raw_P3fV4dWNeMkN5RJMhV8e.calendario does not exist!")
            return False
        
        # 2. Skip structure check for now and go directly to data
        logger.info("🔍 Step 2: Skipping structure check, going to data...")
        
        # 3. Contar registros totales
        logger.info("🔍 Step 3: Counting total records...")
        
        count_query = "SELECT COUNT(*) FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario;"
        count_result = await db.execute_query(count_query)
        logger.debug(f"Count result: {count_result}")
        
        if count_result:
            count_row = safe_extract_row_data(count_result[0], "count")
            total_count = count_row[0] if len(count_row) > 0 else 0
        else:
            total_count = 0
        
        logger.info(f"Total records: {total_count:,}")
        
        if total_count == 0:
            logger.warning("⚠️ Table is empty!")
            return False
        
        # 4. Mostrar algunos registros de ejemplo SIN asumir estructura
        logger.info("🔍 Step 4: Showing sample records...")
        
        # Empezar con consulta simple
        simple_query = "SELECT archivo FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario LIMIT 3;"
        
        simple_result = await db.execute_query(simple_query)
        logger.info("Sample archivo values:")
        logger.info("-" * 80)
        
        for i, row in enumerate(simple_result, 1):
            row_data = safe_extract_row_data(row, f"sample row {i}")
            archivo = row_data[0] if len(row_data) > 0 else "N/A"
            logger.info(f"  {i}. {archivo}")
        
        # 5. Ahora intentar con más columnas
        logger.info("🔍 Step 5: Trying multi-column query...")
        
        multi_query = """
        SELECT archivo, fecha_apertura 
        FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario 
        ORDER BY fecha_apertura DESC 
        LIMIT 5;
        """
        
        multi_result = await db.execute_query(multi_query)
        logger.info("Multi-column results:")
        logger.info("-" * 80)
        
        for i, row in enumerate(multi_result, 1):
            row_data = safe_extract_row_data(row, f"multi row {i}")
            
            if len(row_data) >= 2:
                archivo = row_data[0]
                fecha_apertura = row_data[1]
                logger.info(f"  {i}. {archivo} | {fecha_apertura}")
            else:
                logger.info(f"  {i}. Unexpected format: {row_data}")
        
        # 6. Buscar la campaña específica
        logger.info("🔍 Step 6: Searching for specific campaign...")
        
        search_campaign = "Cartera_Agencia_Cobranding_Gestion_Temprana_20250401_25"
        
        # Búsqueda exacta
        exact_query = "SELECT archivo FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario WHERE archivo = $1;"
        
        exact_result = await db.execute_query(exact_query, search_campaign)
        
        if exact_result:
            logger.info(f"✅ Found exact match for '{search_campaign}'")
        else:
            logger.warning(f"❌ No exact match for '{search_campaign}'")
            
            # Búsqueda parcial
            search_query = """
            SELECT archivo 
            FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario 
            WHERE archivo ILIKE $1
            LIMIT 10;
            """
            
            search_result = await db.execute_query(search_query, f"%Cartera_Agencia%")
            
            if search_result:
                logger.info("🔍 Found similar campaigns:")
                for i, row in enumerate(search_result, 1):
                    row_data = safe_extract_row_data(row, f"search row {i}")
                    archivo = row_data[0] if len(row_data) > 0 else "N/A"
                    logger.info(f"  {i}. {archivo}")
            else:
                logger.warning("❌ No similar campaigns found")
        
        # 7. Mostrar las 10 campañas más recientes para que puedas elegir
        logger.info("🔍 Step 7: Most recent campaigns for selection...")
        
        recent_query = """
        SELECT archivo 
        FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario 
        ORDER BY fecha_apertura DESC 
        LIMIT 15;
        """
        
        recent_result = await db.execute_query(recent_query)
        logger.info("📋 15 Most recent campaigns (copy exact name):")
        logger.info("=" * 100)
        
        for i, row in enumerate(recent_result, 1):
            row_data = safe_extract_row_data(row, f"recent row {i}")
            archivo = row_data[0] if len(row_data) > 0 else "N/A"
            logger.info(f"{i:2d}. {archivo}")
        
        logger.info("=" * 100)
        logger.info("💡 Copy one of the exact names above and use it with the CLI")
        
        logger.info("✅ Diagnosis completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Diagnosis failed: {e}", exc_info=True)
        return False
        
    finally:
        try:
            await etl_dependencies.shutdown_resources()
            logger.info("🔌 Resources shut down")
        except Exception as e:
            logger.error(f"❌ Shutdown error: {e}")


async def main():
    """Función principal"""
    logger.info("🚀 Starting campaign diagnosis...")
    
    success = await debug_campaigns()
    
    if success:
        logger.info("🎉 Diagnosis completed - check logs above for campaign names")
        logger.info("💡 Use: uv run etl/run_aux_mart_cli.py aux-mart --campaign \"[exact_name_from_list]\"")
        return 0
    else:
        logger.error("❌ Diagnosis failed - check errors above")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)