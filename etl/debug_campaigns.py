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
        table_exists = exists_result[0][0] if exists_result else False
        
        logger.info(f"Table exists: {table_exists}")
        
        if not table_exists:
            logger.error("❌ Table raw_P3fV4dWNeMkN5RJMhV8e.calendario does not exist!")
            
            # Verificar qué tablas existen en el schema
            schema_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'raw_P3fV4dWNeMkN5RJMhV8e'
            ORDER BY table_name;
            """
            
            schema_result = await db.execute_query(schema_query)
            logger.info("Available tables in raw_P3fV4dWNeMkN5RJMhV8e schema:")
            for row in schema_result:
                table_name = row[0] if isinstance(row, tuple) else row['table_name']
                logger.info(f"  • {table_name}")
            
            return False
        
        # 2. Verificar estructura de la tabla
        logger.info("🔍 Step 2: Checking table structure...")
        
        structure_query = """
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns 
        WHERE table_schema = 'raw_P3fV4dWNeMkN5RJMhV8e' 
        AND table_name = 'calendario'
        ORDER BY ordinal_position;
        """
        
        structure_result = await db.execute_query(structure_query)
        logger.info("Table structure:")
        for row in structure_result:
            if isinstance(row, tuple):
                col_name, data_type, nullable = row
            else:
                col_name = row['column_name']
                data_type = row['data_type']
                nullable = row['is_nullable']
            logger.info(f"  • {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        # 3. Contar registros totales
        logger.info("🔍 Step 3: Counting total records...")
        
        count_query = "SELECT COUNT(*) FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario;"
        count_result = await db.execute_query(count_query)
        total_count = count_result[0][0] if count_result else 0
        
        logger.info(f"Total records: {total_count:,}")
        
        if total_count == 0:
            logger.warning("⚠️ Table is empty!")
            return False
        
        # 4. Mostrar algunos registros de ejemplo
        logger.info("🔍 Step 4: Showing sample records...")
        
        sample_query = """
        SELECT archivo, fecha_apertura, fecha_cierre
        FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario 
        ORDER BY fecha_apertura DESC 
        LIMIT 5;
        """
        
        sample_result = await db.execute_query(sample_query)
        logger.info("Sample records:")
        logger.info("-" * 100)
        
        for i, row in enumerate(sample_result, 1):
            logger.info(f"Raw row {i} type: {type(row)}")
            logger.info(f"Raw row {i} content: {row}")
            
            try:
                if isinstance(row, tuple):
                    archivo = row[0] if len(row) > 0 else "N/A"
                    fecha_apertura = row[1] if len(row) > 1 else "N/A"
                    fecha_cierre = row[2] if len(row) > 2 else "N/A"
                else:
                    archivo = row.get('archivo', 'N/A')
                    fecha_apertura = row.get('fecha_apertura', 'N/A')
                    fecha_cierre = row.get('fecha_cierre', 'N/A')
                
                logger.info(f"  {i}. Archivo: {archivo}")
                logger.info(f"     Apertura: {fecha_apertura}")
                logger.info(f"     Cierre: {fecha_cierre}")
                logger.info("-" * 50)
                
            except Exception as e:
                logger.error(f"❌ Error processing row {i}: {e}")
                logger.error(f"Row data: {row}")
        
        # 5. Buscar la campaña específica que estabas probando
        logger.info("🔍 Step 5: Searching for specific campaign...")
        
        search_campaign = "Cartera_Agencia_Cobranding_Gestion_Temprana_20250401_25"
        
        search_query = """
        SELECT archivo, fecha_apertura, fecha_cierre
        FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario 
        WHERE archivo ILIKE $1
        LIMIT 5;
        """
        
        search_result = await db.execute_query(search_query, f"%{search_campaign[:30]}%")
        
        if search_result:
            logger.info(f"Found campaigns matching '{search_campaign[:30]}':")
            for i, row in enumerate(search_result, 1):
                if isinstance(row, tuple):
                    archivo = row[0]
                else:
                    archivo = row.get('archivo', 'N/A')
                logger.info(f"  {i}. {archivo}")
        else:
            logger.warning(f"No campaigns found matching '{search_campaign[:30]}'")
            
            # Buscar campañas con nombres similares
            partial_search = search_query
            partial_result = await db.execute_query(partial_search, "%Cartera_Agencia%")
            
            if partial_result:
                logger.info("Similar campaigns found:")
                for i, row in enumerate(partial_result, 1):
                    if isinstance(row, tuple):
                        archivo = row[0]
                    else:
                        archivo = row.get('archivo', 'N/A')
                    logger.info(f"  {i}. {archivo}")
        
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
        logger.info("🎉 Diagnosis completed - check logs above for details")
        return 0
    else:
        logger.error("❌ Diagnosis failed - check errors above")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)