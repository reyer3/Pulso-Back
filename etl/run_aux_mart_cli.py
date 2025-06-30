#!/usr/bin/env python3
# etl/run_aux_mart_cli.py

"""
🎯 CLI PARA AUX Y MART: Comandos para ejecutar capas superiores

EJECUCIÓN:
python etl/run_aux_mart_cli.py [comando] --campaign [archivo_campaña]

COMANDOS:
- aux-only: Solo capa AUX
- mart-only: Solo capa MART  
- aux-mart: AUX + MART
- validate: Validar capas
"""

import asyncio
import argparse
import sys
from datetime import datetime, date
from typing import Optional

from etl.dependencies import etl_dependencies
from etl.models import CampaignWindow
from shared.core.logging import get_logger

logger = get_logger(__name__)


async def get_campaign_from_postgres(archivo: str) -> CampaignWindow:
    """
    🔍 Obtiene información de campaña desde PostgreSQL (no BigQuery).
    
    Args:
        archivo: Nombre del archivo de campaña
        
    Returns:
        CampaignWindow con información de la campaña
    """
    try:
        # Usar la base de datos ya conectada
        db = etl_dependencies._db_manager
        
        # Query desde la tabla calendario en PostgreSQL
        query = """
        SELECT 
            archivo,
            fecha_apertura,
            fecha_cierre,
            tipo_cartera,
            estado_cartera
        FROM raw_P3fV4dWNeMkN5RJMhV8e.calendario
        WHERE archivo = $1
        LIMIT 1
        """
        
        result = await db.execute_query(query, archivo)
        
        if not result:
            raise ValueError(f"Campaign '{archivo}' not found in PostgreSQL calendario table")
        
        row = result[0]
        
        # Convertir fechas si es necesario
        fecha_apertura = row['fecha_apertura']
        if isinstance(fecha_apertura, str):
            fecha_apertura = datetime.strptime(fecha_apertura, '%Y-%m-%d').date()
        elif isinstance(fecha_apertura, datetime):
            fecha_apertura = fecha_apertura.date()
            
        fecha_cierre = row.get('fecha_cierre')
        if fecha_cierre and isinstance(fecha_cierre, str):
            fecha_cierre = datetime.strptime(fecha_cierre, '%Y-%m-%d').date()
        elif fecha_cierre and isinstance(fecha_cierre, datetime):
            fecha_cierre = fecha_cierre.date()
        
        campaign = CampaignWindow(
            archivo=row['archivo'],
            fecha_apertura=fecha_apertura,
            fecha_cierre=fecha_cierre,
            tipo_cartera=row.get('tipo_cartera', 'UNKNOWN'),
            estado_cartera=row.get('estado_cartera', 'UNKNOWN')
        )
        
        logger.info(f"📅 Campaign found: {campaign.archivo} ({campaign.fecha_apertura} to {campaign.fecha_cierre})")
        return campaign
        
    except Exception as e:
        raise Exception(f"Failed to get campaign info for '{archivo}': {str(e)}")


async def cmd_aux_only(args) -> bool:
    """🏗️ Comando: Ejecutar solo capa AUX"""
    try:
        logger.info("🏗️ Starting AUX-only execution...")
        
        # Obtener campaña desde PostgreSQL
        campaign = await get_campaign_from_postgres(args.campaign)
        
        # Crear AUX pipeline directamente (sin dependency que falta)
        from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
        aux_pipeline = AuxBuildPipeline(etl_dependencies._db_manager, "P3fV4dWNeMkN5RJMhV8e")
        
        # Ejecutar AUX
        result = await aux_pipeline.run_for_campaign(campaign)
        
        # Mostrar resultados
        logger.info("=" * 60)
        logger.info("📊 AUX EXECUTION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Status: {result['status'].upper()}")
        logger.info(f"Steps: {result['successful_steps']}/{result['total_steps']}")
        logger.info(f"Rows processed: {result['total_rows_processed']:,}")
        logger.info(f"Duration: {result['duration_seconds']:.2f}s")
        
        if result['status'] == 'success':
            logger.info("✅ AUX execution completed successfully")
            
            # Validar salida
            validation = await aux_pipeline.validate_aux_output(campaign)
            logger.info(f"🔍 Validation: {validation['overall_status'].upper()}")
            
            return True
        else:
            logger.error(f"❌ AUX execution failed: {result.get('error_message', 'Unknown error')}")
            return False
            
    except Exception as e:
        logger.error(f"❌ AUX execution error: {e}")
        return False


async def cmd_mart_only(args) -> bool:
    """📈 Comando: Ejecutar solo capa MART"""
    try:
        logger.info("📈 Starting MART-only execution...")
        
        # Obtener campaña desde PostgreSQL
        campaign = await get_campaign_from_postgres(args.campaign)
        
        # Usar MART pipeline existente
        mart_pipeline = await etl_dependencies.mart_build_pipeline()
        await mart_pipeline.run_for_campaign(campaign)
        
        logger.info("✅ MART execution completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ MART execution error: {e}")
        return False


async def cmd_aux_mart(args) -> bool:
    """🔄 Comando: Ejecutar AUX + MART secuencialmente"""
    try:
        logger.info("🔄 Starting AUX + MART execution...")
        
        # Obtener campaña desde PostgreSQL
        campaign = await get_campaign_from_postgres(args.campaign)
        
        # PASO 1: Ejecutar AUX
        logger.info("\n" + "="*50)
        logger.info("🏗️ STEP 1: AUX LAYER")
        logger.info("="*50)
        
        from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
        aux_pipeline = AuxBuildPipeline(etl_dependencies._db_manager, "P3fV4dWNeMkN5RJMhV8e")
        
        aux_result = await aux_pipeline.run_for_campaign(campaign)
        
        if aux_result['status'] != 'success':
            raise Exception(f"AUX step failed: {aux_result.get('error_message', 'Unknown error')}")
            
        logger.info(f"✅ AUX completed: {aux_result['successful_steps']}/{aux_result['total_steps']} steps")
        
        # PASO 2: Ejecutar MART
        logger.info("\n" + "="*50)
        logger.info("📈 STEP 2: MART LAYER")
        logger.info("="*50)
        
        mart_pipeline = await etl_dependencies.mart_build_pipeline()
        await mart_pipeline.run_for_campaign(campaign)
        
        logger.info("✅ MART completed successfully")
        
        # Resumen final
        total_duration = aux_result['duration_seconds']  # Aproximado
        
        logger.info("\n" + "="*60)
        logger.info("🎉 AUX + MART EXECUTION COMPLETED")
        logger.info("="*60)
        logger.info(f"📅 Campaign: {campaign.archivo}")
        logger.info(f"🏗️ AUX: {aux_result['successful_steps']}/{aux_result['total_steps']} steps, {aux_result['total_rows_processed']:,} rows")
        logger.info(f"📈 MART: Construction completed")
        logger.info(f"⏱️ Total duration: ~{total_duration:.2f}s")
        logger.info("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ AUX + MART execution error: {e}")
        return False


async def cmd_validate(args) -> bool:
    """🔍 Comando: Validar todas las capas"""
    try:
        logger.info("🔍 Starting validation...")
        
        # Obtener campaña desde PostgreSQL
        campaign = await get_campaign_from_postgres(args.campaign)
        
        validation_results = {
            "campaign": campaign.archivo,
            "raw_validation": {"status": "unknown"},
            "aux_validation": {"status": "unknown"},
            "mart_validation": {"status": "unknown"}
        }
        
        # Validar RAW
        logger.info("🔍 Validating RAW layer...")
        try:
            db = etl_dependencies._db_manager
            critical_tables = ["asignaciones", "voicebot_gestiones", "mibotair_gestiones"]
            
            for table in critical_tables:
                count_query = f"""
                SELECT COUNT(*) as count 
                FROM raw_P3fV4dWNeMkN5RJMhV8e.{table} 
                WHERE archivo = $1
                """
                result = await db.execute_query(count_query, campaign.archivo)
                count = result[0]['count'] if result else 0
                
                if count == 0:
                    raise Exception(f"RAW table {table} has no records for campaign")
                    
                logger.debug(f"✅ RAW {table}: {count:,} records")
                
            validation_results["raw_validation"]["status"] = "pass"
            logger.info("✅ RAW validation passed")
            
        except Exception as e:
            validation_results["raw_validation"] = {"status": "fail", "error": str(e)}
            logger.error(f"❌ RAW validation failed: {e}")
        
        # Validar AUX
        logger.info("🔍 Validating AUX layer...")
        try:
            from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
            aux_pipeline = AuxBuildPipeline(etl_dependencies._db_manager, "P3fV4dWNeMkN5RJMhV8e")
            
            aux_validation = await aux_pipeline.validate_aux_output(campaign)
            validation_results["aux_validation"] = aux_validation
            
            if aux_validation["overall_status"] == "pass":
                logger.info("✅ AUX validation passed")
            else:
                logger.warning("⚠️ AUX validation issues detected")
                
        except Exception as e:
            validation_results["aux_validation"] = {"status": "fail", "error": str(e)}
            logger.error(f"❌ AUX validation failed: {e}")
        
        # Validar MART  
        logger.info("🔍 Validating MART layer...")
        try:
            db = etl_dependencies._db_manager
            mart_tables = ["dashboard_data"]
            
            for table in mart_tables:
                count_query = f"""
                SELECT COUNT(*) as count 
                FROM mart_P3fV4dWNeMkN5RJMhV8e.{table} 
                WHERE archivo = $1
                """
                result = await db.execute_query(count_query, campaign.archivo)
                count = result[0]['count'] if result else 0
                
                logger.debug(f"✅ MART {table}: {count:,} records")
                
            validation_results["mart_validation"]["status"] = "pass"
            logger.info("✅ MART validation completed")
            
        except Exception as e:
            validation_results["mart_validation"] = {"status": "fail", "error": str(e)}
            logger.warning(f"⚠️ MART validation failed: {e}")
        
        # Resumen
        passed_validations = sum(
            1 for v in validation_results.values() 
            if isinstance(v, dict) and v.get("status") == "pass"
        )
        total_validations = len([k for k in validation_results.keys() if k.endswith("_validation")])
        
        overall_status = "pass" if passed_validations == total_validations else "fail"
        
        logger.info("\n" + "="*60)
        logger.info("📊 VALIDATION RESULTS")
        logger.info("="*60)
        logger.info(f"📅 Campaign: {campaign.archivo}")
        logger.info(f"✅ Passed: {passed_validations}/{total_validations} layers")
        logger.info(f"🎯 Overall: {overall_status.upper()}")
        logger.info("="*60)
        
        return overall_status == "pass"
        
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return False


async def main():
    """Función principal del CLI"""
    start_time = datetime.now()
    
    parser = argparse.ArgumentParser(description="AUX & MART Pipeline CLI")
    
    # Subcomandos
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # aux-only
    aux_parser = subparsers.add_parser('aux-only', help='Execute only AUX layer')
    aux_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # mart-only  
    mart_parser = subparsers.add_parser('mart-only', help='Execute only MART layer')
    mart_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # aux-mart
    aux_mart_parser = subparsers.add_parser('aux-mart', help='Execute AUX + MART')
    aux_mart_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # validate
    validate_parser = subparsers.add_parser('validate', help='Validate all layers')
    validate_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Log inicio
    logger.info("=" * 60)
    logger.info("🎯 AUX & MART PIPELINE CLI")
    logger.info("=" * 60)
    logger.info(f"Command: {args.command}")
    logger.info(f"Campaign: {args.campaign}")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 60)
    
    success = False
    
    try:
        # Inicializar recursos
        logger.info("🔧 Initializing ETL resources...")
        await etl_dependencies.init_resources()
        logger.info("✅ Dependencies initialized")
        
        # Ejecutar comando
        if args.command == 'aux-only':
            success = await cmd_aux_only(args)
        elif args.command == 'mart-only':
            success = await cmd_mart_only(args)
        elif args.command == 'aux-mart':
            success = await cmd_aux_mart(args)
        elif args.command == 'validate':
            success = await cmd_validate(args)
        else:
            logger.error(f"❌ Unknown command: {args.command}")
            success = False
            
    except KeyboardInterrupt:
        logger.warning("⚠️ Interrupted by user")
        success = False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        success = False
    finally:
        try:
            await etl_dependencies.shutdown_resources()
            logger.info("✅ Resources shut down gracefully")
        except Exception as e:
            logger.error(f"❌ Shutdown error: {e}")
    
    # Log final
    duration = (datetime.now() - start_time).total_seconds()
    status = "SUCCESS" if success else "FAILED"
    
    logger.info("=" * 60)
    logger.info(f"🏁 EXECUTION {status}")
    logger.info(f"Command: {args.command}")
    logger.info(f"Campaign: {args.campaign}")
    logger.info(f"Duration: {duration:.2f}s ({duration/60:.1f} minutes)")
    logger.info("=" * 60)
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)