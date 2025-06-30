# etl/run_aux_mart_cli.py

"""
🎯 CLI PARA AUX Y MART: Comandos para ejecutar capas superiores

Este archivo contiene comandos CLI específicos para:
- AUX layer (transformaciones intermedias)
- MART layer (data marts de negocio)  
- Pipeline completo RAW→AUX→MART

EJECUCIÓN:
python etl/run_aux_mart_cli.py [comando] [opciones]

COMANDOS DISPONIBLES:
- aux-only: Ejecuta solo capa AUX
- mart-only: Ejecuta solo capa MART
- aux-mart: Ejecuta AUX + MART
- full-etl: Ejecuta RAW + AUX + MART
- validate: Valida todas las capas
"""

import asyncio
import argparse
import sys
from datetime import datetime, date
from typing import Optional, List

# Imports del sistema ETL
from etl.dependencies import etl_dependencies
from etl.models import CampaignWindow
from shared.core.logging import get_logger

logger = get_logger(__name__)


def parse_date(date_string: str) -> date:
    """Helper para parsear fechas desde argumentos."""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Use YYYY-MM-DD")


async def get_campaign_by_archivo(archivo: str) -> CampaignWindow:
    """
    Obtiene información de campaña desde la base de datos.
    
    Args:
        archivo: Nombre del archivo de campaña
        
    Returns:
        CampaignWindow con información de la campaña
    """
    try:
        # Usar extractor de BigQuery para obtener info de campaña
        extractor = etl_dependencies.bigquery_extractor()
        
        query = f"""
        SELECT 
            ARCHIVO as archivo,
            fecha_apertura,
            fecha_cierre,
            TIPO_CARTERA as tipo_cartera,
            ESTADO_CARTERA as estado_cartera
        FROM `mibot-222814.BI_USA.bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5`
        WHERE ARCHIVO = '{archivo}'
        LIMIT 1
        """
        
        async for batch in extractor.stream_custom_query(query, batch_size=1):
            if batch:
                row = batch[0]
                
                # Convertir fechas safely
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
                
                return CampaignWindow(
                    archivo=row['archivo'],
                    fecha_apertura=fecha_apertura,
                    fecha_cierre=fecha_cierre,
                    tipo_cartera=row.get('tipo_cartera', 'UNKNOWN'),
                    estado_cartera=row.get('estado_cartera', 'UNKNOWN')
                )
        
        raise ValueError(f"Campaign '{archivo}' not found in calendario")
        
    except Exception as e:
        raise Exception(f"Failed to get campaign info for '{archivo}': {str(e)}")


async def cmd_aux_only(args) -> bool:
    """🏗️ Comando: Ejecutar solo capa AUX"""
    try:
        logger.info("🏗️ Starting AUX-only execution...")
        
        # Obtener campaña
        campaign = await get_campaign_by_archivo(args.campaign)
        logger.info(f"📅 Campaign: {campaign.archivo} ({campaign.fecha_apertura} to {campaign.fecha_cierre})")
        
        # Ejecutar AUX
        aux_pipeline = await etl_dependencies.aux_build_pipeline()
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
        
        # Obtener campaña
        campaign = await get_campaign_by_archivo(args.campaign)
        logger.info(f"📅 Campaign: {campaign.archivo} ({campaign.fecha_apertura} to {campaign.fecha_cierre})")
        
        # Ejecutar MART
        mart_pipeline = await etl_dependencies.mart_build_pipeline()
        await mart_pipeline.run_for_campaign(campaign)
        
        logger.info("✅ MART execution completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ MART execution error: {e}")
        return False


async def cmd_aux_mart(args) -> bool:
    """🔄 Comando: Ejecutar AUX + MART"""
    try:
        logger.info("🔄 Starting AUX + MART execution...")
        
        # Obtener campaña
        campaign = await get_campaign_by_archivo(args.campaign)
        
        # Usar orquestador para AUX + MART
        orchestrator = await etl_dependencies.full_etl_orchestrator()
        result = await orchestrator.run_aux_mart_only(campaign)
        
        # Mostrar resultados
        logger.info("=" * 60)
        logger.info("📊 AUX + MART EXECUTION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Status: {result['overall_status'].upper()}")
        logger.info(f"Steps executed: {', '.join(result['steps_executed'])}")
        logger.info(f"Duration: {result['duration_seconds']:.2f}s")
        logger.info("=" * 60)
        
        return result['overall_status'] == 'success'
        
    except Exception as e:
        logger.error(f"❌ AUX + MART execution error: {e}")
        return False


async def cmd_full_etl(args) -> bool:
    """🚀 Comando: Ejecutar pipeline completo RAW→AUX→MART"""
    try:
        logger.info("🚀 Starting FULL ETL execution...")
        
        # Obtener campaña
        campaign = await get_campaign_by_archivo(args.campaign)
        
        # Usar orquestador para pipeline completo
        orchestrator = await etl_dependencies.full_etl_orchestrator()
        result = await orchestrator.run_full_etl_for_campaign(
            campaign,
            skip_raw=args.skip_raw,
            skip_aux=args.skip_aux,
            skip_mart=args.skip_mart,
            validate_steps=not args.no_validation
        )
        
        # Mostrar resultados detallados
        logger.info("=" * 60)
        logger.info("📊 FULL ETL EXECUTION RESULTS")
        logger.info("=" * 60)
        logger.info(result['summary'])
        logger.info("=" * 60)
        
        return result['overall_status'] == 'success'
        
    except Exception as e:
        logger.error(f"❌ Full ETL execution error: {e}")
        return False


async def cmd_validate(args) -> bool:
    """🔍 Comando: Validar todas las capas del pipeline"""
    try:
        logger.info("🔍 Starting pipeline validation...")
        
        # Obtener campaña
        campaign = await get_campaign_by_archivo(args.campaign)
        logger.info(f"📅 Campaign: {campaign.archivo} ({campaign.fecha_apertura} to {campaign.fecha_cierre})")
        
        # Validar usando orquestador
        orchestrator = await etl_dependencies.full_etl_orchestrator()
        validation_result = await orchestrator.validate_full_pipeline(campaign)
        
        # Mostrar resultados de validación
        logger.info("=" * 60)
        logger.info("🔍 PIPELINE VALIDATION RESULTS")
        logger.info("=" * 60)
        
        # RAW validation
        raw_status = validation_result['raw_validation']['status']
        raw_emoji = "✅" if raw_status == "pass" else "❌"
        logger.info(f"{raw_emoji} RAW Layer: {raw_status.upper()}")
        if raw_status == "fail":
            logger.info(f"    └─ Error: {validation_result['raw_validation'].get('error', 'Unknown')}")
        
        # AUX validation
        aux_status = validation_result['aux_validation'].get('overall_status', 'unknown')
        aux_emoji = "✅" if aux_status == "pass" else "❌"
        logger.info(f"{aux_emoji} AUX Layer: {aux_status.upper()}")
        if aux_status == "fail":
            aux_tables = validation_result['aux_validation'].get('table_validations', {})
            for table, result in aux_tables.items():
                if result.get('status') == 'fail':
                    logger.info(f"    └─ {table}: {result.get('record_count', 0)} records")
        
        # MART validation
        mart_status = validation_result['mart_validation']['status']
        mart_emoji = "✅" if mart_status == "pass" else "❌"
        logger.info(f"{mart_emoji} MART Layer: {mart_status.upper()}")
        if mart_status == "fail":
            logger.info(f"    └─ Error: {validation_result['mart_validation'].get('error', 'Unknown')}")
        
        # Overall status
        overall_status = validation_result['overall_status']
        overall_emoji = "✅" if overall_status == "pass" else "❌"
        logger.info(f"\n{overall_emoji} Overall Status: {overall_status.upper()}")
        logger.info("=" * 60)
        
        return overall_status == "pass"
        
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return False


def create_parser():
    """Crea el parser de argumentos para el CLI"""
    parser = argparse.ArgumentParser(
        description="AUX and MART Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS DE USO:

# Ejecutar solo AUX para una campaña
python etl/run_aux_mart_cli.py aux-only --campaign "CAMPAIGN_2024_001"

# Ejecutar solo MART para una campaña  
python etl/run_aux_mart_cli.py mart-only --campaign "CAMPAIGN_2024_001"

# Ejecutar AUX + MART (asumiendo RAW ya cargado)
python etl/run_aux_mart_cli.py aux-mart --campaign "CAMPAIGN_2024_001"

# Ejecutar pipeline completo RAW→AUX→MART
python etl/run_aux_mart_cli.py full-etl --campaign "CAMPAIGN_2024_001"

# Ejecutar solo AUX + MART (saltar RAW)
python etl/run_aux_mart_cli.py full-etl --campaign "CAMPAIGN_2024_001" --skip-raw

# Validar todas las capas
python etl/run_aux_mart_cli.py validate --campaign "CAMPAIGN_2024_001"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Comando aux-only
    aux_parser = subparsers.add_parser('aux-only', help='Execute only AUX layer')
    aux_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # Comando mart-only
    mart_parser = subparsers.add_parser('mart-only', help='Execute only MART layer')
    mart_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # Comando aux-mart
    aux_mart_parser = subparsers.add_parser('aux-mart', help='Execute AUX + MART layers')
    aux_mart_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # Comando full-etl
    full_parser = subparsers.add_parser('full-etl', help='Execute complete RAW→AUX→MART pipeline')
    full_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    full_parser.add_argument('--skip-raw', action='store_true', help='Skip RAW layer execution')
    full_parser.add_argument('--skip-aux', action='store_true', help='Skip AUX layer execution')
    full_parser.add_argument('--skip-mart', action='store_true', help='Skip MART layer execution')
    full_parser.add_argument('--no-validation', action='store_true', help='Skip step validations')
    
    # Comando validate
    validate_parser = subparsers.add_parser('validate', help='Validate all pipeline layers')
    validate_parser.add_argument('--campaign', required=True, help='Campaign archivo name')
    
    # Opciones globales
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Set logging level')
    
    return parser


async def main():
    """Función principal del CLI"""
    start_time = datetime.now()
    success = False
    
    try:
        # Parse arguments
        parser = create_parser()
        args = parser.parse_args()
        
        if not args.command:
            parser.print_help()
            return
        
        # Configure logging
        import logging
        logging.getLogger().setLevel(getattr(logging, args.log_level))
        
        logger.info("=" * 60)
        logger.info("🎯 AUX & MART PIPELINE CLI")
        logger.info("=" * 60)
        logger.info(f"Command: {args.command}")
        logger.info(f"Campaign: {args.campaign}")
        logger.info(f"Start time: {start_time}")
        logger.info("=" * 60)
        
        # Initialize dependencies
        await etl_dependencies.init_resources()
        logger.info("✅ Dependencies initialized")
        
        # Execute command
        if args.command == 'aux-only':
            success = await cmd_aux_only(args)
        elif args.command == 'mart-only':
            success = await cmd_mart_only(args)
        elif args.command == 'aux-mart':
            success = await cmd_aux_mart(args)
        elif args.command == 'full-etl':
            success = await cmd_full_etl(args)
        elif args.command == 'validate':
            success = await cmd_validate(args)
        else:
            logger.error(f"❌ Unknown command: {args.command}")
            parser.print_help()
            success = False
        
        # Final summary
        duration = (datetime.now() - start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"
        
        logger.info("=" * 60)
        logger.info(f"🏁 EXECUTION {status}")
        logger.info(f"Command: {args.command}")
        logger.info(f"Campaign: {args.campaign}")
        logger.info(f"Duration: {duration:.2f}s ({duration/60:.1f} minutes)")
        logger.info("=" * 60)
        
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
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())