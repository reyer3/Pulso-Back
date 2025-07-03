#!/usr/bin/env python3
"""
🚀 ETL Entry Point - Standard CLI Interface

Entry point estándar para ejecutar el pipeline ETL incremental.
Maneja argumentos, logging, y orquestación básica.

Usage:
    python etl/main.py                              # Procesar todas las tablas
    python etl/main.py --tables asignaciones pagos  # Tablas específicas
    python etl/main.py --log-level DEBUG            # Con debug
    python etl/main.py --dry-run                    # Solo mostrar qué se haría

Autor: Ricky para Pulso-Back
"""

import asyncio
import argparse
import sys
import logging
from typing import List, Optional

from etl.pipelines.simple_incremental_pipeline import SimpleIncrementalPipeline
from etl.config import ETLConfig


def setup_logging(level: str = "INFO") -> None:
    """
    Configurar logging para el ETL
    
    Args:
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configurar loggers específicos
    etl_logger = logging.getLogger('etl')
    etl_logger.setLevel(getattr(logging, level.upper()))


def parse_arguments() -> argparse.Namespace:
    """
    Parsear argumentos de línea de comandos
    
    Returns:
        Namespace con argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description="ETL Incremental Pipeline - Extrae y carga datos desde BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Procesar todas las tablas
  %(prog)s --tables asignaciones trandeuda   # Tablas específicas
  %(prog)s --log-level DEBUG                 # Con logging detallado
  %(prog)s --dry-run                         # Solo mostrar plan de ejecución
  %(prog)s --list-tables                     # Listar tablas disponibles
        """
    )
    
    parser.add_argument(
        '--tables',
        nargs='+',
        help='Tablas específicas a procesar (default: todas las configuradas)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Nivel de logging (default: INFO)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Mostrar qué tablas se procesarían sin ejecutar'
    )
    
    parser.add_argument(
        '--list-tables',
        action='store_true',
        help='Listar todas las tablas disponibles y salir'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='ETL Pipeline 1.0.0'
    )
    
    return parser.parse_args()


def validate_tables(table_names: Optional[List[str]]) -> List[str]:
    """
    Validar que las tablas especificadas existen en la configuración
    
    Args:
        table_names: Lista de nombres de tablas a validar
        
    Returns:
        Lista de tablas válidas
        
    Raises:
        SystemExit: Si alguna tabla no existe
    """
    if not table_names:
        return ETLConfig.get_raw_source_tables()
    
    available_tables = ETLConfig.get_raw_source_tables()
    invalid_tables = [t for t in table_names if t not in available_tables]
    
    if invalid_tables:
        print(f"❌ Error: Tablas no válidas: {', '.join(invalid_tables)}", file=sys.stderr)
        print(f"📋 Tablas disponibles: {', '.join(available_tables)}", file=sys.stderr)
        sys.exit(1)
    
    return table_names


def list_available_tables() -> None:
    """
    Mostrar todas las tablas disponibles y sus configuraciones
    """
    print("📋 TABLAS DISPONIBLES PARA ETL:")
    print("=" * 60)
    
    tables = ETLConfig.get_raw_source_tables()
    
    for table_name in sorted(tables):
        try:
            config = ETLConfig.get_config(table_name)
            print(f"📊 {table_name}")
            print(f"   Source: {config.source_table}")
            print(f"   Incremental: {config.incremental_column or 'Full refresh'}")
            print(f"   Primary Key: {', '.join(config.primary_key)}")
            print(f"   Batch Size: {config.batch_size:,}")
            print()
        except Exception as e:
            print(f"❌ {table_name}: Error en configuración - {e}")
    
    print(f"📊 Total: {len(tables)} tablas configuradas")


async def run_dry_run(tables: List[str]) -> None:
    """
    Ejecutar modo dry-run (solo mostrar plan)
    
    Args:
        tables: Lista de tablas a procesar
    """
    print("🔍 DRY RUN MODE - Plan de Ejecución")
    print("=" * 50)
    print(f"📊 Tablas a procesar: {len(tables)}")
    print(f"📋 Lista de tablas:")
    
    for i, table_name in enumerate(tables, 1):
        try:
            config = ETLConfig.get_config(table_name)
            print(f"  {i}. {table_name}")
            print(f"     Source: {config.source_table}")
            print(f"     Incremental: {config.incremental_column or 'Full refresh'}")
        except Exception as e:
            print(f"  {i}. {table_name} - ❌ Error: {e}")
    
    print("\n✅ Dry run completado. Use sin --dry-run para ejecutar.")


async def main() -> None:
    """
    Función principal del ETL
    """
    # Parsear argumentos
    args = parse_arguments()
    
    # Configurar logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Comando especial: listar tablas
        if args.list_tables:
            list_available_tables()
            return
        
        # Validar tablas
        tables_to_process = validate_tables(args.tables)
        
        # Comando especial: dry run
        if args.dry_run:
            await run_dry_run(tables_to_process)
            return
        
        # Ejecutar pipeline ETL
        logger.info("🚀 Starting ETL Pipeline")
        logger.info(f"📊 Tables to process: {len(tables_to_process)}")
        logger.info(f"📋 Tables: {', '.join(tables_to_process)}")
        
        # Crear y ejecutar pipeline
        pipeline = SimpleIncrementalPipeline()
        
        try:
            result = await pipeline.process_tables(tables_to_process)
            
            # Determinar exit code basado en resultado
            if result["status"] == "success":
                logger.info("🎉 ETL Pipeline completed successfully!")
                exit_code = 0
            else:
                logger.warning(
                    f"⚠️ ETL Pipeline completed with issues: "
                    f"{result['successful_tables']}/{result['total_tables']} tables successful"
                )
                exit_code = 1
            
        finally:
            # Cleanup resources
            await pipeline.cleanup()
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("⚠️ ETL interrupted by user (Ctrl+C)")
        sys.exit(130)  # Standard exit code for SIGINT
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
