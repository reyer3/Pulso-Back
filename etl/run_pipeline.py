# run_pipeline.py

"""
🚀 ETL Pipeline Runner - Híbrido Calendario + Watermarks

NUEVAS FUNCIONALIDADES:
- Backfill histórico guiado por calendario
- Incrementales modernos con watermarks
- Estrategias de extracción configurables
- Soporte para cargas retroactivas de 3 meses

MODOS DISPONIBLES:
1. catchup-calendar: Backfill histórico por campañas
2. catchup-incremental: Refresh incremental con watermarks
3. catchup-hybrid: Decisión automática inteligente
4. catchup-tables: Procesar tablas específicas

USAGE:
    # Backfill histórico (tu caso de 3 meses)
    python run_pipeline.py catchup-calendar --from-date 2024-10-01 --to-date 2024-12-31

    # Incremental diario
    python run_pipeline.py catchup-incremental

    # Híbrido automático
    python run_pipeline.py catchup-hybrid --limit 10

    # Tablas específicas
    python run_pipeline.py catchup-tables --tables calendario,asignaciones --strategy calendar
"""

import asyncio
import argparse
import sys
from datetime import datetime, date
from typing import Optional, List

# Imports del sistema ETL
from etl.dependencies import etl_dependencies
from etl.pipelines.campaign_catchup_pipeline import CampaignCatchUpPipeline
from etl.pipelines.hybrid_raw_data_pipeline import HybridRawDataPipeline, ExtractionStrategy
from etl.models import CampaignWindow
from etl.config import ETLConfig
from shared.core.logging import get_logger

# Logger para el runner
logger = get_logger(__name__)


async def get_campaigns_in_date_range(start_date: date, end_date: date) -> List[CampaignWindow]:
    """
    🗓️ Obtiene campañas en un rango de fechas desde BigQuery/PostgreSQL.

    Esta función debe implementarse según tu fuente de datos de calendario.
    Por ahora es un placeholder que debes adaptar.
    """
    # TODO: Implementar consulta real a tu tabla de calendario
    # Ejemplo básico - adaptar a tu implementación real

    logger.info(f"📅 Fetching campaigns from {start_date} to {end_date}")

    # Placeholder - reemplazar con tu lógica real
    from etl.extractors.bigquery_extractor import BigQueryExtractor
    extractor = etl_dependencies.bigquery_extractor()

    query = f"""
    SELECT 
        ARCHIVO as archivo,
        fecha_apertura,
        fecha_cierre,
        TIPO_CARTERA as tipo_cartera,
        ESTADO_CARTERA as estado_cartera
    FROM `{ETLConfig.PROJECT_ID}.{ETLConfig.BQ_DATASET}.{ETLConfig.get_config('calendario').source_table}`
    WHERE fecha_apertura BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY fecha_apertura
    """

    # Ejecutar query y convertir a CampaignWindow objects
    campaigns = []
    async for batch in extractor.stream_custom_query(query, batch_size=1000):
        for row in batch:
            campaign = CampaignWindow(
                archivo=row['archivo'],
                fecha_apertura=row['fecha_apertura'],
                fecha_cierre=row.get('fecha_cierre'),
                tipo_cartera=row.get('tipo_cartera', 'UNKNOWN'),
                estado_cartera=row.get('estado_cartera', 'UNKNOWN')
            )
            campaigns.append(campaign)

    logger.info(f"📅 Found {len(campaigns)} campaigns in date range")
    return campaigns


async def run_calendar_backfill(args) -> bool:
    """
    🗓️ Ejecuta backfill histórico guiado por calendario.
    IDEAL PARA: Tu caso de cargas retroactivas de 3 meses.
    """
    try:
        logger.info("🗓️ Starting calendar-driven backfill...")

        # Validar fechas requeridas
        if not args.from_date or not args.to_date:
            logger.error("❌ Calendar backfill requires --from-date and --to-date")
            return False

        # Obtener pipeline híbrido
        hybrid_pipeline: HybridRawDataPipeline = etl_dependencies.hybrid_raw_pipeline()

        # Obtener campañas en el rango
        campaigns = await get_campaigns_in_date_range(args.from_date, args.to_date)

        if not campaigns:
            logger.warning(f"⚠️ No campaigns found between {args.from_date} and {args.to_date}")
            return True

        # Filtrar por límite si se especifica
        if args.limit:
            campaigns = campaigns[:args.limit]
            logger.info(f"🔢 Limited to first {args.limit} campaigns")

        # Ejecutar backfill
        start_time = datetime.now()

        result = await hybrid_pipeline.run_calendar_backfill(
            campaigns=campaigns,
            specific_tables=args.tables if hasattr(args, 'tables') and args.tables else None,
            extend_windows=not args.exact_windows,
            update_watermarks=not args.no_watermarks
        )

        duration = (datetime.now() - start_time).total_seconds()

        # Mostrar resultados detallados
        logger.info("=" * 80)
        logger.info("📊 CALENDAR BACKFILL RESULTS")
        logger.info("=" * 80)
        logger.info(f"📅 Date range: {args.from_date} to {args.to_date}")
        logger.info(f"🎯 Campaigns processed: {result['successful_campaigns']}/{result['total_campaigns']}")
        logger.info(f"📊 Total records loaded: {result['total_records']:,}")
        logger.info(f"⏱️ Duration: {duration:.2f}s")
        logger.info(f"🔄 Watermarks updated: {result['watermarks_updated']}")

        # Detalles por campaña si hay fallos
        failed_campaigns = [r for r in result['campaign_results'] if r['tables_failed'] > 0]
        if failed_campaigns:
            logger.warning(f"⚠️ {len(failed_campaigns)} campaigns had failures:")
            for campaign_result in failed_campaigns:
                logger.warning(
                    f"  - {campaign_result['campaign']}: "
                    f"{campaign_result['tables_failed']} tables failed"
                )

        return result['status'] in ['success', 'partial']

    except Exception as e:
        logger.error(f"❌ Calendar backfill failed: {e}", exc_info=True)
        return False


async def run_incremental_refresh(args) -> bool:
    """
    ⏰ Ejecuta refresh incremental con watermarks.
    IDEAL PARA: Cargas diarias automáticas.
    """
    try:
        logger.info("⏰ Starting watermark-driven incremental refresh...")

        # Obtener pipeline híbrido
        hybrid_pipeline: HybridRawDataPipeline = etl_dependencies.hybrid_raw_pipeline()

        # Ejecutar incremental
        start_time = datetime.now()

        result = await hybrid_pipeline.run_incremental_refresh(
            specific_tables=args.tables if hasattr(args, 'tables') and args.tables else None,
            force_full_refresh=args.force
        )

        duration = (datetime.now() - start_time).total_seconds()

        # Mostrar resultados
        logger.info("=" * 80)
        logger.info("📊 INCREMENTAL REFRESH RESULTS")
        logger.info("=" * 80)
        logger.info(f"✅ Successful tables: {len(result['successful_tables'])}")
        logger.info(f"❌ Failed tables: {len(result['failed_tables'])}")
        logger.info(f"📊 Records loaded: {result['total_records_loaded']:,}")
        logger.info(f"⏱️ Duration: {duration:.2f}s")
        logger.info(f"🧹 Stale extractions cleaned: {result['stale_extractions_cleaned']}")

        if result['failed_tables']:
            logger.warning(f"⚠️ Failed tables: {', '.join(result['failed_tables'])}")

        return result['status'] in ['success', 'partial']

    except Exception as e:
        logger.error(f"❌ Incremental refresh failed: {e}", exc_info=True)
        return False


async def run_hybrid_auto(args) -> bool:
    """
    🎯 Ejecuta modo híbrido con decisión automática.
    COMBINA: Calendario para histórico + Watermarks para reciente.
    """
    try:
        logger.info("🎯 Starting hybrid auto-strategy pipeline...")

        # Usar el pipeline de catchup tradicional con estrategia híbrida
        catchup_pipeline: CampaignCatchUpPipeline = etl_dependencies.campaign_catchup_pipeline()

        # Parámetros híbridos
        pipeline_params = {
            'force_refresh_all': args.force,
            'max_campaigns': args.limit,
            'dry_run': args.dry_run,
            'skip_validation': args.skip_validation,
            'parallel_workers': args.parallel,
            'extraction_strategy': 'hybrid_auto'  # Nuevo parámetro
        }

        start_time = datetime.now()

        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - Validating hybrid strategy")
            result = await catchup_pipeline.validate_pending_campaigns()
        else:
            result = await catchup_pipeline.run_all_pending_campaigns(**pipeline_params)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 80)
        logger.info("📊 HYBRID AUTO RESULTS")
        logger.info("=" * 80)
        logger.info(f"📊 Result: {result}")
        logger.info(f"⏱️ Duration: {duration:.2f}s")

        return True

    except Exception as e:
        logger.error(f"❌ Hybrid auto pipeline failed: {e}", exc_info=True)
        return False


async def run_specific_tables(args) -> bool:
    """
    🎯 Ejecuta procesamiento de tablas específicas con estrategia configurable.
    """
    try:
        if not args.tables:
            logger.error("❌ --tables is required for specific table processing")
            return False

        logger.info(f"🎯 Processing specific tables: {args.tables}")

        # Obtener pipeline híbrido
        hybrid_pipeline: HybridRawDataPipeline = etl_dependencies.hybrid_raw_pipeline()

        # Determinar estrategia
        if args.strategy == 'calendar':
            if not args.from_date or not args.to_date:
                logger.error("❌ Calendar strategy requires --from-date and --to-date")
                return False

            campaigns = await get_campaigns_in_date_range(args.from_date, args.to_date)

            result = await hybrid_pipeline.run_calendar_backfill(
                campaigns=campaigns,
                specific_tables=args.tables,
                extend_windows=not args.exact_windows,
                update_watermarks=not args.no_watermarks
            )

        elif args.strategy == 'incremental':
            result = await hybrid_pipeline.run_incremental_refresh(
                specific_tables=args.tables,
                force_full_refresh=args.force
            )

        else:
            logger.error(f"❌ Unknown strategy: {args.strategy}")
            return False

        logger.info(f"📊 Specific tables result: {result['status']}")
        return result['status'] in ['success', 'partial']

    except Exception as e:
        logger.error(f"❌ Specific tables processing failed: {e}", exc_info=True)
        return False


def parse_date(date_string: str) -> date:
    """Helper para parsear fechas desde argumentos."""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Use YYYY-MM-DD")


def parse_table_list(table_string: str) -> List[str]:
    """Helper para parsear lista de tablas desde argumentos."""
    return [table.strip() for table in table_string.split(',') if table.strip()]


async def main():
    """
    Punto de entrada principal - ACTUALIZADO para pipeline híbrido.
    """
    start_time = datetime.now()
    success = False

    try:
        # Configurar parser principal
        parser = argparse.ArgumentParser(
            description="Pulso-Back ETL Pipeline Runner - Hybrid Calendar + Watermarks",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
🎯 PIPELINE MODES:

1. CALENDAR BACKFILL (Histórico):
   python run_pipeline.py catchup-calendar --from-date 2024-10-01 --to-date 2024-12-31

2. INCREMENTAL REFRESH (Diario):
   python run_pipeline.py catchup-incremental

3. HYBRID AUTO (Inteligente):
   python run_pipeline.py catchup-hybrid --limit 10

4. SPECIFIC TABLES (Selectivo):
   python run_pipeline.py catchup-tables --tables calendario,asignaciones --strategy calendar --from-date 2024-12-01 --to-date 2024-12-31

📅 CALENDAR OPTIONS:
   --from-date YYYY-MM-DD    Start date for calendar mode
   --to-date YYYY-MM-DD      End date for calendar mode
   --exact-windows           Use exact campaign dates (no extension)
   --no-watermarks           Skip watermark updates

⏰ INCREMENTAL OPTIONS:
   --force                   Force full refresh instead of incremental

🎯 TABLE OPTIONS:
   --tables table1,table2    Process specific tables only
   --strategy calendar|incremental  Strategy for specific tables

🔧 GENERAL OPTIONS:
   --limit N                 Limit number of campaigns/tables
   --parallel N              Parallel workers (default: 3)
   --dry-run                 Validate without executing
   --log-level DEBUG|INFO    Logging level
            """
        )

        # Subcommands para diferentes modos
        subparsers = parser.add_subparsers(dest='pipeline', help='Pipeline mode')

        # 1. Calendar backfill parser
        calendar_parser = subparsers.add_parser(
            'catchup-calendar',
            help='Historical backfill guided by calendar'
        )
        calendar_parser.add_argument('--from-date', type=parse_date, required=True, help='Start date (YYYY-MM-DD)')
        calendar_parser.add_argument('--to-date', type=parse_date, required=True, help='End date (YYYY-MM-DD)')
        calendar_parser.add_argument('--limit', type=int, help='Limit number of campaigns')
        calendar_parser.add_argument('--exact-windows', action='store_true', help='Use exact campaign dates')
        calendar_parser.add_argument('--no-watermarks', action='store_true', help='Skip watermark updates')
        calendar_parser.add_argument('--tables', type=parse_table_list, help='Specific tables (comma-separated)')

        # 2. Incremental refresh parser
        incremental_parser = subparsers.add_parser(
            'catchup-incremental',
            help='Incremental refresh with watermarks'
        )
        incremental_parser.add_argument('--force', action='store_true', help='Force full refresh')
        incremental_parser.add_argument('--tables', type=parse_table_list, help='Specific tables (comma-separated)')

        # 3. Hybrid auto parser
        hybrid_parser = subparsers.add_parser(
            'catchup-hybrid',
            help='Hybrid auto-strategy (intelligent decision)'
        )
        hybrid_parser.add_argument('--force', action='store_true', help='Force refresh all')
        hybrid_parser.add_argument('--limit', type=int, help='Limit campaigns')
        hybrid_parser.add_argument('--dry-run', action='store_true', help='Validate only')
        hybrid_parser.add_argument('--skip-validation', action='store_true', help='Skip validation checks')
        hybrid_parser.add_argument('--parallel', type=int, default=3, help='Parallel workers')

        # 4. Specific tables parser
        tables_parser = subparsers.add_parser(
            'catchup-tables',
            help='Process specific tables with chosen strategy'
        )
        tables_parser.add_argument('--tables', type=parse_table_list, required=True, help='Tables to process')
        tables_parser.add_argument('--strategy', choices=['calendar', 'incremental'], required=True,
                                   help='Extraction strategy')
        tables_parser.add_argument('--from-date', type=parse_date, help='Start date for calendar strategy')
        tables_parser.add_argument('--to-date', type=parse_date, help='End date for calendar strategy')
        tables_parser.add_argument('--force', action='store_true', help='Force full refresh')
        tables_parser.add_argument('--exact-windows', action='store_true', help='Use exact campaign dates')
        tables_parser.add_argument('--no-watermarks', action='store_true', help='Skip watermark updates')

        # Argumentos globales
        parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                            help='Logging level')

        # Parse arguments
        args = parser.parse_args()

        if not args.pipeline:
            parser.print_help()
            return

        # Configurar logging
        import logging
        logging.getLogger().setLevel(getattr(logging, args.log_level))

        logger.info("=" * 80)
        logger.info("🚀 PULSO-BACK ETL PIPELINE RUNNER - HYBRID")
        logger.info("=" * 80)
        logger.info(f"Mode: {args.pipeline}")
        logger.info(f"Arguments: {vars(args)}")
        logger.info(f"Start time: {start_time}")

        # Inicializar dependencias
        logger.info("🔌 Initializing ETL dependencies...")
        await etl_dependencies.init_resources()
        logger.info("✅ Dependencies initialized successfully")

        # Ejecutar pipeline según el modo
        if args.pipeline == 'catchup-calendar':
            success = await run_calendar_backfill(args)
        elif args.pipeline == 'catchup-incremental':
            success = await run_incremental_refresh(args)
        elif args.pipeline == 'catchup-hybrid':
            success = await run_hybrid_auto(args)
        elif args.pipeline == 'catchup-tables':
            success = await run_specific_tables(args)
        else:
            logger.error(f"❌ Unknown pipeline mode: {args.pipeline}")
            success = False

        # Mostrar resumen final
        duration = (datetime.now() - start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"

        logger.info("=" * 80)
        logger.info(f"🏁 PIPELINE EXECUTION {status}")
        logger.info(f"Mode: {args.pipeline}")
        logger.info(f"Total duration: {duration:.2f}s")
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline execution interrupted by user")
        success = False
    except Exception as e:
        logger.error(f"❌ Unexpected error in main: {e}", exc_info=True)
        success = False
    finally:
        # Cleanup resources
        try:
            logger.info("🔌 Shutting down resources...")
            await etl_dependencies.shutdown_resources()
            logger.info("✅ Resources shut down gracefully")
        except Exception as e:
            logger.error(f"❌ Error during resource cleanup: {e}")

    # Exit con código apropiado
    exit_code = 0 if success else 1
    logger.info(f"🚪 Exiting with code {exit_code}")
    sys.exit(exit_code)


if __name__ == "__main__":
    # Configurar manejo de excepciones
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical("❌ Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


    sys.excepthook = handle_exception

    # Ejecutar pipeline
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Pipeline runner terminated by user")
        sys.exit(130)