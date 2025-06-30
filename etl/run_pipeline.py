# etl/run_pipeline.py - TEMPORAL FIX

"""
🚀 ETL Pipeline Runner - Con fallback temporal

TEMPORAL FIX: Usa el pipeline existente si el híbrido no está disponible
"""

import asyncio
import argparse
import sys
from datetime import datetime, date, timedelta
from typing import Optional, List

# Imports del sistema ETL
from etl.dependencies import etl_dependencies
from etl.pipelines.campaign_catchup_pipeline import CampaignCatchUpPipeline
from etl.models import CampaignWindow
from etl.config import ETLConfig
from shared.core.logging import get_logger

# Logger para el runner
logger = get_logger(__name__)

# 🔧 TEMPORAL: Try import híbrido desde raw_data_pipeline.py
try:
    from etl.pipelines.raw_data_pipeline import HybridRawDataPipeline, ExtractionStrategy

    HYBRID_AVAILABLE = True
    logger.info("✅ Hybrid pipeline available")
except ImportError as e:
    logger.warning(f"⚠️ Hybrid pipeline not available, using fallback: {e}")
    from etl.pipelines.raw_data_pipeline import RawDataPipeline

    HYBRID_AVAILABLE = False


async def get_campaigns_in_date_range(start_date: date, end_date: date) -> List[CampaignWindow]:
    """
    🗓️ Obtiene campañas en un rango de fechas desde BigQuery.
    """
    logger.info(f"📅 Fetching campaigns from {start_date} to {end_date}")

    # Usar extractor de BigQuery para obtener campañas
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

    campaigns = []
    try:
        async for batch in extractor.stream_custom_query(query, batch_size=1000):
            for row in batch:
                # Safely convert date fields
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
                campaigns.append(campaign)
    except Exception as e:
        logger.error(f"❌ Error fetching campaigns: {e}")
        raise

    logger.info(f"📅 Found {len(campaigns)} campaigns in date range")
    return campaigns


async def run_calendar_backfill(args) -> bool:
    """
    🗓️ Ejecuta backfill histórico - con fallback temporal.
    """
    try:
        logger.info("🗓️ Starting calendar-driven backfill...")

        if not args.from_date or not args.to_date:
            logger.error("❌ Calendar backfill requires --from-date and --to-date")
            return False

        # Obtener campañas
        campaigns = await get_campaigns_in_date_range(args.from_date, args.to_date)

        if not campaigns:
            logger.warning(f"⚠️ No campaigns found between {args.from_date} and {args.to_date}")
            return True

        if args.limit:
            campaigns = campaigns[:args.limit]
            logger.info(f"🔢 Limited to first {args.limit} campaigns")

        start_time = datetime.now()

        if HYBRID_AVAILABLE:
            # 🆕 Usar pipeline híbrido
            try:
                hybrid_pipeline = await etl_dependencies.hybrid_raw_pipeline()

                result = await hybrid_pipeline.run_calendar_backfill(
                    campaigns=campaigns,
                    specific_tables=args.tables if hasattr(args, 'tables') and args.tables else None,
                    extend_windows=not args.exact_windows,
                    update_watermarks=not args.no_watermarks
                )

                logger.info("✅ Used hybrid pipeline successfully")

            except Exception as e:
                logger.error(f"❌ Hybrid pipeline failed: {e}")
                return False
        else:
            # 🔄 FALLBACK: Usar pipeline original con bucle manual
            logger.info("🔄 Using fallback: original pipeline with manual loop")

            raw_pipeline = await etl_dependencies.raw_data_pipeline()

            successful_campaigns = 0
            total_records = 0

            for i, campaign in enumerate(campaigns, 1):
                logger.info(f"📅 Processing campaign {i}/{len(campaigns)}: {campaign.archivo}")

                try:
                    campaign_result = await raw_pipeline.run_for_campaign(campaign)

                    if campaign_result['status'] in ['success', 'partial']:
                        successful_campaigns += 1
                        total_records += campaign_result.get('total_records_loaded', 0)
                        logger.info(f"✅ Campaign {campaign.archivo} completed")
                    else:
                        logger.error(f"❌ Campaign {campaign.archivo} failed")

                except Exception as e:
                    logger.error(f"❌ Campaign {campaign.archivo} error: {e}")
                    continue

            # Crear resultado compatible
            result = {
                'status': 'success' if successful_campaigns == len(campaigns) else 'partial',
                'successful_campaigns': successful_campaigns,
                'total_campaigns': len(campaigns),
                'total_records': total_records,
                'watermarks_updated': False  # Original pipeline no maneja watermarks
            }

        duration = (datetime.now() - start_time).total_seconds()

        # Mostrar resultados
        logger.info("=" * 80)
        logger.info("📊 CALENDAR BACKFILL RESULTS")
        logger.info("=" * 80)
        logger.info(f"📅 Date range: {args.from_date} to {args.to_date}")
        logger.info(f"🎯 Campaigns processed: {result['successful_campaigns']}/{result['total_campaigns']}")
        logger.info(f"📊 Total records loaded: {result['total_records']:,}")
        logger.info(f"⏱️ Duration: {duration:.2f}s")
        logger.info(f"🔄 Watermarks updated: {result['watermarks_updated']}")

        return result['status'] in ['success', 'partial']

    except Exception as e:
        logger.error(f"❌ Calendar backfill failed: {e}", exc_info=True)
        return False


async def run_incremental_refresh(args) -> bool:
    """
    ⏰ Ejecuta refresh incremental - con fallback temporal.
    """
    try:
        logger.info("⏰ Starting incremental refresh...")

        if HYBRID_AVAILABLE:
            # Usar pipeline híbrido
            try:
                hybrid_pipeline = await etl_dependencies.hybrid_raw_pipeline()

                result = await hybrid_pipeline.run_incremental_refresh(
                    specific_tables=args.tables if hasattr(args, 'tables') and args.tables else None,
                    force_full_refresh=args.force
                )

                logger.info("✅ Used hybrid incremental successfully")

            except Exception as e:
                logger.error(f"❌ Hybrid incremental failed: {e}")
                return False
        else:
            # FALLBACK: Sin watermarks, usar full refresh
            logger.warning("🔄 Fallback mode: Using full refresh (no watermarks)")

            raw_pipeline = await etl_dependencies.raw_data_pipeline()

            # Sin campañas específicas, usar todas las tablas con full refresh
            fake_campaign = CampaignWindow(
                archivo="INCREMENTAL_RUN",
                fecha_apertura=date.today() - timedelta(days=30),
                fecha_cierre=date.today(),
                tipo_cartera="ALL",
                estado_cartera="OPEN"
            )

            campaign_result = await raw_pipeline.run_for_campaign(fake_campaign)

            result = {
                'status': campaign_result['status'],
                'successful_tables': campaign_result.get('successful_tables', []),
                'failed_tables': campaign_result.get('failed_tables', []),
                'total_records_loaded': campaign_result.get('total_records_loaded', 0),
                'stale_extractions_cleaned': 0
            }

        # Mostrar resultados
        logger.info("=" * 80)
        logger.info("📊 INCREMENTAL REFRESH RESULTS")
        logger.info("=" * 80)
        logger.info(f"✅ Successful tables: {len(result['successful_tables'])}")
        logger.info(f"❌ Failed tables: {len(result['failed_tables'])}")
        logger.info(f"📊 Records loaded: {result['total_records_loaded']:,}")

        return result['status'] in ['success', 'partial']

    except Exception as e:
        logger.error(f"❌ Incremental refresh failed: {e}", exc_info=True)
        return False


async def run_hybrid_auto(args) -> bool:
    """
    🎯 Ejecuta modo híbrido - con fallback temporal.
    """
    try:
        logger.info("🎯 Starting hybrid auto-strategy...")

        if not HYBRID_AVAILABLE:
            logger.warning("⚠️ Hybrid mode not available, using traditional catchup")

        # Usar pipeline de catchup tradicional
        catchup_pipeline: CampaignCatchUpPipeline = await etl_dependencies.campaign_catchup_pipeline()

        # 🔧 FIX: Solo pasar parámetros que acepta el método
        pipeline_params = {
            'force_refresh_all': args.force,
            'max_campaigns': args.limit,
            'batch_size': args.parallel,  # Usar parallel como batch_size
        }

        start_time = datetime.now()

        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - Listing campaigns to process...")
            # Obtener campañas para mostrar qué se procesaría
            campaigns = await catchup_pipeline.get_campaign_windows(limit=args.limit)
            
            logger.info("=" * 80)
            logger.info("📋 DRY RUN RESULTS")
            logger.info("=" * 80)
            logger.info(f"📊 Total campaigns found: {len(campaigns)}")
            
            for i, campaign in enumerate(campaigns[:10], 1):  # Mostrar máximo 10
                logger.info(f"  {i}. {campaign.archivo} ({campaign.fecha_apertura} - {campaign.fecha_cierre})")
            
            if len(campaigns) > 10:
                logger.info(f"  ... and {len(campaigns) - 10} more campaigns")
                
            return True
        else:
            # Ejecutar el pipeline real
            result = await catchup_pipeline.run_all_pending_campaigns(**pipeline_params)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 80)
        logger.info("📊 HYBRID AUTO RESULTS")
        logger.info("=" * 80)
        logger.info(f"📊 Status: {result.get('status', 'unknown')}")
        logger.info(f"🎯 Campaigns processed: {result.get('campaigns_processed', 0)}")
        logger.info(f"✅ Successful: {result.get('campaigns_successful', 0)}")
        logger.info(f"❌ Failed: {result.get('campaigns_failed', 0)}")
        logger.info(f"📊 Raw records: {result.get('total_raw_records', 0):,}")
        logger.info(f"📊 Mart records: {result.get('total_mart_records', 0):,}")
        logger.info(f"⏱️ Duration: {duration:.2f}s")

        return result.get('status') in ['completed', 'success']

    except Exception as e:
        logger.error(f"❌ Hybrid auto failed: {e}", exc_info=True)
        return False


# ... (resto del código igual: parse_date, parse_table_list, main, etc.)
# Solo cambian las funciones de arriba

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
    """Punto de entrada principal con soporte para fallback."""
    start_time = datetime.now()
    success = False

    try:
        # Parser básico para el fix temporal
        parser = argparse.ArgumentParser(description="ETL Pipeline Runner - Hybrid with Fallback")

        subparsers = parser.add_subparsers(dest='pipeline', help='Pipeline mode')

        # Calendar parser
        calendar_parser = subparsers.add_parser('catchup-calendar')
        calendar_parser.add_argument('--from-date', type=parse_date, required=True)
        calendar_parser.add_argument('--to-date', type=parse_date, required=True)
        calendar_parser.add_argument('--limit', type=int)
        calendar_parser.add_argument('--exact-windows', action='store_true')
        calendar_parser.add_argument('--no-watermarks', action='store_true')
        calendar_parser.add_argument('--tables', type=parse_table_list)

        # Incremental parser
        incremental_parser = subparsers.add_parser('catchup-incremental')
        incremental_parser.add_argument('--force', action='store_true')
        incremental_parser.add_argument('--tables', type=parse_table_list)

        # Hybrid parser
        hybrid_parser = subparsers.add_parser('catchup-hybrid')
        hybrid_parser.add_argument('--force', action='store_true')
        hybrid_parser.add_argument('--limit', type=int)
        hybrid_parser.add_argument('--dry-run', action='store_true')
        hybrid_parser.add_argument('--skip-validation', action='store_true')
        hybrid_parser.add_argument('--parallel', type=int, default=3)

        parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')

        args = parser.parse_args()

        if not args.pipeline:
            parser.print_help()
            return

        # Configure logging
        import logging
        logging.getLogger().setLevel(getattr(logging, args.log_level))

        logger.info("=" * 80)
        logger.info("🚀 PULSO-BACK ETL PIPELINE RUNNER")
        logger.info("=" * 80)
        logger.info(f"Mode: {args.pipeline}")
        logger.info(f"Hybrid available: {HYBRID_AVAILABLE}")
        logger.info(f"Start time: {start_time}")

        # Initialize
        await etl_dependencies.init_resources()
        logger.info("✅ Dependencies initialized")

        # Execute pipeline
        if args.pipeline == 'catchup-calendar':
            success = await run_calendar_backfill(args)
        elif args.pipeline == 'catchup-incremental':
            success = await run_incremental_refresh(args)
        elif args.pipeline == 'catchup-hybrid':
            success = await run_hybrid_auto(args)
        else:
            logger.error(f"❌ Unknown pipeline: {args.pipeline}")
            success = False

        duration = (datetime.now() - start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"

        logger.info("=" * 80)
        logger.info(f"🏁 PIPELINE EXECUTION {status}")
        logger.info(f"Total duration: {duration:.2f}s")
        logger.info("=" * 80)

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