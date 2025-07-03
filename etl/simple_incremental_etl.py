"""
🎯 ETL Incremental Simplificado - Solo Datos Nuevos

Extrae únicamente datos incrementales usando watermarks simples:
- WHERE fecha > última_fecha_extraída
- Sin lógicas de negocio complejas
- Sin campañas ni transformaciones complejas
- Actualización atómica de watermarks

Autor: Ricky para Pulso-Back
"""

import asyncio
import argparse
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Dict, Any, List
import logging

from etl.extractors.bigquery_extractor import BigQueryExtractor
from etl.loaders.postgres_loader import PostgresLoader
from etl.config import ETLConfig
from etl.watermarks import (
    ensure_watermark_table,
    get_last_extracted_date,
    update_watermark
)
from shared.database.connection import get_database_manager


class SimpleIncrementalETL:
    """ETL Incremental Puro - Solo datos nuevos"""
    
    def __init__(self):
        self.extractor = BigQueryExtractor()
        self.logger = logging.getLogger(__name__)
        self.loader = None
    
    async def init(self):
        """Inicializar componentes"""
        await ensure_watermark_table()
        db_manager = await get_database_manager()
        self.loader = PostgresLoader(db_manager)
    
    async def extract_incremental_data(self, table_name: str) -> Dict[str, Any]:
        """
        Extraer solo datos nuevos desde último watermark
        """
        config = ETLConfig.get_config(table_name)
        
        # Obtener último watermark
        last_extracted = await get_last_extracted_date(table_name)
        
        # Determinar rango de extracción
        if last_extracted:
            # Incremental: desde watermark hasta ahora
            start_date = last_extracted
            end_date = datetime.now(timezone.utc)
            self.logger.info(f"📅 {table_name}: incremental desde {start_date}")
        else:
            # Primera extracción: últimos 30 días
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=30)
            self.logger.info(f"🆕 {table_name}: primera extracción (últimos 30 días)")
        
        # Construir query incremental
        source_table = f"{ETLConfig.PROJECT_ID}.{ETLConfig.BQ_DATASET}.{config.source_table}"
        
        if config.incremental_column:
            # Tabla con columna de fecha
            query = f"""
            SELECT * FROM `{source_table}`
            WHERE {config.incremental_column} > TIMESTAMP('{start_date.isoformat()}')
              AND {config.incremental_column} <= TIMESTAMP('{end_date.isoformat()}')
            ORDER BY {config.incremental_column}
            """
        else:
            # Tabla sin fecha (dimensiones) - extracción completa
            query = f"SELECT * FROM `{source_table}`"
        
        self.logger.info(f"🔍 Extracting {table_name}...")
        
        # Extraer datos
        records = []
        record_count = 0
        
        async for batch in self.extractor.stream_custom_query(query, batch_size=10000):
            records.extend(batch)
            record_count += len(batch)
            
            if record_count % 50000 == 0:
                self.logger.info(f"⏳ Extracted {record_count:,} records...")
        
        self.logger.info(f"✅ Extracted {record_count:,} records from {table_name}")
        
        return {
            "table_name": table_name,
            "records": records,
            "record_count": record_count,
            "start_date": start_date,
            "end_date": end_date,
            "is_incremental": last_extracted is not None
        }
    
    async def load_incremental_data(self, extraction_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cargar datos extraídos a PostgreSQL
        """
        table_name = extraction_result["table_name"]
        records = extraction_result["records"]
        
        if not records:
            self.logger.info(f"ℹ️ {table_name}: no new data to load")
            return {"records_loaded": 0, "status": "no_data"}
        
        config = ETLConfig.get_config(table_name)
        target_table = ETLConfig.get_fq_table_name(table_name)
        
        # Cargar datos usando UPSERT
        load_result = await self.loader.upsert_records(
            table_name=target_table,
            records=records,
            primary_key=config.primary_key,
            batch_size=config.batch_size
        )
        
        self.logger.info(f"✅ Loaded {load_result['records_loaded']:,} records to {table_name}")
        
        return load_result
    
    async def sync_table_incremental(self, table_name: str) -> Dict[str, Any]:
        """
        Sincronizar una tabla de forma incremental
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"🚀 Starting incremental sync: {table_name}")
            
            # 1. Extraer datos incrementales
            extraction_result = await self.extract_incremental_data(table_name)
            
            if extraction_result["record_count"] == 0:
                return {
                    "table_name": table_name,
                    "status": "no_data",
                    "records_extracted": 0,
                    "records_loaded": 0,
                    "duration_seconds": (datetime.now() - start_time).total_seconds()
                }
            
            # 2. Cargar datos
            load_result = await self.load_incremental_data(extraction_result)
            
            # 3. Actualizar watermark SOLO si la carga fue exitosa
            if load_result.get("records_loaded", 0) > 0:
                await update_watermark(
                    table_name, 
                    extraction_result["end_date"]
                )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return {
                "table_name": table_name,
                "status": "success",
                "records_extracted": extraction_result["record_count"],
                "records_loaded": load_result.get("records_loaded", 0),
                "duration_seconds": duration,
                "watermark_updated": True,
                "is_incremental": extraction_result["is_incremental"]
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ {table_name} failed: {e}")
            
            return {
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "duration_seconds": duration
            }
    
    async def sync_all_incremental(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Sincronizar todas las tablas incrementalmente
        """
        tables_to_sync = tables or ETLConfig.get_raw_source_tables()
        total_start = datetime.now()
        
        self.logger.info("="*80)
        self.logger.info("🚀 INCREMENTAL ETL SYNC")
        self.logger.info("="*80)
        self.logger.info(f"📊 Tables: {len(tables_to_sync)}")
        
        results = []
        successful_tables = 0
        total_extracted = 0
        total_loaded = 0
        
        for i, table_name in enumerate(tables_to_sync, 1):
            self.logger.info(f"\n📋 [{i}/{len(tables_to_sync)}] Processing: {table_name}")
            
            result = await self.sync_table_incremental(table_name)
            results.append(result)
            
            if result["status"] == "success":
                successful_tables += 1
                total_extracted += result["records_extracted"]
                total_loaded += result["records_loaded"]
            elif result["status"] == "no_data":
                successful_tables += 1  # No data is also success
        
        total_duration = (datetime.now() - total_start).total_seconds()
        
        # Resumen final
        self.logger.info("\n" + "="*80)
        self.logger.info("📊 INCREMENTAL SYNC RESULTS")
        self.logger.info("="*80)
        self.logger.info(f"✅ Successful: {successful_tables}/{len(tables_to_sync)}")
        self.logger.info(f"📊 Total extracted: {total_extracted:,}")
        self.logger.info(f"📊 Total loaded: {total_loaded:,}")
        self.logger.info(f"⏱️ Duration: {total_duration:.2f}s")
        
        return {
            "status": "success" if successful_tables == len(tables_to_sync) else "partial",
            "successful_tables": successful_tables,
            "total_tables": len(tables_to_sync),
            "total_extracted": total_extracted,
            "total_loaded": total_loaded,
            "duration_seconds": total_duration,
            "results": results
        }


async def main():
    """CLI Interface"""
    parser = argparse.ArgumentParser(description="Simple Incremental ETL")
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        etl = SimpleIncrementalETL()
        await etl.init()
        
        result = await etl.sync_all_incremental(args.tables)
        
        exit_code = 0 if result["status"] == "success" else 1
        exit(exit_code)
        
    except Exception as e:
        logging.error(f"❌ ETL failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
