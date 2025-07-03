"""
🎯 Simple Incremental Pipeline - Core ETL Logic

Pipeline ETL incremental puro separado del entry point.
Contiene toda la lógica de negocio para extracción incremental.

Características:
- Extract incremental basado en watermarks
- Load con UPSERT a PostgreSQL  
- Update de watermarks atómico
- Sin lógicas de negocio complejas

Autor: Ricky para Pulso-Back
"""

from datetime import datetime, timezone, timedelta
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


class SimpleIncrementalPipeline:
    """
    Pipeline ETL incremental puro
    
    Responsabilidades:
    - Extraer datos incrementales desde BigQuery
    - Cargar datos a PostgreSQL con UPSERT
    - Actualizar watermarks después de éxito
    """
    
    def __init__(self):
        self.extractor = BigQueryExtractor()
        self.loader = None
        self.logger = logging.getLogger(__name__)
        self._initialized = False
    
    async def initialize(self) -> None:
        """
        Inicializar componentes del pipeline
        """
        if self._initialized:
            return
            
        self.logger.info("🔧 Initializing pipeline components...")
        
        # Asegurar tabla de watermarks
        await ensure_watermark_table()
        
        # Inicializar loader con database manager
        db_manager = await get_database_manager()
        self.loader = PostgresLoader(db_manager)
        
        self._initialized = True
        self.logger.info("✅ Pipeline initialized successfully")
    
    async def extract_incremental_data(self, table_name: str) -> Dict[str, Any]:
        """
        Extraer datos incrementales para una tabla
        
        Args:
            table_name: Nombre de la tabla a procesar
            
        Returns:
            Dict con datos extraídos y metadatos
        """
        if not self._initialized:
            await self.initialize()
            
        config = ETLConfig.get_config(table_name)
        
        # Obtener último watermark
        last_extracted = await get_last_extracted_date(table_name)
        
        # Determinar rango de extracción
        if last_extracted:
            # Incremental: desde watermark hasta ahora
            start_date = last_extracted
            end_date = datetime.now(timezone.utc)
            extraction_type = "incremental"
            self.logger.info(f"📅 {table_name}: incremental desde {start_date}")
        else:
            # Primera extracción: últimos 30 días por defecto
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=30)
            extraction_type = "initial"
            self.logger.info(f"🆕 {table_name}: primera extracción (últimos 30 días)")
        
        # Construir query incremental
        source_table = f"{ETLConfig.PROJECT_ID}.{ETLConfig.BQ_DATASET}.{config.source_table}"
        
        if config.incremental_column:
            # Tabla con columna de fecha para filtrado incremental
            query = f"""
            SELECT * FROM `{source_table}`
            WHERE {config.incremental_column} > TIMESTAMP('{start_date.isoformat()}')
              AND {config.incremental_column} <= TIMESTAMP('{end_date.isoformat()}')
            ORDER BY {config.incremental_column}
            """
        else:
            # Tabla sin fecha (dimensiones) - extracción completa
            query = f"SELECT * FROM `{source_table}`"
            extraction_type = "full"
        
        # Extraer datos
        self.logger.info(f"🔍 Extracting {table_name}...")
        
        records = []
        record_count = 0
        
        try:
            async for batch in self.extractor.stream_custom_query(query, batch_size=config.batch_size):
                records.extend(batch)
                record_count += len(batch)
                
                # Log progreso cada 50k registros
                if record_count % 50000 == 0:
                    self.logger.info(f"⏳ {table_name}: extracted {record_count:,} records...")
            
            self.logger.info(f"✅ {table_name}: extracted {record_count:,} records total")
            
            return {
                "table_name": table_name,
                "records": records,
                "record_count": record_count,
                "start_date": start_date,
                "end_date": end_date,
                "extraction_type": extraction_type,
                "status": "success"
            }
            
        except Exception as e:
            self.logger.error(f"❌ {table_name}: extraction failed - {e}")
            return {
                "table_name": table_name,
                "records": [],
                "record_count": 0,
                "status": "failed",
                "error": str(e)
            }
    
    async def load_data(self, extraction_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cargar datos extraídos a PostgreSQL
        
        Args:
            extraction_result: Resultado de extract_incremental_data()
            
        Returns:
            Dict con resultado de la carga
        """
        table_name = extraction_result["table_name"]
        records = extraction_result["records"]
        
        if extraction_result["status"] != "success":
            return {
                "table_name": table_name,
                "status": "skipped",
                "reason": "extraction_failed",
                "records_loaded": 0
            }
        
        if not records:
            self.logger.info(f"ℹ️ {table_name}: no new data to load")
            return {
                "table_name": table_name,
                "status": "no_data",
                "records_loaded": 0
            }
        
        try:
            config = ETLConfig.get_config(table_name)
            target_table = ETLConfig.get_fq_table_name(table_name)
            
            # Cargar datos usando UPSERT
            load_result = await self.loader.upsert_records(
                table_name=target_table,
                records=records,
                primary_key=config.primary_key,
                batch_size=config.batch_size
            )
            
            records_loaded = load_result.get("records_loaded", 0)
            self.logger.info(f"✅ {table_name}: loaded {records_loaded:,} records")
            
            return {
                "table_name": table_name,
                "status": "success",
                "records_loaded": records_loaded
            }
            
        except Exception as e:
            self.logger.error(f"❌ {table_name}: load failed - {e}")
            return {
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "records_loaded": 0
            }
    
    async def update_watermark_after_success(
        self, 
        extraction_result: Dict[str, Any], 
        load_result: Dict[str, Any]
    ) -> bool:
        """
        Actualizar watermark solo si extraction y load fueron exitosos
        
        Args:
            extraction_result: Resultado de extracción
            load_result: Resultado de carga
            
        Returns:
            True si watermark fue actualizado
        """
        table_name = extraction_result["table_name"]
        
        # Solo actualizar si ambos procesos fueron exitosos
        if (extraction_result["status"] == "success" and 
            load_result["status"] == "success" and
            load_result["records_loaded"] > 0):
            
            try:
                await update_watermark(table_name, extraction_result["end_date"])
                self.logger.debug(f"✅ {table_name}: watermark updated")
                return True
                
            except Exception as e:
                self.logger.error(f"❌ {table_name}: watermark update failed - {e}")
                return False
        else:
            self.logger.debug(f"⏭️ {table_name}: watermark not updated (no successful load)")
            return False
    
    async def process_table(self, table_name: str) -> Dict[str, Any]:
        """
        Procesar una tabla completa: extract -> load -> update watermark
        
        Args:
            table_name: Nombre de la tabla a procesar
            
        Returns:
            Dict con resultado completo del procesamiento
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"🚀 Processing table: {table_name}")
            
            # 1. Extract
            extraction_result = await self.extract_incremental_data(table_name)
            
            # 2. Load
            load_result = await self.load_data(extraction_result)
            
            # 3. Update watermark (solo si todo fue exitoso)
            watermark_updated = await self.update_watermark_after_success(
                extraction_result, load_result
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Determinar status general
            if (extraction_result["status"] == "success" and 
                load_result["status"] in ["success", "no_data"]):
                overall_status = "success"
            else:
                overall_status = "failed"
            
            result = {
                "table_name": table_name,
                "status": overall_status,
                "records_extracted": extraction_result["record_count"],
                "records_loaded": load_result.get("records_loaded", 0),
                "watermark_updated": watermark_updated,
                "extraction_type": extraction_result.get("extraction_type", "unknown"),
                "duration_seconds": duration
            }
            
            # Agregar errores si los hay
            if extraction_result.get("error"):
                result["extraction_error"] = extraction_result["error"]
            if load_result.get("error"):
                result["load_error"] = load_result["error"]
            
            status_emoji = "✅" if overall_status == "success" else "❌"
            self.logger.info(
                f"{status_emoji} {table_name}: {result['records_loaded']:,} records "
                f"in {duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ {table_name}: processing failed - {e}")
            
            return {
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "duration_seconds": duration,
                "records_extracted": 0,
                "records_loaded": 0,
                "watermark_updated": False
            }
    
    async def process_tables(self, table_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Procesar múltiples tablas
        
        Args:
            table_names: Lista de tablas a procesar, None para todas las configuradas
            
        Returns:
            Dict con resultados de todas las tablas
        """
        if not self._initialized:
            await self.initialize()
            
        # Usar todas las tablas configuradas si no se especifica
        tables_to_process = table_names or ETLConfig.get_raw_source_tables()
        
        total_start = datetime.now()
        
        self.logger.info("="*80)
        self.logger.info("🚀 SIMPLE INCREMENTAL PIPELINE")
        self.logger.info("="*80)
        self.logger.info(f"📊 Tables to process: {len(tables_to_process)}")
        self.logger.info(f"📋 Tables: {', '.join(tables_to_process)}")
        
        results = []
        successful_tables = 0
        total_extracted = 0
        total_loaded = 0
        
        # Procesar cada tabla
        for i, table_name in enumerate(tables_to_process, 1):
            self.logger.info(f"\n📋 [{i}/{len(tables_to_process)}] Processing: {table_name}")
            
            result = await self.process_table(table_name)
            results.append(result)
            
            if result["status"] == "success":
                successful_tables += 1
                total_extracted += result["records_extracted"]
                total_loaded += result["records_loaded"]
        
        total_duration = (datetime.now() - total_start).total_seconds()
        
        # Resumen final
        self.logger.info("\n" + "="*80)
        self.logger.info("📊 PIPELINE EXECUTION RESULTS")
        self.logger.info("="*80)
        self.logger.info(f"✅ Successful tables: {successful_tables}/{len(tables_to_process)}")
        self.logger.info(f"❌ Failed tables: {len(tables_to_process) - successful_tables}")
        self.logger.info(f"📊 Total extracted: {total_extracted:,}")
        self.logger.info(f"📊 Total loaded: {total_loaded:,}")
        self.logger.info(f"⏱️ Total duration: {total_duration:.2f}s")
        
        # Mostrar tablas fallidas si las hay
        failed_tables = [r for r in results if r["status"] == "failed"]
        if failed_tables:
            self.logger.error("❌ Failed tables:")
            for result in failed_tables:
                error_msg = result.get("error", "Unknown error")
                self.logger.error(f"  - {result['table_name']}: {error_msg}")
        
        return {
            "status": "success" if successful_tables == len(tables_to_process) else "partial",
            "successful_tables": successful_tables,
            "total_tables": len(tables_to_process),
            "total_extracted": total_extracted,
            "total_loaded": total_loaded,
            "duration_seconds": total_duration,
            "table_results": results
        }
    
    async def cleanup(self) -> None:
        """
        Limpiar recursos del pipeline
        """
        if self.loader and hasattr(self.loader, 'cleanup'):
            await self.loader.cleanup()
        
        self.logger.info("🧹 Pipeline resources cleaned up")
