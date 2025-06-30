# etl/pipelines/raw_data_pipeline_creado_el_single.py

"""
🎯 MÉTODO: extract_table_by_creado_el_range para HybridRawDataPipeline

Este archivo contiene el método público para extraer UNA tabla por rango de creado_el.

INSTRUCCIONES DE INTEGRACIÓN:
1. Copiar el método extract_table_by_creado_el_range
2. Agregarlo a la clase HybridRawDataPipeline al final de la clase (después de run_incremental_refresh)
"""

import uuid
from datetime import date, datetime
from etl.models import TableLoadResult

# MÉTODO A AGREGAR A HybridRawDataPipeline:
async def extract_table_by_creado_el_range(
        self,
        table_name: str,
        start_date: date,
        end_date: date,
        include_timestamps: bool = True,
        update_watermark: bool = False
) -> TableLoadResult:
    """
    🎯 🆕 Extrae una tabla específica filtrando por rango de creado_el.
    
    CASOS DE USO:
    - Extracciones de recuperación por fechas específicas
    - Análisis ad-hoc por rangos temporales  
    - Cargas de mantenimiento independientes de campañas
    - Reprocessing de datos corruptos en un período específico
    
    Args:
        table_name: Nombre de la tabla a extraer (asignaciones, trandeuda, pagos)
        start_date: Fecha de inicio del rango (inclusive)
        end_date: Fecha de fin del rango (inclusive)  
        include_timestamps: Si usar timestamps completos o solo fechas
        update_watermark: Si actualizar watermark (normalmente False para ad-hoc)
        
    Returns:
        TableLoadResult con estadísticas de la extracción
        
    Raises:
        ValueError: Si la tabla no tiene creado_el como incremental_column
        
    EJEMPLOS DE USO:
        # Extraer asignaciones de diciembre 2024
        result = await pipeline.extract_table_by_creado_el_range(
            "asignaciones", 
            date(2024, 12, 1), 
            date(2024, 12, 31)
        )
        
        # Extraer pagos de una semana específica con solo fechas
        result = await pipeline.extract_table_by_creado_el_range(
            "pagos",
            date(2024, 12, 15), 
            date(2024, 12, 21),
            include_timestamps=False
        )
    """
    extraction_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    self.logger.info(
        f"🎯 Starting creado_el range extraction for {table_name} "
        f"({start_date} to {end_date}, ID: {extraction_id})"
    )
    
    try:
        # Obtener configuración de la tabla
        config = ETLConfig.get_config(table_name)
        
        # Construir query con filtro por creado_el
        query = await self._build_creado_el_filter_query(
            table_name, start_date, end_date, include_timestamps
        )
        
        # Opcional: marcar inicio en watermark
        if update_watermark:
            watermark_manager = await self._get_watermark_manager()
            await watermark_manager.start_extraction(table_name, extraction_id)
        
        # Stream de datos: Extract → Transform → Load
        raw_stream = self.extractor.stream_custom_query(query, config.batch_size)
        transformed_stream = self._transform_stream_with_backpressure(table_name, raw_stream)
        
        # Cargar datos con upsert
        load_result = await self.loader.load_data_streaming(
            table_name=config.table_name,
            table_type=config.table_type,
            data_stream=transformed_stream,
            primary_key=config.primary_key,
            upsert=True
        )
        
        if load_result.status not in ["success", "partial_success"]:
            raise Exception(f"Load failed: {load_result.error_message}")
        
        # Calcular estadísticas
        duration = (datetime.now() - start_time).total_seconds()
        records_loaded = getattr(load_result, 'inserted_records', 0) + getattr(load_result, 'updated_records', 0)
        
        # Opcional: actualizar watermark
        if update_watermark:
            watermark_manager = await self._get_watermark_manager()
            watermark_timestamp = datetime.combine(end_date, datetime.max.time())
            await watermark_manager.update_watermark(
                table_name=table_name,
                timestamp=watermark_timestamp,
                records_extracted=records_loaded,
                extraction_duration_seconds=duration,
                status="success",
                extraction_id=extraction_id,
                metadata={
                    "strategy": "creado_el_filter",
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                    "include_timestamps": include_timestamps
                }
            )
        
        self.logger.info(
            f"✅ Creado_el extraction completed for {table_name}: {records_loaded:,} records, "
            f"duration: {duration:.2f}s"
        )
        
        return TableLoadResult(
            table_name=table_name,
            records_processed=getattr(load_result, 'processed_records', 0) or records_loaded,
            records_loaded=records_loaded,
            duration_seconds=duration,
            status="success",
            error_message=None
        )
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"Creado_el extraction failed for {table_name}: {str(e)}"
        self.logger.error(error_msg)
        
        # Actualizar watermark con error si se requería
        if update_watermark:
            try:
                watermark_manager = await self._get_watermark_manager()
                await watermark_manager.update_watermark(
                    table_name=table_name,
                    timestamp=datetime.now(),
                    status="failed",
                    error_message=error_msg,
                    extraction_id=extraction_id,
                    extraction_duration_seconds=duration
                )
            except Exception as wm_error:
                self.logger.error(f"Failed to update watermark: {wm_error}")
        
        return TableLoadResult(
            table_name=table_name,
            records_processed=0,
            records_loaded=0,
            duration_seconds=duration,
            status="failed",
            error_message=error_msg
        )

# POSICIÓN EN raw_data_pipeline.py:
"""
Agregar al final de la clase HybridRawDataPipeline, 
después del método run_incremental_refresh (línea ~735)
"""

# DEPENDENCIAS NECESARIAS:
"""
- ETLConfig (ya importado)
- TableLoadResult (ya importado) 
- uuid (ya importado)
- datetime (ya importado)
- Método _build_creado_el_filter_query (del archivo anterior)
"""