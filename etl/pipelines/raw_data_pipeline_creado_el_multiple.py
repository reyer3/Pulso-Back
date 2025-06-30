# etl/pipelines/raw_data_pipeline_creado_el_multiple.py

"""
🎯 MÉTODO: extract_multiple_tables_by_creado_el_range para HybridRawDataPipeline

Este archivo contiene el método público para extraer MÚLTIPLES tablas por rango de creado_el.

INSTRUCCIONES DE INTEGRACIÓN:
1. Copiar el método extract_multiple_tables_by_creado_el_range
2. Agregarlo a la clase HybridRawDataPipeline después del método extract_table_by_creado_el_range
"""

import asyncio
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from etl.config import ETLConfig
from etl.models import TableLoadResult

# MÉTODO A AGREGAR A HybridRawDataPipeline:
async def extract_multiple_tables_by_creado_el_range(
        self,
        table_names: Optional[List[str]],
        start_date: date,
        end_date: date,
        include_timestamps: bool = True,
        update_watermarks: bool = False,
        max_parallel: int = 3
) -> Dict[str, Any]:
    """
    🎯 🆕 Extrae múltiples tablas filtrando por rango de creado_el.
    
    CASOS DE USO:
    - Recuperación masiva por fechas específicas
    - Reprocessing de datos por rango temporal
    - Cargas de reparación independientes de campañas
    - Análisis temporal de múltiples fuentes
    
    Args:
        table_names: Lista de tablas a extraer (None = todas las tablas con creado_el)
        start_date: Fecha de inicio del rango (inclusive)
        end_date: Fecha de fin del rango (inclusive)
        include_timestamps: Si usar timestamps completos o solo fechas
        update_watermarks: Si actualizar watermarks
        max_parallel: Máximo de tablas en paralelo
        
    Returns:
        Dict con resumen de extracciones y resultados por tabla
        
    EJEMPLOS DE USO:
        # Extraer todas las tablas con creado_el para diciembre 2024
        result = await pipeline.extract_multiple_tables_by_creado_el_range(
            None,  # Todas las tablas
            date(2024, 12, 1), 
            date(2024, 12, 31)
        )
        
        # Extraer tablas específicas para una semana
        result = await pipeline.extract_multiple_tables_by_creado_el_range(
            ["asignaciones", "trandeuda"],
            date(2024, 12, 15), 
            date(2024, 12, 21),
            max_parallel=2
        )
    """
    start_time = datetime.now()
    
    # Determinar tablas a procesar
    if table_names is None:
        # Auto-detectar tablas que tienen creado_el como incremental_column
        all_tables = ETLConfig.get_raw_source_tables()
        target_tables = []
        for table in all_tables:
            try:
                config = ETLConfig.get_config(table)
                if config.incremental_column == "creado_el":
                    target_tables.append(table)
            except Exception as e:
                self.logger.warning(f"Could not check {table}: {e}")
                continue
    else:
        target_tables = table_names
        # Validar que todas las tablas tengan creado_el
        invalid_tables = []
        for table in target_tables:
            try:
                config = ETLConfig.get_config(table)
                if config.incremental_column != "creado_el":
                    invalid_tables.append(f"{table} (uses {config.incremental_column})")
            except Exception as e:
                invalid_tables.append(f"{table} (error: {e})")
        
        if invalid_tables:
            raise ValueError(
                f"Tables without 'creado_el' as incremental column: {invalid_tables}. "
                f"Valid tables: asignaciones, trandeuda, pagos"
            )
    
    if not target_tables:
        self.logger.warning("No tables found with 'creado_el' as incremental column")
        return {
            "status": "success",
            "mode": "creado_el_range_extraction", 
            "tables_processed": 0,
            "message": "No valid tables found"
        }
    
    self.logger.info(
        f"🎯 Starting creado_el range extraction for {len(target_tables)} tables "
        f"({start_date} to {end_date}): {target_tables}"
    )
    
    # Procesar tablas en paralelo con semáforo
    semaphore = asyncio.Semaphore(max_parallel)
    
    async def process_table(table_name: str):
        async with semaphore:
            try:
                return await self.extract_table_by_creado_el_range(
                    table_name=table_name,
                    start_date=start_date,
                    end_date=end_date,
                    include_timestamps=include_timestamps,
                    update_watermark=update_watermarks
                )
            except Exception as e:
                self.logger.error(f"Error processing {table_name}: {e}")
                return TableLoadResult(
                    table_name=table_name,
                    records_processed=0,
                    records_loaded=0,
                    duration_seconds=0,
                    status="failed",
                    error_message=str(e)
                )
    
    # Ejecutar extracciones en paralelo
    tasks = [process_table(table_name) for table_name in target_tables]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Procesar resultados
    successful_tables = []
    failed_tables = []
    total_records = 0
    table_results = {}
    
    for i, result in enumerate(results):
        table_name = target_tables[i]
        
        if isinstance(result, Exception):
            failed_tables.append(table_name)
            table_results[table_name] = {
                "records_loaded": 0,
                "duration_seconds": 0,
                "status": "failed",
                "error_message": str(result)
            }
        elif isinstance(result, TableLoadResult):
            table_results[table_name] = {
                "records_loaded": result.records_loaded,
                "duration_seconds": result.duration_seconds,
                "status": result.status,
                "error_message": result.error_message
            }
            if result.status == "success":
                successful_tables.append(table_name)
                total_records += result.records_loaded
            else:
                failed_tables.append(table_name)
        else:
            # Resultado inesperado
            failed_tables.append(table_name)
            table_results[table_name] = {
                "records_loaded": 0,
                "duration_seconds": 0,
                "status": "failed", 
                "error_message": f"Unexpected result type: {type(result)}"
            }
    
    # Calcular estadísticas finales
    duration = (datetime.now() - start_time).total_seconds()
    final_status = "success" if not failed_tables else ("partial" if successful_tables else "failed")
    
    summary = {
        "status": final_status,
        "mode": "creado_el_range_extraction",
        "date_range": {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "include_timestamps": include_timestamps
        },
        "duration_seconds": duration,
        "total_records_loaded": total_records,
        "successful_tables": successful_tables,
        "failed_tables": failed_tables,
        "tables_processed": len(target_tables),
        "watermarks_updated": update_watermarks,
        "max_parallel": max_parallel,
        "table_results": table_results
    }
    
    self.logger.info(
        f"🏁 Creado_el range extraction completed: {len(successful_tables)}/{len(target_tables)} tables, "
        f"{total_records:,} records in {duration:.2f}s"
    )
    
    # Log detalles si hay fallos
    if failed_tables:
        self.logger.warning(f"❌ Failed tables: {failed_tables}")
        for table in failed_tables:
            error = table_results[table]["error_message"]
            self.logger.warning(f"  • {table}: {error}")
    
    return summary

# MÉTODO AUXILIAR para listar tablas válidas:
def get_tables_with_creado_el(self) -> List[str]:
    """
    🔍 🆕 Obtiene lista de tablas que usan creado_el como incremental_column.
    
    Returns:
        Lista de nombres de tablas que pueden usar filtrado por creado_el
        
    EJEMPLO DE USO:
        valid_tables = pipeline.get_tables_with_creado_el()
        print(f"Tablas válidas para creado_el: {valid_tables}")
    """
    tables_with_creado_el = []
    all_tables = ETLConfig.get_raw_source_tables()
    
    for table in all_tables:
        try:
            config = ETLConfig.get_config(table)
            if config.incremental_column == "creado_el":
                tables_with_creado_el.append(table)
        except Exception as e:
            self.logger.debug(f"Could not check table {table}: {e}")
            continue
    
    return tables_with_creado_el

# POSICIÓN EN raw_data_pipeline.py:
"""
Agregar después del método extract_table_by_creado_el_range
"""

# DEPENDENCIAS NECESARIAS:
"""
- asyncio (ya importado)
- ETLConfig (ya importado) 
- TableLoadResult (ya importado)
- typing imports (ya importado)
- Método extract_table_by_creado_el_range (del archivo anterior)
"""