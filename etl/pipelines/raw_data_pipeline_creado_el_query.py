# etl/pipelines/raw_data_pipeline_creado_el_query.py

"""
🎯 MÉTODO: _build_creado_el_filter_query para HybridRawDataPipeline

Este archivo contiene el método para construir queries con filtro directo por creado_el.

INSTRUCCIONES DE INTEGRACIÓN:
1. Copiar el método _build_creado_el_filter_query
2. Agregarlo a la clase HybridRawDataPipeline después del método _build_watermark_driven_query
"""

from datetime import date, datetime
from etl.config import ETLConfig

# MÉTODO A AGREGAR A HybridRawDataPipeline:
async def _build_creado_el_filter_query(
        self,
        table_name: str,
        start_date: date,
        end_date: date,
        include_timestamps: bool = True
) -> str:
    """
    🗓️ 🆕 Construye query con filtro directo por creado_el.
    
    CASOS DE USO:
    - Extracciones de recuperación por fechas específicas
    - Análisis ad-hoc por rangos temporales
    - Cargas independientes de campañas y watermarks
    
    Args:
        table_name: Nombre de la tabla a filtrar
        start_date: Fecha de inicio (inclusive)
        end_date: Fecha de fin (inclusive)
        include_timestamps: Si incluir timestamp completo o solo fechas
        
    Returns:
        Query SQL con filtro por creado_el
        
    Raises:
        ValueError: Si la tabla no tiene creado_el como incremental_column
        
    EJEMPLO DE USO:
        query = await pipeline._build_creado_el_filter_query(
            "asignaciones", 
            date(2024, 12, 1), 
            date(2024, 12, 31)
        )
    """
    # Verificar que la tabla tenga creado_el como incremental column
    config = ETLConfig.get_config(table_name)
    
    if config.incremental_column != "creado_el":
        raise ValueError(
            f"Table '{table_name}' does not use 'creado_el' as incremental column. "
            f"Current incremental column: {config.incremental_column}. "
            f"Valid tables with creado_el: asignaciones, trandeuda, pagos"
        )
    
    # Obtener template SQL
    template = self._get_query_template(table_name)
    
    # Construir filtro directo por creado_el
    if include_timestamps:
        # Incluir horas para mayor precisión
        start_datetime = f"{start_date} 00:00:00"
        end_datetime = f"{end_date} 23:59:59"
        incremental_filter = f"creado_el BETWEEN TIMESTAMP('{start_datetime}') AND TIMESTAMP('{end_datetime}')"
    else:
        # Solo fechas (más compatible con diferentes tipos de datos)
        incremental_filter = f"DATE(creado_el) BETWEEN '{start_date}' AND '{end_date}'"
    
    self.logger.info(
        f"🗓️ Building creado_el filter query for {table_name}: {start_date} to {end_date} "
        f"(timestamps: {include_timestamps})"
    )
    
    # Formatear query con parámetros
    formatted_query = template.format(
        project_id=ETLConfig.PROJECT_ID,
        dataset_id=ETLConfig.BQ_DATASET,
        incremental_filter=incremental_filter,
        campaign_archivo="CREADO_EL_FILTER"  # Placeholder para identificar este tipo de query
    )
    
    return formatted_query

# POSICIÓN EN raw_data_pipeline.py:
"""
Agregar después del método _build_watermark_driven_query (línea ~206)
y antes del método _build_hybrid_query (línea ~246)
"""

# TABLAS VÁLIDAS PARA ESTE MÉTODO:
"""
Tablas que tienen creado_el como incremental_column:
- asignaciones
- trandeuda  
- pagos

Para verificar, usar:
config = ETLConfig.get_config(table_name)
if config.incremental_column == "creado_el":
    # Tabla válida para filtrado por creado_el
"""