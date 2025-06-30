# etl/pipelines/raw_data_pipeline_creado_el_enum.py

"""
🎯 EXTENSIÓN DE ENUM: Nueva estrategia de extracción por creado_el

Este archivo define la extensión del enum ExtractionStrategy para incluir
filtrado directo por creado_el.

AGREGAR A raw_data_pipeline.py:
- En ExtractionStrategy enum, agregar: CREADO_EL_FILTER = "creado_el_filter"
"""

from enum import Enum

class ExtractionStrategyExtended(str, Enum):
    """
    Estrategias de extracción disponibles - VERSIÓN EXTENDIDA
    
    INSTRUCCIONES:
    1. Reemplazar ExtractionStrategy en raw_data_pipeline.py con esta versión
    2. O agregar solo la línea CREADO_EL_FILTER al enum existente
    """
    CALENDAR_DRIVEN = "calendar_driven"  # Guiado por fechas de campaña
    WATERMARK_DRIVEN = "watermark_driven"  # Guiado por watermarks
    HYBRID_AUTO = "hybrid_auto"  # Decisión automática inteligente
    CREADO_EL_FILTER = "creado_el_filter"  # 🆕 Filtrado directo por creado_el

# CÓDIGO A AGREGAR DIRECTAMENTE:
"""
En línea 35 de raw_data_pipeline.py, agregar después de HYBRID_AUTO:

    CREADO_EL_FILTER = "creado_el_filter"  # 🆕 Filtrado directo por creado_el
"""