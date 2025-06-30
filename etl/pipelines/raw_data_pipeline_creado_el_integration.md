# etl/pipelines/raw_data_pipeline_creado_el_integration.md

# 🎯 GUÍA DE INTEGRACIÓN: Filtrado por creado_el en HybridRawDataPipeline

## 📋 Resumen de Archivos Creados

1. **raw_data_pipeline_creado_el_enum.py** - Nueva estrategia enum
2. **raw_data_pipeline_creado_el_query.py** - Método constructor de queries  
3. **raw_data_pipeline_creado_el_single.py** - Método para una tabla
4. **raw_data_pipeline_creado_el_multiple.py** - Método para múltiples tablas

## 🔧 Pasos de Integración Manual

### PASO 1: Actualizar ExtractionStrategy (línea ~35)
```python
class ExtractionStrategy(str, Enum):
    """Estrategias de extracción disponibles"""
    CALENDAR_DRIVEN = "calendar_driven"  # Guiado por fechas de campaña
    WATERMARK_DRIVEN = "watermark_driven"  # Guiado por watermarks
    HYBRID_AUTO = "hybrid_auto"  # Decisión automática inteligente
    CREADO_EL_FILTER = "creado_el_filter"  # 🆕 Filtrado directo por creado_el
```

### PASO 2: Agregar método _build_creado_el_filter_query (después línea ~206)
Copiar método completo desde `raw_data_pipeline_creado_el_query.py`

### PASO 3: Agregar método extract_table_by_creado_el_range (después línea ~735)  
Copiar método completo desde `raw_data_pipeline_creado_el_single.py`

### PASO 4: Agregar métodos múltiples (después del paso 3)
Copiar métodos desde `raw_data_pipeline_creado_el_multiple.py`:
- `extract_multiple_tables_by_creado_el_range`
- `get_tables_with_creado_el`

## 📖 Ejemplos de Uso Después de Integración

```python
# Obtener pipeline
pipeline = await etl_dependencies.hybrid_raw_pipeline()

# Ver tablas válidas
valid_tables = pipeline.get_tables_with_creado_el()
print(f"Tablas con creado_el: {valid_tables}")  # ['asignaciones', 'trandeuda', 'pagos']

# Extraer una tabla por rango de fechas
result = await pipeline.extract_table_by_creado_el_range(
    "asignaciones", 
    date(2024, 12, 1), 
    date(2024, 12, 31)
)
print(f"Registros cargados: {result.records_loaded}")

# Extraer múltiples tablas
result = await pipeline.extract_multiple_tables_by_creado_el_range(
    ["asignaciones", "trandeuda"],
    date(2024, 12, 15), 
    date(2024, 12, 21)
)
print(f"Resumen: {result['successful_tables']}")
```

## 🔍 Validación de Integración

```python
# Test básico
pipeline = await etl_dependencies.hybrid_raw_pipeline()

# 1. Verificar que el enum tiene la nueva estrategia
assert hasattr(ExtractionStrategy, 'CREADO_EL_FILTER')

# 2. Verificar que los métodos existen
assert hasattr(pipeline, '_build_creado_el_filter_query')
assert hasattr(pipeline, 'extract_table_by_creado_el_range') 
assert hasattr(pipeline, 'extract_multiple_tables_by_creado_el_range')
assert hasattr(pipeline, 'get_tables_with_creado_el')

# 3. Verificar tablas válidas
valid_tables = pipeline.get_tables_with_creado_el()
expected_tables = ['asignaciones', 'trandeuda', 'pagos']
assert all(table in valid_tables for table in expected_tables)

print("✅ Integración validada correctamente")
```

## 🎯 Casos de Uso Principales

### 1. Recuperación de Datos
```python
# Recuperar datos perdidos de una fecha específica
await pipeline.extract_table_by_creado_el_range(
    "pagos", 
    date(2024, 12, 25), 
    date(2024, 12, 25)  # Un solo día
)
```

### 2. Análisis Ad-hoc
```python
# Extraer datos para análisis sin afectar watermarks
await pipeline.extract_multiple_tables_by_creado_el_range(
    None,  # Todas las tablas
    date(2024, 11, 1), 
    date(2024, 11, 30),
    update_watermarks=False
)
```

### 3. Reprocessing Temporal
```python
# Reprocesar una semana específica con todas las tablas
await pipeline.extract_multiple_tables_by_creado_el_range(
    ["asignaciones", "trandeuda", "pagos"],
    date(2024, 12, 10), 
    date(2024, 12, 16),
    include_timestamps=True,
    max_parallel=2
)
```

### 4. Validación de Datos
```python
# Verificar integridad de datos en un período
result = await pipeline.extract_table_by_creado_el_range(
    "asignaciones",
    date(2024, 12, 1), 
    date(2024, 12, 1),
    include_timestamps=False,
    update_watermark=False  # No afectar watermarks
)

if result.status == "success":
    print(f"Integridad OK: {result.records_loaded} registros encontrados")
```

## ⚡ Comandos CLI (Para implementación futura)

```bash
# Extraer tabla específica por rango
python -c "
import asyncio
from datetime import date
from etl.dependencies import etl_dependencies

async def extract_by_date():
    await etl_dependencies.init_resources()
    pipeline = await etl_dependencies.hybrid_raw_pipeline()
    
    result = await pipeline.extract_table_by_creado_el_range(
        'asignaciones', 
        date(2024, 12, 1), 
        date(2024, 12, 31)
    )
    print(f'Loaded: {result.records_loaded} records')
    
    await etl_dependencies.shutdown_resources()

asyncio.run(extract_by_date())
"

# Ver tablas válidas
python -c "
import asyncio
from etl.dependencies import etl_dependencies

async def show_valid_tables():
    await etl_dependencies.init_resources()
    pipeline = await etl_dependencies.hybrid_raw_pipeline()
    
    tables = pipeline.get_tables_with_creado_el()
    print(f'Tablas válidas: {tables}')
    
    await etl_dependencies.shutdown_resources()

asyncio.run(show_valid_tables())
"
```

## 🚨 Validaciones y Errores Comunes

### Error: Tabla sin creado_el
```python
# Esto fallará:
await pipeline.extract_table_by_creado_el_range("ejecutivos", ...)
# Error: Table 'ejecutivos' does not use 'creado_el' as incremental column

# Solución: Verificar primero
valid_tables = pipeline.get_tables_with_creado_el()
if "ejecutivos" in valid_tables:
    # OK para extraer
```

### Error: Fechas inválidas
```python
# Esto fallará:
await pipeline.extract_table_by_creado_el_range(
    "asignaciones",
    date(2024, 12, 31),  # fin antes que inicio
    date(2024, 12, 1)
)

# Solución: Validar fechas
if start_date <= end_date:
    # OK para extraer
```

### Error: Query malformada
```python
# Si el template SQL no tiene {incremental_filter}
# Error: KeyError: 'incremental_filter'

# Verificar que el archivo SQL tenga:
# WHERE creado_el {incremental_filter}
```

## 🔧 Troubleshooting

### 1. Verificar configuración de tabla
```python
from etl.config import ETLConfig
config = ETLConfig.get_config("asignaciones")
print(f"Incremental column: {config.incremental_column}")
# Debe ser: creado_el
```

### 2. Verificar template SQL
```python
pipeline = await etl_dependencies.hybrid_raw_pipeline()
template = pipeline._get_query_template("asignaciones")
print("Placeholders en template:", 
      [p for p in ["incremental_filter", "project_id", "dataset_id", "campaign_archivo"] 
       if f"{{{p}}}" in template])
```

### 3. Test de query building
```python
query = await pipeline._build_creado_el_filter_query(
    "asignaciones", 
    date(2024, 12, 1), 
    date(2024, 12, 31)
)
print("Query generada:")
print(query[:500] + "..." if len(query) > 500 else query)
```

## 📊 Métricas de Performance

```python
import time
from datetime import date

start_time = time.time()

result = await pipeline.extract_multiple_tables_by_creado_el_range(
    None,  # Todas las tablas
    date(2024, 12, 1), 
    date(2024, 12, 7),  # Una semana
    max_parallel=3
)

end_time = time.time()
duration = end_time - start_time

print(f"⏱️ Performance Metrics:")
print(f"   Total duration: {duration:.2f}s")
print(f"   Records loaded: {result['total_records_loaded']:,}")
print(f"   Records/second: {result['total_records_loaded']/duration:.0f}")
print(f"   Successful tables: {len(result['successful_tables'])}")
print(f"   Parallel workers: {result['max_parallel']}")
```

## ✅ Checklist Final

- [ ] ExtractionStrategy.CREADO_EL_FILTER agregado
- [ ] Método _build_creado_el_filter_query integrado
- [ ] Método extract_table_by_creado_el_range integrado
- [ ] Método extract_multiple_tables_by_creado_el_range integrado
- [ ] Método get_tables_with_creado_el integrado
- [ ] Validación básica ejecutada
- [ ] Test con una tabla ejecutado
- [ ] Test con múltiples tablas ejecutado
- [ ] Documentación de casos de uso revisada

## 🎯 Próximos Pasos Opcionales

1. **CLI Command**: Crear comando específico en `run_pipeline.py`
2. **Scheduling**: Integrar con sistema de cron/scheduler
3. **Monitoring**: Añadir métricas y alertas específicas
4. **Documentation**: Expandir documentación de usuario
5. **Testing**: Crear tests unitarios y de integración