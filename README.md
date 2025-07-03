# 🚀 Pulso-Back: API + ETL Backend (CLEAN ARCHITECTURE)

**API + ETL Backend Simplificado para Dashboard Cobranzas Telefónica**

FastAPI + Redis + PostgreSQL + BigQuery con ETL Incremental Puro

## 🏗️ Arquitectura Simplificada

```
BigQuery → ETL Pipeline → PostgreSQL → FastAPI → React Dashboard
                ↓
        Watermarks Simples (solo última fecha)
```

## 🎯 Stack Tecnológico

- **FastAPI** - API REST con OpenAPI automático
- **Redis** - Cache de consultas frecuentes
- **PostgreSQL (asyncpg)** - Storage para ETL
- **BigQuery** - Source of truth
- **ETL Incremental** - Solo datos nuevos sin lógicas complejas

## 🚀 Quick Start

```bash
# 1. Clonar repositorio
git clone https://github.com/reyer3/Pulso-Back.git
cd Pulso-Back

# 2. Configurar variables de entorno
cp .env.production.example .env
# Editar .env con tus credenciales

# 3. Aplicar migraciones
yoyo apply

# 4. Ejecutar ETL
python etl/main.py

# 5. Verificar API
curl http://localhost:8000/health
```

## 📁 Estructura Clean Architecture

```
pulso-back/
├── app/                         # FastAPI Application  
│   ├── api/v1/                 # API endpoints
│   ├── core/                   # Configuration
│   ├── models/                 # Pydantic models
│   ├── repositories/           # Data access layer
│   ├── services/               # Business logic
│   └── utils/                  # Utilities
├── etl/                        # ETL Clean Architecture
│   ├── main.py                 # 🆕 Standard entry point
│   ├── pipelines/              
│   │   └── simple_incremental_pipeline.py  # Core pipeline logic
│   ├── extractors/             # BigQuery extraction
│   ├── loaders/                # PostgreSQL loading
│   ├── watermarks.py           # Simple watermark management
│   ├── config.py               # Table configuration
│   └── sql/                    # SQL queries
├── scripts/                    # Deployment & utilities
└── tests/                      # Testing
```

## 🔄 ETL Commands

### **🚀 Standard Entry Point (Recomendado):**

```bash
# Procesar todas las tablas
python etl/main.py

# Tablas específicas
python etl/main.py --tables asignaciones trandeuda pagos

# Con logging detallado
python etl/main.py --log-level DEBUG

# Ver plan de ejecución (dry run)
python etl/main.py --dry-run

# Listar tablas disponibles
python etl/main.py --list-tables

# Ayuda completa
python etl/main.py --help
```

### **🔄 Legacy Entry Point (Compatibilidad):**
```bash
# Wrapper de compatibilidad (redirige a main.py)
python etl/simple_incremental_etl.py
```

### **⚙️ Comandos de Producción:**

```bash
# Cron job (cada 3 horas)
0 */3 * * * cd /app && python etl/main.py >> /var/log/etl.log 2>&1

# Docker
docker run pulso-back python etl/main.py --tables asignaciones

# Kubernetes CronJob
kubectl apply -f k8s/etl-cronjob.yaml
```

## 🎯 Cómo Funciona el ETL

### **Pipeline Flow:**
1. **Lee watermarks**: Última fecha extraída por tabla
2. **Extrae incremental**: `WHERE fecha > watermark` desde BigQuery
3. **Carga datos**: UPSERT a PostgreSQL con manejo de duplicados
4. **Actualiza watermark**: Nueva fecha máxima extraída

### **Ejemplo de Ejecución:**
```bash
$ python etl/main.py --tables asignaciones --log-level INFO

🚀 SIMPLE INCREMENTAL PIPELINE
================================================================================
📊 Tables to process: 1
📋 Tables: asignaciones

📋 [1/1] Processing: asignaciones
🆕 asignaciones: primera extracción (últimos 30 días)
🔍 Extracting asignaciones...
✅ asignaciones: extracted 15,432 records total
✅ asignaciones: loaded 15,432 records
✅ asignaciones: 15,432 records in 23.45s

📊 PIPELINE EXECUTION RESULTS
================================================================================
✅ Successful tables: 1/1
❌ Failed tables: 0
📊 Total extracted: 15,432
📊 Total loaded: 15,432
⏱️ Total duration: 23.45s

🎉 ETL Pipeline completed successfully!
```

## 📊 Watermarks Simples

### **Monitoreo:**
```sql
-- Ver estado actual de watermarks
SELECT table_name, last_extracted_at, updated_at 
FROM etl_watermarks_simple 
ORDER BY updated_at DESC;
```

### **Operaciones Manuales:**
```sql
-- Reset watermark para re-extraer datos
UPDATE etl_watermarks_simple 
SET last_extracted_at = '2025-07-01 00:00:00+00'
WHERE table_name = 'asignaciones';

-- Eliminar watermark (fuerza extracción completa)
DELETE FROM etl_watermarks_simple 
WHERE table_name = 'asignaciones';
```

### **Health Check:**
```bash
python -c "
import asyncio
from etl.watermarks import get_watermark_status
print(asyncio.run(get_watermark_status()))
"
```

## 📋 API Endpoints

```
GET  /api/v1/dashboard     # Dashboard principal
GET  /api/v1/evolution     # Evolutivos por día
GET  /api/v1/assignment    # Análisis de asignación
GET  /api/v1/health        # Health check
GET  /docs                 # OpenAPI docs
```

## 🔧 Configuración de Tablas

Las tablas se configuran en `etl/config.py`:

```python
"asignaciones": ExtractionConfig(
    table_name="asignaciones",
    table_type=TableType.RAW,
    source_table="batch_P3fV4dWNeMkN5RJMhV8e_asignacion",
    incremental_column="creado_el",           # Columna para filtrado incremental
    primary_key=["cod_luna", "cuenta"],      # Keys para UPSERT
    batch_size=50000                         # Tamaño de lote
)
```

## 🐳 Docker & Production

### **Variables de Entorno:**
```bash
# Database
POSTGRES_HOST=prod-db.company.com
POSTGRES_PASSWORD=secure_password
POSTGRES_SSLMODE=require

# BigQuery
BIGQUERY_PROJECT_ID=mibot-222814
BIGQUERY_CREDENTIALS_PATH=/app/credentials.json

# ETL
ETL_DEFAULT_BATCH_SIZE=10000
```

### **Docker Compose:**
```yaml
services:
  etl:
    image: pulso-back:latest
    environment:
      - POSTGRES_HOST=db
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    command: ["python", "etl/main.py"]
```

### **Kubernetes CronJob:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: pulso-etl
spec:
  schedule: "0 */3 * * *"  # Cada 3 horas
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: etl
            image: pulso-back:latest
            command: ["python", "etl/main.py"]
            env:
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: postgres-secret
                  key: password
```

## 🧪 Testing

```bash
# Test del pipeline
python etl/main.py --dry-run

# Test de tablas específicas  
python etl/main.py --tables calendario --log-level DEBUG

# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/
```

## 🎯 Ventajas de la Nueva Arquitectura

### **✅ Clean Architecture:**
- **Entry point limpio**: Solo maneja CLI y orquestación
- **Pipeline separado**: Lógica de negocio aislada y testeable
- **Watermarks modulares**: Reutilizables en otros scripts
- **Configuración centralizada**: Una sola fuente de verdad

### **✅ Mantenibilidad:**
- **1 entry point**: `etl/main.py` (estándar)
- **1 pipeline**: `SimpleIncrementalPipeline` (core logic)
- **Separación clara**: CLI vs Business Logic
- **Fácil testing**: Cada componente es testeable por separado

### **✅ Operacional:**
- **CLI robusto**: Validaciones, dry-run, help completo
- **Logging estructurado**: Levels apropiados por ambiente
- **Exit codes**: Integración con schedulers y CI/CD
- **Compatibilidad**: Entry point legacy para transición

### **✅ Performance:**
- **ETL incremental puro**: Solo datos nuevos
- **Watermarks optimizados**: Batch operations, validaciones
- **UPSERT eficiente**: Manejo inteligente de duplicados
- **Configuración por tabla**: Batch sizes optimizados

---

## 🔄 Migración desde Versión Anterior

### **Old Way (Deprecated):**
```bash
python etl/simple_incremental_etl.py --tables asignaciones
```

### **New Way (Recommended):**
```bash
python etl/main.py --tables asignaciones
```

### **Compatibility:**
- El entry point anterior sigue funcionando (muestra warning)
- Todos los argumentos se mapean automáticamente
- Misma funcionalidad, mejor arquitectura

---

**Refactorizado por Ricardo Reyes para Onbotgo**

> 🎯 **Filosofía**: Clean Architecture + ETL incremental puro + Entry points estándar
