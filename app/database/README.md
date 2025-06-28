# 🗃️ Pulso Database & ETL Setup Guide

Guía completa para configurar PostgreSQL/TimescaleDB y el sistema ETL incremental para el dashboard de cobranzas Telefónica.

## 🎯 Arquitectura de Datos

### **Esquema de Datos**
```
┌─────────────────────────────────────────────────────────────┐
│                     TIMESCALEDB / POSTGRESQL                │
├─────────────────────────────────────────────────────────────┤
│  📊 DASHBOARD TABLES (Hypertables)                         │
│  ├── dashboard_data     → Métricas principales agregadas   │
│  ├── evolution_data     → Series de tiempo para trending   │
│  ├── assignment_data    → Análisis de composición mensual  │
│  ├── operation_data     → Métricas operativas por hora     │
│  └── productivity_data  → Performance de agentes           │
│                                                             │
│  🔧 ETL CONTROL TABLES                                     │
│  ├── etl_watermarks     → Control de extracciones         │
│  └── etl_execution_log  → Log detallado de ejecuciones    │
└─────────────────────────────────────────────────────────────┘
```

### **Mapeo TypeScript ↔ Database**
| Frontend (TypeScript) | Database Table | Primary Key |
|----------------------|----------------|-------------|
| `DataRow` | `dashboard_data` | `(fecha_foto, archivo, cartera, servicio)` |
| `EvolutionDataPoint` | `evolution_data` | `(fecha_foto, archivo)` |
| `AssignmentKPI` | `assignment_data` | `(periodo, archivo, cartera)` |
| `ChannelMetric` | `operation_data` | `(fecha_foto, hora, canal, archivo)` |
| `AgentRankingRow` | `productivity_data` | `(fecha_foto, correo_agente, archivo)` |

## 🚀 Setup Inicial

### **1. Variables de Entorno**

Crea un archivo `.env` basado en `.env.example`:

```bash
# PostgreSQL/TimescaleDB Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/pulso_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=pulso_db
POSTGRES_USER=username
POSTGRES_PASSWORD=password

# BigQuery Configuration (for ETL)
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
BIGQUERY_PROJECT_ID=mibot-222814

# TimescaleDB Specific (Optional)
TIMESCALEDB_TELEMETRY=off
```

### **2. Configuración de Base de Datos**

#### **Opción A: Setup Automático (Recomendado)**

```bash
# Setup completo automático
python -m app.database.setup_timescaledb

# Solo verificar estado
python -m app.database.setup_timescaledb check

# Solo validar configuración
python -m app.database.setup_timescaledb validate
```

#### **Opción B: Setup Manual**

```bash
# 1. Instalar TimescaleDB (si no está instalado)
python -m app.database.setup_timescaledb install

# 2. Ejecutar migraciones
alembic -c app/database/alembic.ini upgrade head

# 3. Configurar hypertables (opcional)
python -c "
import asyncio
from app.database.setup_timescaledb import TimescaleDBSetup
setup = TimescaleDBSetup('your_database_url')
asyncio.run(setup.configure_hypertables())
"
```

### **3. Verificación del Setup**

```bash
# Verificar configuración completa
python -m app.database.setup_timescaledb validate

# Debería devolver:
# Overall Status: optimal      (con TimescaleDB)
# Overall Status: basic        (PostgreSQL estándar)
```

## 🔄 Gestión de Migraciones

### **Comandos Alembic Útiles**

```bash
# Ver estado actual
alembic -c app/database/alembic.ini current

# Ver historial de migraciones
alembic -c app/database/alembic.ini history

# Crear nueva migración
alembic -c app/database/alembic.ini revision -m "Description"

# Ejecutar migraciones
alembic -c app/database/alembic.ini upgrade head

# Rollback una migración
alembic -c app/database/alembic.ini downgrade -1
```

### **Estructura de Migraciones**

```
app/database/migrations/
├── env.py                    # Configuración Alembic
├── script.py.mako           # Template para migraciones
└── versions/
    └── 001_initial_etl_tables.py  # Migración inicial
```

## 🎯 Configuración ETL

### **Tablas y Primary Keys**

El sistema ETL está configurado para usar los siguientes primary keys:

```python
# Configuración en app/etl/config.py
EXTRACTION_CONFIGS = {
    "dashboard_data": {
        "primary_key": ["fecha_foto", "archivo", "cartera", "servicio"],
        "incremental_column": "fecha_foto",
        "lookback_days": 7
    },
    "evolution_data": {
        "primary_key": ["fecha_foto", "archivo"],
        "incremental_column": "fecha_foto",
        "lookback_days": 3
    },
    # ... más configuraciones
}
```

### **Uso del ETL**

```bash
# Refresh completo del dashboard
curl -X POST "http://localhost:8000/api/v1/etl/refresh/dashboard" \
  -H "Content-Type: application/json" \
  -d '{"force": true}'

# Verificar estado
curl "http://localhost:8000/api/v1/etl/status"

# Refresh tabla específica
curl -X POST "http://localhost:8000/api/v1/etl/refresh/table/dashboard_data?force=true"
```

## 🛠️ Desarrollo y Testing

### **Testing Individual de Componentes**

```python
# Test modelos de base de datos
from app.models.database import DashboardDataModel
from app.database.setup_timescaledb import TimescaleDBSetup

# Test ETL components
from app.etl.utils import quick_test, validate_all_tables

# Ejecutar tests
result = await quick_test()
validation = await validate_all_tables()
```

### **Debugging ETL**

```python
# Verificar watermarks
from app.etl.watermarks import get_watermark_manager

manager = await get_watermark_manager()
watermarks = await manager.get_all_watermarks()

# Test extractor BigQuery
from app.etl.extractors.bigquery_extractor import get_extractor

extractor = await get_extractor()
test_result = await extractor.test_query("SELECT 1 as test")

# Test loader PostgreSQL
from app.etl.loaders.postgres_loader import get_loader

loader = await get_loader()
stats = await loader.get_table_stats("dashboard_data")
```

### **Validación de Schema**

```python
# Verificar que schemas de DB coincidan con modelos
from app.etl.loaders.postgres_loader import PostgresLoader

loader = PostgresLoader()
for table_name in ["dashboard_data", "evolution_data"]:
    result = await loader.validate_table_schema(table_name)
    print(f"{table_name}: {result['status']}")
```

## 🔧 Configuración para Producción

### **Optimizaciones de PostgreSQL/TimescaleDB**

```sql
-- Configuraciones recomendadas para postgresql.conf
shared_preload_libraries = 'timescaledb'
max_worker_processes = 16
max_parallel_workers_per_gather = 2
work_mem = 256MB
maintenance_work_mem = 512MB
checkpoint_timeout = 10min
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 500

-- TimescaleDB específico
timescaledb.max_background_workers = 4
```

### **Configuración de Hypertables**

Las siguientes tablas se configuran automáticamente como hypertables:

| Tabla | Tiempo Partición | Chunk Interval | Retención |
|-------|------------------|----------------|-----------|
| `dashboard_data` | `fecha_foto` | 7 días | 2 años |
| `evolution_data` | `fecha_foto` | 7 días | 2 años |
| `operation_data` | `fecha_foto` | 1 día | 1 año |
| `productivity_data` | `fecha_foto` | 7 días | 2 años |
| `etl_execution_log` | `started_at` | 1 mes | 6 meses |

### **Monitoring de Performance**

```sql
-- Verificar chunks de TimescaleDB
SELECT hypertable_name, chunk_name, range_start, range_end 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'dashboard_data'
ORDER BY range_start DESC;

-- Verificar políticas de retención
SELECT * FROM timescaledb_information.retention_policies;

-- Estadísticas de compresión
SELECT * FROM timescaledb_information.compression_settings;
```

## 🚨 Troubleshooting

### **Problemas Comunes**

#### **1. TimescaleDB no disponible**
```bash
# Error: TimescaleDB extension not found
# Solución: Instalar TimescaleDB o usar PostgreSQL estándar

# Verificar extensión
psql -d pulso_db -c "SELECT * FROM pg_extension WHERE extname = 'timescaledb';"

# Instalar extensión (requiere superuser)
psql -d pulso_db -c "CREATE EXTENSION timescaledb;"
```

#### **2. Error en migraciones**
```bash
# Error: relation already exists
# Solución: Reset del estado de Alembic

alembic -c app/database/alembic.ini stamp head
alembic -c app/database/alembic.ini upgrade head
```

#### **3. Error de conexión a BigQuery**
```bash
# Error: google.auth.exceptions.DefaultCredentialsError
# Solución: Configurar credenciales

export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
gcloud auth application-default login
```

#### **4. Primary key conflicts en UPSERT**
```bash
# Error: duplicate key value violates unique constraint
# Solución: Verificar configuración de primary keys

# Verificar configuración
python -c "
from app.etl.config import ETLConfig
print(ETLConfig.get_config('dashboard_data').primary_key)
"
```

### **Logs y Debugging**

```python
# Habilitar logging detallado
import logging
logging.basicConfig(level=logging.DEBUG)

# Ver logs de ETL
from app.etl.utils import ETLTester
tester = ETLTester()
result = await tester.test_table_extraction("dashboard_data")
```

## 📊 Métricas y Monitoring

### **Endpoints de Monitoreo**

```bash
# Estado general del ETL
curl "http://localhost:8000/api/v1/etl/status"

# Estado de tabla específica
curl "http://localhost:8000/api/v1/etl/status/table/dashboard_data"

# Health check
curl "http://localhost:8000/api/v1/etl/health"

# Configuración de tablas
curl "http://localhost:8000/api/v1/etl/config/tables"
```

### **Cleanup y Mantenimiento**

```bash
# Limpiar extracciones fallidas
curl -X POST "http://localhost:8000/api/v1/etl/cleanup"

# Recovery de emergencia
python -c "
import asyncio
from app.etl.utils import emergency_recovery
result = asyncio.run(emergency_recovery())
print(result)
"
```

## 🎯 Próximos Pasos

### **Para Desarrollo**
1. ✅ Configurar base de datos con el script de setup
2. ✅ Ejecutar migraciones iniciales
3. ✅ Probar extractores y loaders individualmente
4. ✅ Configurar variables de entorno

### **Para Producción**
1. 📋 Configurar TimescaleDB en el servidor
2. 📋 Aplicar optimizaciones de performance
3. 📋 Configurar monitoreo y alertas
4. 📋 Configurar backup automático

### **Mejoras Futuras**
- [ ] Continuous aggregates automáticos
- [ ] Compresión de datos históricos
- [ ] Particionamiento adicional por cartera
- [ ] Métricas de monitoring con Prometheus
- [ ] Alertas automáticas por email/Slack

---

**🎉 Con esta configuración, tienes un sistema ETL incremental production-ready con PostgreSQL/TimescaleDB optimizado para el dashboard de cobranzas telefónica.**
