# 🚀 Pulso-Back: API + ETL Backend (SIMPLIFICADO)

**API + ETL Backend Simplificado para Dashboard Cobranzas Telefónica**

FastAPI + Redis + PostgreSQL + BigQuery con ETL Incremental Puro

## 🏗️ Arquitectura Simplificada

```
BigQuery → ETL Incremental → PostgreSQL → FastAPI → React Dashboard
                ↓
        Watermarks Simples (solo última fecha)
```

## 🎯 Stack Tecnológico

- **FastAPI** - API REST con OpenAPI automático
- **Redis** - Cache de consultas frecuentes
- **PostgreSQL (asyncpg)** - Storage para ETL
- **BigQuery** - Source of truth
- **ETL Incremental** - Solo datos nuevos sin lógicas complejas

## 🔄 ETL Incremental Simplificado

**Características:**
- ✅ **Watermarks simples**: Solo última fecha extraída por tabla
- ✅ **Incremental puro**: `WHERE fecha > última_fecha_extraída`
- ✅ **Sin lógicas de negocio**: Solo extracción y carga
- ✅ **Sin campañas complejas**: Directo por fechas
- ✅ **Debuggeable**: Un solo archivo, lógica lineal

## 🚀 Quick Start

```bash
# 1. Clonar repositorio
git clone https://github.com/reyer3/Pulso-Back.git
cd Pulso-Back

# 2. ETL Incremental
python etl/simple_incremental_etl.py --tables asignaciones trandeuda pagos

# 3. Verificar API
curl http://localhost:8000/health
```

## 📁 Estructura Simplificada

```
pulso-back/
├── app/                    # FastAPI Application  
│   ├── api/v1/            # API endpoints
│   ├── core/              # Configuration
│   ├── models/            # Pydantic models
│   ├── repositories/      # Data access layer
│   ├── services/          # Business logic
│   └── utils/             # Utilities
├── etl/                   # ETL Simplificado
│   ├── simple_incremental_etl.py  # 🆕 ETL principal
│   ├── extractors/        # BigQuery extraction
│   ├── loaders/           # PostgreSQL loading
│   ├── sql/               # SQL queries
│   └── config.py          # Table configuration
├── scripts/               # Deployment & cleanup
└── tests/                 # Testing
```

## 🔄 ETL Incremental

**Ejecutar ETL:**

```bash
# Todas las tablas
python etl/simple_incremental_etl.py

# Tablas específicas
python etl/simple_incremental_etl.py --tables asignaciones trandeuda pagos

# Con debug
python etl/simple_incremental_etl.py --log-level DEBUG
```

**Cómo funciona:**
1. **Lee watermark**: Última fecha extraída por tabla
2. **Extrae incremental**: `WHERE fecha > watermark`
3. **Carga datos**: UPSERT a PostgreSQL
4. **Actualiza watermark**: Nueva fecha máxima

## 📊 Watermarks Simples

```sql
-- Ver estado de watermarks
SELECT table_name, last_extracted_at, updated_at 
FROM etl_watermarks_simple 
ORDER BY updated_at DESC;

-- Reset manual de watermark
UPDATE etl_watermarks_simple 
SET last_extracted_at = '2025-07-01 00:00:00+00'
WHERE table_name = 'asignaciones';
```

## 📋 API Endpoints

```
GET  /api/v1/dashboard     # Dashboard principal
GET  /api/v1/evolution     # Evolutivos por día
GET  /api/v1/assignment    # Análisis de asignación
GET  /api/v1/health        # Health check
GET  /docs                 # OpenAPI docs
```

## 🧹 Limpieza de Archivos

Se eliminaron componentes complejos innecesarios:
- ❌ `campaign_catchup_pipeline.py`
- ❌ `hybrid_raw_pipeline.py`
- ❌ `mart_build_pipeline.py`
- ❌ `dependencies.py` (complejo)
- ❌ `watermarks.py` (complejo)

**Mantenidos:**
- ✅ `simple_incremental_etl.py`
- ✅ `extractors/` y `loaders/`
- ✅ `config.py` y `sql/`

## 🎯 Ventajas de la Simplificación

1. **ETL Predecible**: Solo extrae datos nuevos desde watermark
2. **Debugging Simple**: Un archivo, lógica lineal
3. **Sin Race Conditions**: Watermarks atómicos
4. **Performance**: Solo datos incrementales
5. **Mantenible**: Código claro y directo

## 🐳 Docker & Deploy

```bash
# Production deploy
./scripts/deploy.sh production

# ETL en contenedor
docker run pulso-back python etl/simple_incremental_etl.py
```

## 🧪 Testing

```bash
# Unit tests
pytest tests/unit/

# ETL test
python etl/simple_incremental_etl.py --tables calendario --log-level DEBUG
```

---
**Simplificado por Ricardo Reyes para Onbotgo**

> 🎯 **Filosofía**: ETL incremental puro sin abstracciones innecesarias
