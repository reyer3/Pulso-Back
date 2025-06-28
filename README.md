# 🚀 Pulso-Back: API + ETL Backend

**API + ETL Backend para Dashboard Cobranzas Telefónica**

FastAPI + Redis + PostgreSQL + BigQuery con patrones KISS y DRY

## 🏗️ Arquitectura

```
BigQuery → ETL Pipeline → Redis Cache → FastAPI → React Dashboard
                ↓
            PostgreSQL (persistencia)
```

## 🎯 Stack Tecnológico

- **FastAPI** - API REST con OpenAPI automático
- **Redis** - Cache de consultas frecuentes
- **PostgreSQL (asyncpg)** - Storage para ETL (acceso con `asyncpg`)
- **BigQuery (`google-cloud-bigquery`)** - Source of truth (acceso directo con cliente oficial)
- **Docker** - Containerización
- **Traefik** - Reverse proxy
- **Celery** - Job scheduling
- **Prometheus** - Monitoring

## 📊 Patrones de Diseño

- **Repository Pattern** - Abstracción de acceso a datos
- **Service Pattern** - Lógica de negocio encapsulada
- **Factory Pattern** - Creación de objetos
- **Dependency Injection** - Inversión de control
- **Strategy Pattern** - Algoritmos intercambiables
- **Cache-Aside Pattern** - Estrategia de cache
- **Template Method Pattern** - Pipelines ETL

## 🚀 Quick Start

```bash
# 1. Clonar repositorio
git clone https://github.com/reyer3/Pulso-Back.git
cd Pulso-Back

# 2. Configurar ambiente
cp .env.example .env
# Editar .env con tus credenciales

# 3. Levantar con Docker
docker-compose up -d

# 4. Verificar API
curl http://localhost:8000/health
```

## 📁 Estructura del Proyecto

```
pulso-back/
├── app/                    # FastAPI Application  
│   ├── api/v1/            # API endpoints
│   ├── core/              # Configuration
│   ├── models/            # Pydantic models
│   ├── repositories/      # Data access layer
│   ├── services/          # Business logic
│   └── utils/             # Utilities
├── etl/                   # ETL Pipeline
│   ├── extractors/        # Data extraction
│   ├── transformers/      # Data transformation  
│   ├── loaders/           # Data loading
│   └── pipelines/         # ETL orchestration
├── docker/                # Docker configs
├── scripts/               # Deployment scripts
└── tests/                 # Testing
```

## 🔄 ETL Pipeline

**Refresh cada 3 horas:**
1. **Extract** - BigQuery views
2. **Transform** - Agregaciones y cálculos
3. **Load** - Redis cache + PostgreSQL

## 📋 API Endpoints

```
GET  /api/v1/dashboard     # Dashboard principal
GET  /api/v1/evolution     # Evolutivos por día
GET  /api/v1/assignment    # Análisis de asignación
GET  /api/v1/health        # Health check
GET  /docs                 # OpenAPI docs
```

## 🐳 Docker & Traefik

Integrado con tu stack existente:
- **Traefik** - Routing automático
- **Redis** - Reutiliza tu instancia
- **PostgreSQL** - Nuevo servicio (usando `asyncpg`, no `psycopg2`)

**Nota importante sobre dependencias de base de datos:** El backend solo usa BigQuery (via `google-cloud-bigquery`) y PostgreSQL (via `asyncpg`). No hay dependencia de `psycopg2` ni de SQLAlchemy para el acceso en tiempo de ejecución a PostgreSQL. SQLAlchemy puede seguir usándose para migraciones con Alembic si es necesario.

## 📊 Monitoring

- **Prometheus** - Métricas
- **Grafana** - Dashboards
- **Logs** - Structured logging

## 🧪 Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests  
pytest tests/integration/

# Coverage
pytest --cov=app
```

## 🚀 Deploy

```bash
# Production deploy
./scripts/deploy.sh production

# Staging deploy
./scripts/deploy.sh staging
```

---
**Creado por Ricardo Reyes para Onbotgo**

#!/usr/bin/env bash
set -euo pipefail

PROJECT="BI_USA"
LOCATION="us-east1"

TABLES=(
  "voicebot_P3fV4dWNeMkN5RJMhV8e"
  "batch_P3fV4dWNeMkN5RJMhV8e_pagos"
  "batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda"
  "mibotair_P3fV4dWNeMkN5RJMhV8e"
  "bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5"
)

echo "🚀 EDA simple  • Proyecto: $PROJECT  • Región: $LOCATION"
for t in "${TABLES[@]}"; do
  FULL="$PROJECT.$t"
  echo "=================================================="
  echo "🔹 Tabla: $FULL"

  echo "📌 Esquema bruto:"
  bq --location="$LOCATION" show --format=prettyjson "$FULL"

  echo -e "\n📌 Total de filas:"
  bq --location="$LOCATION" query --quiet --nouse_legacy_sql --format=json \
    "SELECT COUNT(*) AS total FROM \`$FULL\`"

  echo -e "\n📌 Muestra (5 filas):"
  bq --location="$LOCATION" head --max_rows=5 "$FULL"
  echo
done
echo "✅ Terminado."