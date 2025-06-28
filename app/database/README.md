# 🗃️ Pulso Database & ETL Setup Guide

This guide provides a complete overview of the PostgreSQL/TimescaleDB setup and the `yoyo`-based migration system for the Pulso dashboard.

## 🎯 Data Architecture

### **Database Schema**
The database schema is managed through raw SQL migration files located in the `migrations/` directory. This approach provides a clear, version-controlled history of the database structure.

The core components of the schema are:

- **Dashboard Tables (TimescaleDB Hypertables)**: These tables are optimized for time-series data and store the main aggregated metrics for the dashboards.
  - `dashboard_data`: Main aggregated metrics.
  - `evolution_data`: Time-series data for trend analysis.
  - `assignment_data`: Monthly assignment composition analysis.
  - `operation_data`: Hourly operational metrics.
  - `productivity_data`: Agent performance metrics.

- **ETL Control Tables**: These tables manage the ETL process.
  - `etl_watermarks`: Tracks the last extraction point for each table to ensure incremental data loads.
  - `etl_execution_log`: Provides a detailed log of all ETL job executions for monitoring and debugging.

### **Frontend to Database Mapping**
| Frontend (TypeScript) | Database Table      | Primary Key                               |
|-----------------------|---------------------|-------------------------------------------|
| `DataRow`             | `dashboard_data`    | `(fecha_foto, archivo, cartera, servicio)`|
| `EvolutionDataPoint`  | `evolution_data`    | `(fecha_foto, archivo)`                   |
| `AssignmentKPI`       | `assignment_data`   | `(periodo, archivo, cartera)`             |
| `ChannelMetric`       | `operation_data`    | `(fecha_foto, hora, canal, archivo)`      |
| `AgentRankingRow`     | `productivity_data` | `(fecha_foto, correo_agente, archivo)`    |

## 🚀 Initial Setup

### **1. Environment Variables**
Create a `.env` file based on the `.env.example` template and configure your database and BigQuery credentials:

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
```

### **2. Database Setup**
With Docker and Docker Compose, the database will be set up automatically when you start the services. The `yoyo` migrations will be applied on startup to ensure the schema is up to date.

To run the setup manually, you can use the `yoyo-migrations` CLI.

## 🔄 Migration Management

Database migrations are handled by `yoyo-migrations`. All migration files are located in the `migrations/` directory and are written in plain SQL.

### **Useful `yoyo` Commands**

```bash
# Apply all pending migrations
yoyo apply --database "$DATABASE_URL"

# Roll back the last applied migration
yoyo rollback --database "$DATABASE_URL"

# Re-apply all migrations
yoyo reapply --database "$DATABASE_URL"

# Show all migrations and their status
yoyo list --database "$DATABASE_URL"
```

### **Creating a New Migration**
To create a new migration, simply add a new `.sql` file to the `migrations/` directory with a descriptive name (e.g., `008-add-new-feature-table.sql`). `yoyo` will automatically detect and apply it.

## 🎯 ETL Configuration

The ETL system is configured in `app/etl/config.py` and uses the following primary keys for `UPSERT` operations:

```python
# Example from app/etl/config.py
EXTRACTION_CONFIGS = {
    "dashboard_data": {
        "primary_key": ["fecha_foto", "archivo", "cartera", "servicio"],
        "incremental_column": "fecha_foto",
        "lookback_days": 7
    },
    # ... other table configurations
}
```

### **Running the ETL**

```bash
# Trigger a full refresh of the dashboard data
curl -X POST "http://localhost:8000/api/v1/etl/refresh/dashboard" \
  -H "Content-Type: application/json" \
  -d '{"force": true}'

# Check the status of the ETL process
curl "http://localhost:8000/api/v1/etl/status"
```

## 🔧 Production Configuration

### **TimescaleDB Optimizations**
The following tables are configured as TimescaleDB hypertables in the migration files for optimal time-series performance:

| Table               | Time Partition Column | Chunk Interval | Retention Policy |
|---------------------|-----------------------|----------------|------------------|
| `dashboard_data`    | `fecha_foto`          | 7 days         | 2 years          |
| `evolution_data`    | `fecha_foto`          | 7 days         | 2 years          |
| `operation_data`    | `fecha_foto`          | 1 day          | 1 year           |
| `productivity_data` | `fecha_foto`          | 7 days         | 2 years          |
| `etl_execution_log` | `started_at`          | 1 month        | 6 months         |

### **Monitoring Performance**

```sql
-- Check TimescaleDB chunk information
SELECT hypertable_name, chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'dashboard_data'
ORDER BY range_start DESC;

-- Review retention policies
SELECT * FROM timescaledb_information.retention_policies;
```

---

**🎉 With this setup, you have a production-ready, incremental ETL system with a transparent, SQL-based migration process, all optimized for TimescaleDB.**