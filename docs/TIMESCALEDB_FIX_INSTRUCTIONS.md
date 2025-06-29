# ⚡ CORRECCIÓN CRÍTICA TimescaleDB - Instrucciones de Recuperación

## 🔧 **Problema Resuelto**

**Error Original:**
```
cannot create a unique index without the column "periodo_date" (used in partitioning)
HINT: If you're creating a hypertable on a table with a primary key, ensure the partitioning column is part of the primary or composite key.
```

**Causa**: TimescaleDB requiere que las **columnas de partición estén incluidas en las PRIMARY KEYS**.

## ✅ **Correcciones Aplicadas**

### **Primary Keys Actualizadas:**

| **Tabla** | **PK Original** | **PK Corregida (TimescaleDB)** |
|-----------|----------------|--------------------------------|
| `raw_calendario` | `ARCHIVO` | `(ARCHIVO, periodo_date)` |
| `raw_asignaciones` | `(cod_luna, cuenta, archivo)` | `(cod_luna, cuenta, archivo, fecha_asignacion)` |
| `raw_trandeuda` | `(cod_cuenta, nro_documento, archivo)` | `(cod_cuenta, nro_documento, archivo, fecha_proceso)` |
| `raw_pagos` | `(nro_documento, fecha_pago, monto_cancelado)` | ✅ **Ya era correcta** |
| `gestiones_unificadas` | `(cod_luna, timestamp_gestion)` | ✅ **Ya era correcta** |

### **Archivos Corregidos:**
- ✅ `migrations/011-create-raw-calendario-table.sql`
- ✅ `migrations/012-create-raw-asignaciones-table.sql`
- ✅ `migrations/013-create-raw-trandeuda-table.sql`
- ✅ `app/etl/config.py` (primary keys actualizadas)

---

## 🚀 **Pasos Para Aplicar las Correcciones**

### **1. Limpiar Estado de Migraciones Fallidas**

```bash
# Conectar a PostgreSQL
psql -U pulso_sa -d pulso_db

# Verificar qué migraciones están aplicadas
SELECT * FROM _yoyo_migration ORDER BY id;

# Si hay migraciones parcialmente aplicadas, revertirlas
# (Solo si es necesario - verificar primero)
```

### **2. Aplicar Migraciones Corregidas**

```bash
# Desde el directorio Pulso-Back
cd ~/Projects/Pulso-Back

# Aplicar todas las migraciones corregidas
yoyo apply

# Confirmar todas las migraciones cuando se solicite
# y → n → y (yes to individual migrations, then yes to apply)
```

### **3. Verificar Hypertables Creadas**

```bash
# Verificar que TimescaleDB está funcionando
psql -U pulso_sa -d pulso_db -c "SELECT * FROM timescaledb_information.hypertables;"

# Debería mostrar 5 hypertables:
# - raw_calendario (partitioned by periodo_date)
# - raw_asignaciones (partitioned by fecha_asignacion) 
# - raw_trandeuda (partitioned by fecha_proceso)
# - raw_pagos (partitioned by fecha_pago)
# - gestiones_unificadas (partitioned by timestamp_gestion)
```

### **4. Verificar Estructura de Tablas**

```bash
# Verificar primary keys correctas
psql -U pulso_sa -d pulso_db -c "
SELECT 
    tablename, 
    indexname, 
    indexdef 
FROM pg_indexes 
WHERE tablename LIKE 'raw_%' 
  AND indexname LIKE '%pkey%';"
```

---

## 🎯 **Próximos Pasos Después de Migración Exitosa**

### **1. Probar Conectividad ETL**

```bash
# Iniciar aplicación
python app/main.py

# En otra terminal, probar configuración
curl http://localhost:8000/api/v1/etl/config/tables
```

### **2. Probar Extracción Básica**

```bash
# Probar calendario (tabla más simple)
curl -X POST http://localhost:8000/api/v1/etl/refresh/raw_calendario \
  -H "Content-Type: application/json" \
  -d '{"force": true, "max_concurrent": 1}'

# Verificar datos insertados
psql -U pulso_sa -d pulso_db -c "SELECT COUNT(*) FROM raw_calendario;"
```

### **3. Verificar Performance TimescaleDB**

```bash
# Ver chunks creados
psql -U pulso_sa -d pulso_db -c "
SELECT 
    hypertable_name,
    chunk_name,
    range_start,
    range_end 
FROM timescaledb_information.chunks 
ORDER BY hypertable_name, range_start;"
```

---

## 🔧 **Troubleshooting**

### **Si las Migraciones Siguen Fallando:**

1. **Rollback completo**:
   ```bash
   yoyo rollback --all
   ```

2. **Verificar TimescaleDB extension**:
   ```bash
   psql -U pulso_sa -d pulso_db -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
   ```

3. **Re-aplicar migraciones**:
   ```bash
   yoyo apply
   ```

### **Si Hay Problemas de Permisos:**

```bash
# Verificar permisos de usuario
psql -U pulso_sa -d pulso_db -c "
SELECT 
    rolname,
    rolsuper,
    rolcreatedb,
    rolcanlogin 
FROM pg_roles 
WHERE rolname = 'pulso_sa';"
```

---

## ✅ **Resultado Esperado**

Después de aplicar las migraciones corregidas:

1. ✅ **5 hypertables creadas** sin errores
2. ✅ **Primary keys incluyen columnas de partición**
3. ✅ **ETL config actualizada** con PK correctas  
4. ✅ **Sistema listo** para extracción BigQuery → PostgreSQL

**Una vez que las migraciones se apliquen exitosamente, el sistema estará completamente funcional para comenzar las pruebas de extracción de datos.**
