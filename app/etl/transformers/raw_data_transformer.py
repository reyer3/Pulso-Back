"""
🔄 Raw Data Transformers - BigQuery to PostgreSQL Raw Tables
FIXED: Column case sensitivity mapping for PostgreSQL compatibility
FIXED: Removed field duplications that caused INSERT column mismatches
FIXED: Inconsistencias en métodos helper y manejo de errores
ADDED: Debug logging for NULL primary key investigation
CRITICAL FIX: gestiones_unificadas CHECK constraint violations

ISSUE: Transform gestiones data was causing CHECK constraint violations:
- chk_gestiones_unificadas_contactabilidad: Invalid contactabilidad values
- chk_gestiones_unificadas_fecha_consistency: Date inconsistency

SOLUTION:
✅ Added contactabilidad validation and mapping to valid values
✅ Enforced fecha_gestion = DATE(timestamp_gestion) consistency  
✅ Added data cleaning filters for invalid records
✅ Proper fallback to valid default values
✅ Enhanced logging for debugging problematic records
"""

from datetime import datetime, date