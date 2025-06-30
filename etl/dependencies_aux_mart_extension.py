# etl/dependencies_aux_mart_extension.py

"""
🔧 EXTENSIÓN DE DEPENDENCIAS: AUX y MART Pipelines

Este archivo extiende etl/dependencies.py para incluir:
- AuxBuildPipeline
- FullETLOrchestrator  
- Integración con pipelines existentes

INSTRUCCIONES DE INTEGRACIÓN:
1. Agregar estos imports a etl/dependencies.py
2. Agregar los métodos a la clase ETLDependencies
3. Agregar las convenience functions
4. Actualizar métodos existentes
"""

from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
from etl.pipelines.full_etl_orchestrator import FullETLOrchestrator
from etl.config import ETLConfig

# ============================================================================
# IMPORTS A AGREGAR EN etl/dependencies.py (línea ~15):
# ============================================================================
"""
from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
from etl.pipelines.full_etl_orchestrator import FullETLOrchestrator
"""

# ============================================================================
# MÉTODOS A AGREGAR EN LA CLASE ETLDependencies (después de mart_build_pipeline):
# ============================================================================

async def aux_build_pipeline(self):
    """🆕 Get AUX build pipeline instance"""
    if not hasattr(self, '_aux_build_pipeline') or self._aux_build_pipeline is None:
        if self._db_manager is None:
            raise RuntimeError("Database manager not initialized. Call init_resources() first.")
            
        self._aux_build_pipeline = AuxBuildPipeline(
            db_manager=self._db_manager,
            project_uid=ETLConfig.PROJECT_UID
        )
        
    return self._aux_build_pipeline

async def full_etl_orchestrator(self):
    """🆕 Get full ETL orchestrator instance"""
    if not hasattr(self, '_full_etl_orchestrator') or self._full_etl_orchestrator is None:
        if self._db_manager is None:
            raise RuntimeError("Database manager not initialized. Call init_resources() first.")
            
        # Obtener pipeline RAW (híbrido)
        raw_pipeline = await self.hybrid_raw_pipeline()
        
        self._full_etl_orchestrator = FullETLOrchestrator(
            db_manager=self._db_manager,
            project_uid=ETLConfig.PROJECT_UID,
            raw_pipeline=raw_pipeline
        )
        
    return self._full_etl_orchestrator

# ============================================================================
# ACTUALIZAR shutdown_resources (línea ~95) - AGREGAR ESTAS LÍNEAS:
# ============================================================================
"""
# En el método shutdown_resources, agregar después de self._campaign_catchup_pipeline = None:

        # 🆕 Reset AUX and orchestrator instances
        if hasattr(self, '_aux_build_pipeline'):
            self._aux_build_pipeline = None
        if hasattr(self, '_full_etl_orchestrator'):
            self._full_etl_orchestrator = None
"""

# ============================================================================
# ACTUALIZAR health_check (línea ~130) - AGREGAR EN LA SECCIÓN DE CHECKS:
# ============================================================================
"""
# Agregar después del check de mart_pipeline:

            # 🆕 Check AUX pipeline availability
            try:
                aux_pipeline = await self.aux_build_pipeline()
                health_status["aux_pipeline"] = "available"
            except Exception:
                health_status["aux_pipeline"] = "not_available"
                
            # 🆕 Check Full ETL orchestrator
            try:
                orchestrator = await self.full_etl_orchestrator()
                health_status["full_etl_orchestrator"] = "available"
            except Exception:
                orchestrator_health_status["full_etl_orchestrator"] = "not_available"
"""

# ============================================================================
# CONVENIENCE FUNCTIONS A AGREGAR AL FINAL DEL ARCHIVO (línea ~200):
# ============================================================================

async def get_aux_build_pipeline():
    """🆕 Convenience function for getting AUX build pipeline"""
    return await etl_dependencies.aux_build_pipeline()

async def get_full_etl_orchestrator():
    """🆕 Convenience function for getting full ETL orchestrator"""
    return await etl_dependencies.full_etl_orchestrator()

# ============================================================================
# ACTUALIZAR __init__ DE ETLDependencies (línea ~30) - AGREGAR ESTAS LÍNEAS:
# ============================================================================
"""
# En el método __init__, agregar después de self._campaign_catchup_pipeline = None:

        # 🆕 AUX and orchestrator pipelines
        self._aux_build_pipeline = None
        self._full_etl_orchestrator = None
"""

# ============================================================================
# CÓDIGO DE INTEGRACIÓN COMPLETO PARA COPIAR/PEGAR:
# ============================================================================

# 1. IMPORTS (agregar en línea ~15):
IMPORTS_TO_ADD = """
from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
from etl.pipelines.full_etl_orchestrator import FullETLOrchestrator
"""

# 2. INIT (agregar en __init__ después de línea ~30):
INIT_TO_ADD = """
        # 🆕 AUX and orchestrator pipelines
        self._aux_build_pipeline = None
        self._full_etl_orchestrator = None
"""

# 3. MÉTODOS (agregar después de mart_build_pipeline):
METHODS_TO_ADD = """
    async def aux_build_pipeline(self):
        \"\"\"🆕 Get AUX build pipeline instance\"\"\"
        if not hasattr(self, '_aux_build_pipeline') or self._aux_build_pipeline is None:
            if self._db_manager is None:
                raise RuntimeError("Database manager not initialized. Call init_resources() first.")
                
            self._aux_build_pipeline = AuxBuildPipeline(
                db_manager=self._db_manager,
                project_uid=ETLConfig.PROJECT_UID
            )
            
        return self._aux_build_pipeline

    async def full_etl_orchestrator(self):
        \"\"\"🆕 Get full ETL orchestrator instance\"\"\"
        if not hasattr(self, '_full_etl_orchestrator') or self._full_etl_orchestrator is None:
            if self._db_manager is None:
                raise RuntimeError("Database manager not initialized. Call init_resources() first.")
                
            # Obtener pipeline RAW (híbrido)
            raw_pipeline = await self.hybrid_raw_pipeline()
            
            self._full_etl_orchestrator = FullETLOrchestrator(
                db_manager=self._db_manager,
                project_uid=ETLConfig.PROJECT_UID,
                raw_pipeline=raw_pipeline
            )
            
        return self._full_etl_orchestrator
"""

# 4. SHUTDOWN (agregar en shutdown_resources):
SHUTDOWN_TO_ADD = """
        # 🆕 Reset AUX and orchestrator instances
        if hasattr(self, '_aux_build_pipeline'):
            self._aux_build_pipeline = None
        if hasattr(self, '_full_etl_orchestrator'):
            self._full_etl_orchestrator = None
"""

# 5. HEALTH CHECK (agregar en health_check):
HEALTH_CHECK_TO_ADD = """
            # 🆕 Check AUX pipeline availability
            try:
                aux_pipeline = await self.aux_build_pipeline()
                health_status["aux_pipeline"] = "available"
            except Exception:
                health_status["aux_pipeline"] = "not_available"
                
            # 🆕 Check Full ETL orchestrator
            try:
                orchestrator = await self.full_etl_orchestrator()
                health_status["full_etl_orchestrator"] = "available"
            except Exception:
                health_status["full_etl_orchestrator"] = "not_available"
"""

# 6. CONVENIENCE FUNCTIONS (agregar al final):
CONVENIENCE_TO_ADD = """
async def get_aux_build_pipeline():
    \"\"\"🆕 Convenience function for getting AUX build pipeline\"\"\"
    return await etl_dependencies.aux_build_pipeline()

async def get_full_etl_orchestrator():
    \"\"\"🆕 Convenience function for getting full ETL orchestrator\"\"\"
    return await etl_dependencies.full_etl_orchestrator()
"""

# ============================================================================
# EJEMPLOS DE USO DESPUÉS DE LA INTEGRACIÓN:
# ============================================================================

USAGE_EXAMPLES = """
# 1. Ejecutar solo AUX para una campaña
from etl.dependencies import get_aux_build_pipeline
from etl.models import CampaignWindow
from datetime import date

campaign = CampaignWindow(
    archivo="TEST_CAMPAIGN",
    fecha_apertura=date(2024, 12, 1),
    fecha_cierre=date(2024, 12, 31),
    tipo_cartera="REGULAR",
    estado_cartera="OPEN"
)

aux_pipeline = await get_aux_build_pipeline()
result = await aux_pipeline.run_for_campaign(campaign)
print(f"AUX Result: {result['status']}, {result['total_rows_processed']} rows")

# 2. Ejecutar pipeline completo RAW→AUX→MART
from etl.dependencies import get_full_etl_orchestrator

orchestrator = await get_full_etl_orchestrator()
result = await orchestrator.run_full_etl_for_campaign(campaign)
print(f"Full ETL Status: {result['overall_status']}")

# 3. Ejecutar solo AUX→MART (RAW ya cargado)
result = await orchestrator.run_aux_mart_only(campaign)
print(f"AUX+MART Result: {result['overall_status']}")

# 4. Validar pipeline completo
validation = await orchestrator.validate_full_pipeline(campaign)
print(f"Pipeline Validation: {validation['overall_status']}")

# 5. Health check con nuevos pipelines
from etl.dependencies import etl_system_health_check

health = await etl_system_health_check()
print(f"AUX Pipeline: {health['aux_pipeline']}")
print(f"Orchestrator: {health['full_etl_orchestrator']}")
"""

# ============================================================================
# ORDEN DE INTEGRACIÓN RECOMENDADO:
# ============================================================================

INTEGRATION_STEPS = """
PASO 1: Backup del archivo original
cp etl/dependencies.py etl/dependencies.py.backup

PASO 2: Agregar imports (línea ~15)
# Copiar IMPORTS_TO_ADD

PASO 3: Actualizar __init__ (línea ~30)  
# Copiar INIT_TO_ADD

PASO 4: Agregar métodos (después de mart_build_pipeline)
# Copiar METHODS_TO_ADD

PASO 5: Actualizar shutdown_resources (línea ~95)
# Copiar SHUTDOWN_TO_ADD

PASO 6: Actualizar health_check (línea ~130)
# Copiar HEALTH_CHECK_TO_ADD

PASO 7: Agregar convenience functions (al final)
# Copiar CONVENIENCE_TO_ADD

PASO 8: Probar integración
python -c "
import asyncio
from etl.dependencies import get_full_etl_orchestrator, etl_system_health_check

async def test():
    try:
        health = await etl_system_health_check()
        print('Health check OK:', health.get('aux_pipeline'), health.get('full_etl_orchestrator'))
        print('✅ Integration successful')
    except Exception as e:
        print('❌ Integration failed:', e)

asyncio.run(test())
"
"""

print("🔧 Extensión de dependencias lista para integración")
print("📖 Ver INTEGRATION_STEPS para orden de aplicación")