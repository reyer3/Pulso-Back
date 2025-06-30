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
"""

# IMPORTS A AGREGAR EN etl/dependencies.py:
"""
from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
from etl.pipelines.full_etl_orchestrator import FullETLOrchestrator
"""

# MÉTODOS A AGREGAR EN LA CLASE ETLDependencies:

# Pipelines AUX y Full ETL
async def aux_build_pipeline(self):
    """Get AUX build pipeline instance"""
    if not hasattr(self, '_aux_build_pipeline') or self._aux_build_pipeline is None:
        if self._db_manager is None:
            raise RuntimeError("Database manager not initialized. Call init_resources() first.")
            
        self._aux_build_pipeline = AuxBuildPipeline(
            db_manager=self._db_manager,
            project_uid=ETLConfig.PROJECT_UID
        )
        
    return self._aux_build_pipeline

async def full_etl_orchestrator(self):
    """Get full ETL orchestrator instance"""
    if not hasattr(self, '_full_etl_orchestrator') or self._full_etl_orchestrator is None:
        if self._db_manager is None:
            raise RuntimeError("Database manager not initialized. Call init_resources() first.")
            
        # Obtener pipeline RAW
        raw_pipeline = await self.hybrid_raw_pipeline()
        
        self._full_etl_orchestrator = FullETLOrchestrator(
            db_manager=self._db_manager,
            project_uid=ETLConfig.PROJECT_UID,
            raw_pipeline=raw_pipeline
        )
        
    return self._full_etl_orchestrator

# ACTUALIZAR EL MÉTODO shutdown_resources para incluir los nuevos pipelines:
"""
async def shutdown_resources(self) -> None:
    # ... código existente ...
    
    # Reset nuevas instancias
    self._aux_build_pipeline = None
    self._full_etl_orchestrator = None
    
    # ... resto del código existente ...
"""

# ACTUALIZAR EL MÉTODO health_check para incluir validaciones de AUX/MART:
"""
async def health_check(self) -> dict:
    # ... código existente ...
    
    # NEW: Check AUX pipeline availability
    try:
        aux_pipeline = await self.aux_build_pipeline()
        health_status["aux_pipeline"] = "available"
    except Exception:
        health_status["aux_pipeline"] = "not_available"
        
    # NEW: Check Full ETL orchestrator
    try:
        orchestrator = await self.full_etl_orchestrator()
        health_status["full_etl_orchestrator"] = "available"
    except Exception:
        health_status["full_etl_orchestrator"] = "not_available"
    
    # ... resto del código existente ...
"""

# CONVENIENCE FUNCTIONS A AGREGAR AL FINAL DEL ARCHIVO:

async def get_aux_build_pipeline():
    """Convenience function for getting AUX build pipeline"""
    return await etl_dependencies.aux_build_pipeline()

async def get_full_etl_orchestrator():
    """Convenience function for getting full ETL orchestrator"""
    return await etl_dependencies.full_etl_orchestrator()

# EJEMPLOS DE USO DESPUÉS DE LA INTEGRACIÓN:
"""
# Ejecutar solo AUX para una campaña
aux_pipeline = await get_aux_build_pipeline()
result = await aux_pipeline.run_for_campaign(campaign)

# Ejecutar pipeline completo RAW→AUX→MART
orchestrator = await get_full_etl_orchestrator()
result = await orchestrator.run_full_etl_for_campaign(campaign)

# Ejecutar solo AUX→MART (RAW ya cargado)
result = await orchestrator.run_aux_mart_only(campaign)

# Validar pipeline completo
validation = await orchestrator.validate_full_pipeline(campaign)
"""

# ORDEN DE INTEGRACIÓN RECOMENDADO:
"""
1. Agregar imports al inicio del archivo
2. Agregar métodos aux_build_pipeline y full_etl_orchestrator a ETLDependencies
3. Actualizar shutdown_resources y health_check
4. Agregar convenience functions al final
5. Probar con: python -c "import asyncio; from etl.dependencies import get_full_etl_orchestrator; print('OK')"
"""