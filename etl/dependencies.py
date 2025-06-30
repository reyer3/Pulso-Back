# etl/dependencies.py - SINTAXIS CORREGIDA

"""
🔧 ETL Dependencies Container - CORREGIDO

FIXES:
- Comas extra removidas
- Estructura correcta del constructor
- Imports añadidos
- Parámetros ordenados correctamente
"""

from typing import Optional

# Extractors
from etl.extractors.bigquery_extractor import BigQueryExtractor
# Loaders
from etl.loaders.postgres_loader import PostgresLoader
# Pipelines
from etl.pipelines.campaign_catchup_pipeline import CampaignCatchUpPipeline
# Transformers
from etl.transformers.raw_data_transformer import RawTransformerRegistry
from shared.core.logging import LoggerMixin
from shared.database.connection import get_database_manager, DatabaseManager

# 🔧 FIXED: Import desde el archivo correcto donde está el HybridRawDataPipeline
try:
    from etl.pipelines.raw_data_pipeline import HybridRawDataPipeline, ExtractionStrategy

    HYBRID_PIPELINE_AVAILABLE = True
except ImportError:
    HYBRID_PIPELINE_AVAILABLE = False
    HybridRawDataPipeline = None

# 🔧 FIXED: Import condicional para MartBuildPipeline
try:
    from etl.pipelines.mart_build_pipeline import MartBuildPipeline

    MART_PIPELINE_AVAILABLE = True
except ImportError:
    MART_PIPELINE_AVAILABLE = False
    MartBuildPipeline = None

# Watermarks
from etl.watermarks import get_watermark_manager, WatermarkManager
from etl.config import ETLConfig


class ETLDependencies(LoggerMixin):
    """
    Contenedor de dependencias para el sistema ETL.

    🔧 CORREGIDO:
    - Sintaxis de constructores arreglada
    - Imports condicionales para compatibilidad
    - Parámetros ordenados correctamente
    """

    def __init__(self):
        super().__init__()

        # Database
        self._db_manager: Optional[DatabaseManager] = None

        # Core components - cached instances
        self._bigquery_extractor: Optional[BigQueryExtractor] = None
        self._raw_transformer_registry: Optional[RawTransformerRegistry] = None
        self._postgres_loader: Optional[PostgresLoader] = None

        # Watermarks
        self._watermark_manager: Optional[WatermarkManager] = None

        # Pipelines - cached instances
        self._hybrid_raw_pipeline: Optional[HybridRawDataPipeline] = None
        self._mart_build_pipeline = None  # Type will be MartBuildPipeline if available
        self._campaign_catchup_pipeline: Optional[CampaignCatchUpPipeline] = None

    async def init_resources(self) -> None:
        """Initialize managed resources (DB connections, etc.)"""
        self.logger.info("🔧 Initializing ETL resources...")

        # Initialize database manager
        self._db_manager = await get_database_manager()

        # Initialize watermark system
        self._watermark_manager = await get_watermark_manager()

        self.logger.info("✅ ETL resources initialized successfully")

    async def shutdown_resources(self) -> None:
        """Gracefully shutdown managed resources"""
        self.logger.info("🔌 Shutting down ETL resources...")

        # Shutdown database connections
        if self._db_manager:
            await self._db_manager.close()
            self._db_manager = None

        # Reset cached instances
        self._bigquery_extractor = None
        self._raw_transformer_registry = None
        self._postgres_loader = None
        self._watermark_manager = None
        self._raw_data_pipeline = None
        self._hybrid_raw_pipeline = None
        self._mart_build_pipeline = None
        self._campaign_catchup_pipeline = None

        self.logger.info("✅ ETL resources shut down successfully")

    # =============================================================================
    # CORE COMPONENTS
    # =============================================================================

    def bigquery_extractor(self) -> BigQueryExtractor:
        """Get BigQuery extractor instance"""
        if self._bigquery_extractor is None:
            self._bigquery_extractor = BigQueryExtractor()
        return self._bigquery_extractor

    def raw_transformer_registry(self) -> RawTransformerRegistry:
        """Get raw data transformer registry"""
        if self._raw_transformer_registry is None:
            self._raw_transformer_registry = RawTransformerRegistry()
        return self._raw_transformer_registry

    async def postgres_loader(self) -> PostgresLoader:
        """Get PostgreSQL loader instance"""
        if self._postgres_loader is None:
            if self._db_manager is None:
                raise RuntimeError("Database manager not initialized. Call init_resources() first.")
            self._postgres_loader = PostgresLoader(self._db_manager)
        return self._postgres_loader

    async def watermark_manager(self) -> WatermarkManager:
        """Get watermark manager instance"""
        if self._watermark_manager is None:
            self._watermark_manager = await get_watermark_manager()
        return self._watermark_manager

    # =============================================================================
    # PIPELINES
    # =============================================================================


    async def hybrid_raw_pipeline(self):
        """Get hybrid raw data pipeline (si está disponible)"""
        if self._hybrid_raw_pipeline is None:
            extractor = self.bigquery_extractor()
            transformer = self.raw_transformer_registry()
            loader = await self.postgres_loader()
            watermark_manager = await self.watermark_manager()

            self._hybrid_raw_pipeline = HybridRawDataPipeline(
                extractor=extractor,
                transformer=transformer,
                loader=loader,
                watermark_manager=watermark_manager
            )

        return self._hybrid_raw_pipeline

    async def mart_build_pipeline(self):
        """🔧 FIXED: Get mart build pipeline instance"""
        if not MART_PIPELINE_AVAILABLE:
            self.logger.warning("⚠️ Mart build pipeline not available")
            return None

        if self._mart_build_pipeline is None:
            if self._db_manager is None:
                raise RuntimeError("Database manager not initialized. Call init_resources() first.")

            self._mart_build_pipeline = MartBuildPipeline(
                db_manager=self._db_manager,
                project_uid=ETLConfig.PROJECT_UID  # 🔧 FIXED: Sin () porque es una constante
            )

        return self._mart_build_pipeline

    async def campaign_catchup_pipeline(self) -> CampaignCatchUpPipeline:
        """🔧 FIXED: Get campaign catch-up pipeline instance"""
        if self._campaign_catchup_pipeline is None:
            # Obtener dependencias requeridas
            if self._db_manager is None:
                raise RuntimeError("Database manager not initialized. Call init_resources() first.")

            # 🔧 FIXED: Obtener pipelines y managers
            hybrid_pipeline = await self.hybrid_raw_pipeline()
            watermark_manager = await self.watermark_manager()

            # Mart pipeline es opcional
            mart_build_pipeline = None
            if MART_PIPELINE_AVAILABLE:
                mart_build_pipeline = await self.mart_build_pipeline()

            # 🔧 FIXED: Constructor corregido sin comas extra
            self._campaign_catchup_pipeline = CampaignCatchUpPipeline(
                db_manager=self._db_manager,  # Primera posición
                watermark_manager=watermark_manager,  # Segunda posición
                raw_data_pipeline=hybrid_pipeline,  # 🔧 FIXED: Parámetro correcto
                mart_build_pipeline=mart_build_pipeline  # Última posición, opcional
            )

        return self._campaign_catchup_pipeline

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    async def health_check(self) -> dict:
        """Perform health check on all components"""
        health_status = {
            "database": "unknown",
            "bigquery_extractor": "unknown",
            "postgres_loader": "unknown",
            "watermark_system": "unknown",
            "hybrid_pipeline": "unknown",
            "mart_pipeline": "unknown",
            "overall_status": "unknown"
        }

        try:
            # Check database
            if self._db_manager:
                await self._db_manager.execute_query("SELECT 1")
                health_status["database"] = "healthy"
            else:
                health_status["database"] = "not_initialized"

            # Check BigQuery extractor
            extractor = self.bigquery_extractor()
            if extractor:
                health_status["bigquery_extractor"] = "healthy"

            # Check PostgreSQL loader
            try:
                loader = await self.postgres_loader()
                if loader:
                    health_status["postgres_loader"] = "healthy"
            except Exception:
                health_status["postgres_loader"] = "unhealthy"

            # Check watermark system
            try:
                from etl.watermarks import watermark_health_check
                watermark_health = await watermark_health_check()
                health_status["watermark_system"] = watermark_health["status"]
            except Exception:
                health_status["watermark_system"] = "unhealthy"

            # 🔧 FIXED: Check pipeline availability
            health_status["hybrid_pipeline"] = "available" if HYBRID_PIPELINE_AVAILABLE else "not_available"
            health_status["mart_pipeline"] = "available" if MART_PIPELINE_AVAILABLE else "not_available"

            # Overall status
            critical_components = ["database", "bigquery_extractor", "postgres_loader", "watermark_system"]
            unhealthy_critical = [k for k in critical_components if
                                  health_status[k] not in ["healthy", "not_initialized"]]

            if not unhealthy_critical:
                health_status["overall_status"] = "healthy"
            elif len(unhealthy_critical) == len(critical_components):
                health_status["overall_status"] = "critical"
            else:
                health_status["overall_status"] = "degraded"

        except Exception as e:
            health_status["overall_status"] = "critical"
            health_status["error"] = str(e)

        return health_status


# =============================================================================
# GLOBAL SINGLETON INSTANCE
# =============================================================================

# Singleton instance del contenedor de dependencias
etl_dependencies = ETLDependencies()


# =============================================================================
# CONVENIENCE FUNCTIONS (backward compatibility)
# =============================================================================

async def get_bigquery_extractor() -> BigQueryExtractor:
    """Convenience function for getting BigQuery extractor"""
    return etl_dependencies.bigquery_extractor()


async def get_postgres_loader() -> PostgresLoader:
    """Convenience function for getting PostgreSQL loader"""
    return await etl_dependencies.postgres_loader()


async def get_raw_transformer_registry() -> RawTransformerRegistry:
    """Convenience function for getting transformer registry"""
    return etl_dependencies.raw_transformer_registry()


async def get_hybrid_raw_pipeline():
    """Convenience function for getting hybrid pipeline (with fallback)"""
    return await etl_dependencies.hybrid_raw_pipeline()


async def get_campaign_catchup_pipeline() -> CampaignCatchUpPipeline:
    """Convenience function for getting catchup pipeline"""
    return await etl_dependencies.campaign_catchup_pipeline()


# =============================================================================
# INITIALIZATION HELPERS
# =============================================================================

async def initialize_etl_system() -> None:
    """Initialize the complete ETL system"""
    await etl_dependencies.init_resources()


async def shutdown_etl_system() -> None:
    """Shutdown the complete ETL system"""
    await etl_dependencies.shutdown_resources()


async def etl_system_health_check() -> dict:
    """Perform comprehensive ETL system health check"""
    return await etl_dependencies.health_check()