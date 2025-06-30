# etl/dependencies.py

"""
Contenedor de dependencias para el sistema ETL.
Gestiona la inicialización y ciclo de vida de componentes.
"""

from typing import Optional

from etl.config import ETLConfig
from etl.extractors.bigquery_extractor import BigQueryExtractor
from etl.loaders.postgres_loader import PostgresLoader
from etl.pipelines.campaign_catchup_pipeline import CampaignCatchUpPipeline
from etl.pipelines.mart_build_pipeline import MartBuildPipeline
from etl.pipelines.raw_data_pipeline import RawDataPipeline
from etl.transformers.raw_data_transformer import RawTransformerRegistry
from etl.watermarks import WatermarkManager, get_watermark_manager
from shared.core.logging import LoggerMixin
from shared.database.connection import DatabaseManager, get_database_manager


class ETLDependencies(LoggerMixin):
    """
    Contenedor central de dependencias para el sistema ETL.
    Maneja la inicialización lazy y ciclo de vida de componentes.
    """

    def __init__(self):
        super().__init__()
        # Componentes básicos
        self._db_manager: Optional[DatabaseManager] = None
        self._watermark_manager: Optional[WatermarkManager] = None
        self._extractor: Optional[BigQueryExtractor] = None
        self._transformer: Optional[RawTransformerRegistry] = None
        self._loader: Optional[PostgresLoader] = None

        # Pipelines
        self._raw_data_pipeline: Optional[RawDataPipeline] = None
        self._mart_build_pipeline: Optional[MartBuildPipeline] = None
        self._campaign_catchup_pipeline: Optional[CampaignCatchUpPipeline] = None

        # Estado
        self._initialized = False

    async def init_resources(self):
        """Inicializa recursos compartidos como pools de conexión."""
        if self._initialized:
            return

        self.logger.info("🔧 Initializing ETL resources...")

        # Inicializar database manager
        self._db_manager = await get_database_manager()
        self._watermark_manager = await get_watermark_manager()

        self._initialized = True
        self.logger.info("✅ ETL resources initialized successfully")

    async def shutdown_resources(self):
        """Cierra recursos compartidos de manera limpia."""
        if not self._initialized:
            return

        self.logger.info("🔌 Shutting down ETL resources...")

        # Cerrar conexiones de base de datos
        if self._db_manager:
            await self._db_manager.close()

        self._initialized = False
        self.logger.info("✅ ETL resources shut down successfully")

    # ========================================
    # COMPONENTES BÁSICOS
    # ========================================

    def database_manager(self) -> DatabaseManager:
        """Obtiene el gestor de base de datos."""
        if not self._initialized:
            raise RuntimeError("ETL dependencies not initialized. Call init_resources() first.")
        return self._db_manager

    def watermark_manager(self) -> WatermarkManager:
        """Obtiene el gestor de watermarks."""
        if not self._initialized:
            raise RuntimeError("ETL dependencies not initialized. Call init_resources() first.")
        return self._watermark_manager

    def bigquery_extractor(self) -> BigQueryExtractor:
        """Obtiene el extractor de BigQuery (lazy initialization)."""
        if self._extractor is None:
            self._extractor = BigQueryExtractor()
        return self._extractor

    def raw_transformer(self) -> RawTransformerRegistry:
        """Obtiene el registro de transformadores raw (lazy initialization)."""
        if self._transformer is None:
            self._transformer = RawTransformerRegistry()
        return self._transformer

    def postgres_loader(self) -> PostgresLoader:
        """Obtiene el cargador de PostgreSQL (lazy initialization)."""
        if self._loader is None:
            if not self._initialized:
                raise RuntimeError("ETL dependencies not initialized. Call init_resources() first.")
            self._loader = PostgresLoader(self._db_manager)
        return self._loader

    # ========================================
    # PIPELINES
    # ========================================

    def raw_data_pipeline(self) -> RawDataPipeline:
        """Obtiene el pipeline de datos raw (lazy initialization)."""
        if self._raw_data_pipeline is None:
            self._raw_data_pipeline = RawDataPipeline(
                extractor=self.bigquery_extractor(),
                transformer=self.raw_transformer(),
                loader=self.postgres_loader()
            )
        return self._raw_data_pipeline

    def mart_build_pipeline(self) -> MartBuildPipeline:
        """Obtiene el pipeline de construcción de marts (lazy initialization)."""
        if self._mart_build_pipeline is None:
            if not self._initialized:
                raise RuntimeError("ETL dependencies not initialized. Call init_resources() first.")
            self._mart_build_pipeline = MartBuildPipeline(
                db_manager=self._db_manager,
                project_uid=ETLConfig.PROJECT_UID
            )
        return self._mart_build_pipeline

    def campaign_catchup_pipeline(self) -> CampaignCatchUpPipeline:
        """Obtiene el pipeline principal de catch-up (lazy initialization)."""
        if self._campaign_catchup_pipeline is None:
            if not self._initialized:
                raise RuntimeError("ETL dependencies not initialized. Call init_resources() first.")

            self._campaign_catchup_pipeline = CampaignCatchUpPipeline(
                db_manager=self._db_manager,
                watermark_manager=self._watermark_manager,
                raw_data_pipeline=self.raw_data_pipeline(),
                mart_build_pipeline=self.mart_build_pipeline()
            )
        return self._campaign_catchup_pipeline

    # ========================================
    # UTILIDADES
    # ========================================

    def health_check(self) -> dict:
        """Verifica el estado de salud de las dependencias."""
        return {
            "initialized": self._initialized,
            "components": {
                "db_manager": self._db_manager is not None,
                "watermark_manager": self._watermark_manager is not None,
                "extractor": self._extractor is not None,
                "transformer": self._transformer is not None,
                "loader": self._loader is not None,
                "raw_pipeline": self._raw_data_pipeline is not None,
                "mart_pipeline": self._mart_build_pipeline is not None,
                "catchup_pipeline": self._campaign_catchup_pipeline is not None,
            }
        }


# ========================================
# SINGLETON GLOBAL
# ========================================

# Instancia global del contenedor de dependencias
etl_dependencies = ETLDependencies()