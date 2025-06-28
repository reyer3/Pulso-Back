from app.core.config import settings
from app.repositories.bigquery_repo import BigQueryRepository
from app.repositories.postgres_repo import PostgresRepository
from app.services.postgres_service import PostgresService

def get_data_repository():
    """
    Devuelve el repositorio de datos según la configuración global.
    """
    if settings.DATA_SOURCE_TYPE == "bigquery":
        return BigQueryRepository()
    elif settings.DATA_SOURCE_TYPE == "postgresql":
        pg_service = PostgresService(settings.POSTGRES_URL)
        return PostgresRepository(pg_service)
    else:
        raise ValueError(f"Unsupported DATA_SOURCE_TYPE: {settings.DATA_SOURCE_TYPE}")