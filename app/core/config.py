"""
🔧 Configuration management using Pydantic Settings
Centralized settings for the entire application
"""

import os
from typing import List, Optional

from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings with environment variable support
    """
    
    # Application
    API_TITLE: str = Field(default="Pulso-Back API")
    API_VERSION: str = Field(default="v1")
    ENVIRONMENT: str = Field(default="development")
    DEBUG: bool = Field(default=False)
    LOG_LEVEL: str = Field(default="INFO")
    
    # Server
    HOST: str = Field(default="0.0.0.0")
    PORT: int = Field(default=8000)
    RELOAD: bool = Field(default=False)
    WORKERS: int = Field(default=1)
    
    # Data Source Configuration
    DATA_SOURCE_TYPE: str = Field(default="bigquery", description="Data source type: bigquery or postgresql")
    
    # Database
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="pulso_db")
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="password")
    POSTGRES_SCHEMA: str = Field(default="public")
    POSTGRES_URL: Optional[str] = Field(default=None)
    
    # Redis
    REDIS_HOST: str = Field(default="localhost")
    REDIS_PORT: int = Field(default=6379)
    REDIS_DB: int = Field(default=0)
    REDIS_PASSWORD: Optional[str] = Field(default=None)
    REDIS_URL: Optional[str] = Field(default=None)
    
    # Google Cloud
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = Field(default=None)
    BIGQUERY_PROJECT_ID: str = Field(default="mibot-222814")
    BIGQUERY_DATASET: str = Field(default="BI_USA")
    BIGQUERY_LOCATION: str = Field(default="US")
    
    # ETL Configuration
    ETL_SCHEDULE_CRON: str = Field(default="0 */3 * * *")
    ETL_BATCH_SIZE: int = Field(default=10000)
    ETL_TIMEOUT_SECONDS: int = Field(default=3600)
    ETL_RETRY_ATTEMPTS: int = Field(default=3)
    
    # Cache Configuration
    CACHE_TTL_DASHBOARD: int = Field(default=1800)  # 30 minutes
    CACHE_TTL_EVOLUTION: int = Field(default=3600)  # 1 hour
    CACHE_TTL_ASSIGNMENT: int = Field(default=7200)  # 2 hours
    
    # Security
    SECRET_KEY: str = Field(default="dev-secret-key-change-in-production")
    API_KEY: Optional[str] = Field(default=None)
    CORS_ORIGINS: List[str] = Field(default=["*"])

    @validator('CORS_ORIGINS', pre=True)
    def parse_cors_origins(cls, v) -> List[str]:
        """Parse CORS origins from string or list"""
        if isinstance(v, str):
            if not v:  # Handle empty string case
                return []
            return [origin.strip() for origin in v.split(",")]
        return v
    
    
    # Monitoring
    PROMETHEUS_ENABLED: bool = Field(default=True)
    PROMETHEUS_PORT: int = Field(default=9090)
    METRICS_ENDPOINT: str = Field(default="/metrics")
    
    # Logging
    LOG_FORMAT: str = Field(default="json")
    LOG_FILE_PATH: Optional[str] = Field(default=None)
    LOG_MAX_SIZE_MB: int = Field(default=100)
    LOG_BACKUP_COUNT: int = Field(default=5)
    
    @validator('POSTGRES_URL', pre=True)
    def build_postgres_url(cls, v: Optional[str], values: dict) -> str:
        """Build PostgreSQL URL if not provided"""
        if isinstance(v, str):
            return v
        return (
            f"postgresql://{values.get('POSTGRES_USER')}:"
            f"{values.get('POSTGRES_PASSWORD')}@"
            f"{values.get('POSTGRES_HOST')}:"
            f"{values.get('POSTGRES_PORT')}/"
            f"{values.get('POSTGRES_DB')}"
        )
    
    @validator('REDIS_URL', pre=True)
    def build_redis_url(cls, v: Optional[str], values: dict) -> str:
        """Build Redis URL if not provided"""
        if isinstance(v, str):
            return v
        
        password_part = ""
        if values.get('REDIS_PASSWORD'):
            password_part = f":{values.get('REDIS_PASSWORD')}@"
        
        return (
            f"redis://{password_part}"
            f"{values.get('REDIS_HOST')}:"
            f"{values.get('REDIS_PORT')}/"
            f"{values.get('REDIS_DB')}"
        )
    
    
    
    @validator('DATA_SOURCE_TYPE', pre=True)
    def validate_data_source_type(cls, v: str) -> str:
        """Validate data source type"""
        allowed_types = ["bigquery", "postgresql"]
        if v.lower() not in allowed_types:
            raise ValueError(f"DATA_SOURCE_TYPE must be one of: {allowed_types}")
        return v.lower()
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.ENVIRONMENT.lower() in ["development", "dev", "local"]
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.ENVIRONMENT.lower() in ["production", "prod"]
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get settings instance (used by dependency injection)"""
    return settings


# Validate critical settings in production
if settings.is_production:
    assert settings.SECRET_KEY != "dev-secret-key-change-in-production", \
        "SECRET_KEY must be set in production"
    assert settings.GOOGLE_APPLICATION_CREDENTIALS, \
        "GOOGLE_APPLICATION_CREDENTIALS must be set in production"
    assert settings.API_KEY, \
        "API_KEY must be set in production"
