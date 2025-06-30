"""
📋 Structured logging configuration
Centralized logging setup with JSON formatting
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any, Dict

import structlog
from structlog.processors import JSONRenderer

from shared.core.config import settings


def setup_logging() -> None:
    """
    Configure structured logging with structlog
    """
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if settings.LOG_FORMAT != "json" else JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.LOG_LEVEL.upper()),
    )
    
    # File logging if configured
    if settings.LOG_FILE_PATH:
        setup_file_logging()
    
    # Silence noisy loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)


def setup_file_logging() -> None:
    """
    Setup rotating file handler for logs
    """
    if not settings.LOG_FILE_PATH:
        return
    
    log_file = Path(settings.LOG_FILE_PATH)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Rotating file handler
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=settings.LOG_MAX_SIZE_MB * 1024 * 1024,
        backupCount=settings.LOG_BACKUP_COUNT,
    )
    
    if settings.LOG_FORMAT == "json":
        handler.setFormatter(logging.Formatter("%(message)s"))
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )
    
    # Add handler to root logger
    logging.getLogger().addHandler(handler)


class LoggerMixin:
    """
    Mixin to add structured logger to classes
    """
    
    @property
    def logger(self):
        """Get structured logger for this class"""
        return structlog.get_logger(self.__class__.__name__)
    
    def log_context(self, **kwargs: Any) -> Dict[str, Any]:
        """Create logging context"""
        return {
            "class": self.__class__.__name__,
            **kwargs
        }


def get_logger(name: str = None) -> structlog.BoundLogger:
    """
    Get structured logger instance
    """
    return structlog.get_logger(name)


# Request ID context
request_context = structlog.contextvars


def log_request_id(request_id: str) -> None:
    """
    Add request ID to logging context
    """
    request_context.bind_contextvars(request_id=request_id)


def log_user_id(user_id: str) -> None:
    """
    Add user ID to logging context
    """
    request_context.bind_contextvars(user_id=user_id)