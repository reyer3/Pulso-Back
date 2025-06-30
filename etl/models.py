# etl/models.py

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, List, Dict, Any


@dataclass
class CampaignWindow:
    """Representa una ventana de campaña con sus fechas y metadatos."""
    archivo: str
    fecha_apertura: date
    fecha_cierre: Optional[date]
    tipo_cartera: str
    estado_cartera: str

    @property
    def is_active(self) -> bool:
        """Verifica si la campaña está activa."""
        return self.estado_cartera == 'ABIERTA'

    @property
    def duration_days(self) -> Optional[int]:
        """Calcula la duración de la campaña en días."""
        if self.fecha_cierre:
            return (self.fecha_cierre - self.fecha_apertura).days
        return None


@dataclass
class CampaignLoadResult:
    """Resultado del procesamiento de una campaña individual."""
    archivo: str
    status: str  # success, failed, partial
    duration_seconds: float
    errors: List[str]
    tables_loaded: Dict[str, int]  # tabla -> número de registros cargados
    raw_records_total: int = 0
    mart_records_total: int = 0

    @property
    def is_success(self) -> bool:
        return self.status == "success"

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0


@dataclass
class TableLoadResult:
    """Resultado de la carga de una tabla específica."""
    table_name: str
    records_processed: int
    records_loaded: int
    duration_seconds: float
    status: str  # success, failed, partial
    error_message: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Porcentaje de éxito en la carga."""
        if self.records_processed == 0:
            return 100.0 if self.status == "success" else 0.0
        return (self.records_loaded / self.records_processed) * 100


@dataclass
class PipelineExecutionSummary:
    """Resumen completo de la ejecución del pipeline."""
    start_time: datetime
    end_time: Optional[datetime]
    total_campaigns: int
    campaigns_processed: int
    campaigns_successful: int
    campaigns_failed: int
    total_duration_seconds: float
    campaigns_per_minute: float
    error_details: List[Dict[str, Any]]

    @property
    def success_rate(self) -> float:
        """Porcentaje de campañas procesadas exitosamente."""
        if self.campaigns_processed == 0:
            return 0.0
        return (self.campaigns_successful / self.campaigns_processed) * 100