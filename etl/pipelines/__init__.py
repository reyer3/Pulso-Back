# etl/pipelines/__init__.py

"""
🚀 ETL Pipelines Module

Disponibles:
- RawDataPipeline: Pipeline original para datos raw
- HybridRawDataPipeline: Pipeline híbrido calendario + watermarks
- CampaignCatchUpPipeline: Pipeline de catch-up para campañas
"""

from .raw_data_pipeline import HybridRawDataPipeline, ExtractionStrategy
from .campaign_catchup_pipeline import CampaignCatchUpPipeline

__all__ = [
    'HybridRawDataPipeline',
    'ExtractionStrategy',
    'CampaignCatchUpPipeline'
]