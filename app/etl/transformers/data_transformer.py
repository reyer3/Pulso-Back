"""
🔄 Data Transformer - COMPATIBILITY LAYER - CIRCULAR IMPORT FIXED
Maintains compatibility while redirecting to UnifiedTransformerRegistry

UPDATED: Removed circular import by defining get_transformer_registry locally
COMPATIBILITY: Existing code continues to work without changes
"""

# Import only what we need to avoid circular imports
from app.etl.transformers.unified_transformer import (
    get_unified_transformer_registry,
    UnifiedTransformerRegistry,
    transform_raw_data,
    transform_mart_data,
    process_campaign_window
)

# ✅ FIXED: Define get_transformer_registry locally (no circular import)
def get_transformer_registry():
    """Get transformer registry - now returns unified version"""
    return get_unified_transformer_registry()

# Legacy DataTransformer class for backwards compatibility
class DataTransformer:
    """
    DEPRECATED: Legacy DataTransformer class
    Use UnifiedTransformerRegistry instead for new code
    """
    
    def __init__(self):
        self._unified = get_unified_transformer_registry()
    
    def transform_dashboard_data(self, raw_data):
        return self._unified.transform_table_data('dashboard_data', raw_data)
    
    def transform_evolution_data(self, raw_data):
        return self._unified.transform_table_data('evolution_data', raw_data)
    
    def transform_assignment_data(self, raw_data):
        return self._unified.transform_table_data('assignment_data', raw_data)
    
    def transform_operation_data(self, raw_data):
        return self._unified.transform_table_data('operation_data', raw_data)
    
    def transform_productivity_data(self, raw_data):
        return self._unified.transform_table_data('productivity_data', raw_data)
    
    def get_transformation_stats(self):
        return self._unified.get_transformation_stats()
    
    def reset_stats(self):
        return self._unified.reset_all_stats()


# Legacy TransformerRegistry class for backwards compatibility
class TransformerRegistry:
    """
    DEPRECATED: Legacy TransformerRegistry class
    Use UnifiedTransformerRegistry instead for new code
    """
    
    def __init__(self):
        self._unified = get_unified_transformer_registry()
        self.transformer = DataTransformer()  # For legacy compatibility
    
    def transform_table_data(self, table_name, raw_data):
        return self._unified.transform_table_data(table_name, raw_data)
    
    def get_supported_tables(self):
        return self._unified.get_supported_tables()
    
    def get_transformation_stats(self):
        return self._unified.get_transformation_stats()


# Legacy transform_data function for backwards compatibility
def transform_data(table_name, raw_data):
    """Transform raw BigQuery data for a specific table"""
    registry = get_unified_transformer_registry()
    return registry.transform_table_data(table_name, raw_data)
