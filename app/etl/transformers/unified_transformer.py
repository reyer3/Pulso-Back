"""
🔄 Unified Transformer Registry - Complete 3-Layer Pipeline - CIRCULAR IMPORT FIXED
Connects all transformers: Raw → Business Logic → Mart Tables

ARCHITECTURE:
1. RawDataTransformer: BigQuery → Raw Tables (staging)
2. BusinessLogicTransformer: Campaign logic + deduplication  
3. DataTransformer: Final KPIs → Mart Tables

FIXED: Removed circular import completely - all transformers defined locally
"""

from typing import List, Dict, Any, Optional, AsyncGenerator
from app.etl.transformers.raw_data_transformer import get_raw_transformer_registry, RawTransformerRegistry
from app.etl.transformers.business_logic_transformer import get_business_transformer, BusinessLogicTransformer
from app.core.logging import LoggerMixin


# ✅ FIXED: Define mart transformers locally to break circular import
class DataTransformer:
    """
    Basic data transformer for mart tables
    """
    
    def __init__(self):
        self.stats = {
            'records_processed': 0,
            'records_transformed': 0,
            'transformation_errors': 0
        }
    
    def transform_dashboard_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for dashboard_data table"""
        transformed = []
        for record in raw_data:
            transformed_record = {
                **record,
                'processed_at': record.get('extraction_timestamp'),
                'source': 'unified_transformer'
            }
            transformed.append(transformed_record)
        
        self.stats['records_processed'] += len(raw_data)
        self.stats['records_transformed'] += len(transformed)
        return transformed
    
    def transform_evolution_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for evolution_data table"""
        return self.transform_dashboard_data(raw_data)
    
    def transform_assignment_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for assignment_data table"""
        return self.transform_dashboard_data(raw_data)
    
    def transform_operation_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for operation_data table"""
        return self.transform_dashboard_data(raw_data)
    
    def transform_productivity_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for productivity_data table"""
        return self.transform_dashboard_data(raw_data)
    
    def get_transformation_stats(self) -> Dict[str, int]:
        """Get transformation statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset transformation statistics"""
        self.stats = {
            'records_processed': 0,
            'records_transformed': 0,
            'transformation_errors': 0
        }


class MartTransformerRegistry:
    """
    Registry for mart table transformers
    """
    
    def __init__(self):
        self.transformer = DataTransformer()
        self.supported_mart_tables = [
            'dashboard_data',
            'evolution_data', 
            'assignment_data',
            'operation_data',
            'productivity_data'
        ]
    
    def transform_table_data(self, table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for a specific mart table"""
        
        if table_name == 'dashboard_data':
            return self.transformer.transform_dashboard_data(raw_data)
        elif table_name == 'evolution_data':
            return self.transformer.transform_evolution_data(raw_data)
        elif table_name == 'assignment_data':
            return self.transformer.transform_assignment_data(raw_data)
        elif table_name == 'operation_data':
            return self.transformer.transform_operation_data(raw_data)
        elif table_name == 'productivity_data':
            return self.transformer.transform_productivity_data(raw_data)
        else:
            raise ValueError(f"No mart transformer found for table: {table_name}")
    
    def get_supported_tables(self) -> List[str]:
        """Get list of supported mart tables"""
        return self.supported_mart_tables.copy()
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """Get transformation statistics"""
        return {
            'mart_stats': self.transformer.get_transformation_stats(),
            'supported_tables': len(self.supported_mart_tables)
        }


class UnifiedTransformerRegistry(LoggerMixin):
    """
    Unified transformer that handles all 3 pipeline layers:
    - Raw staging tables (minimal transformation)
    - Business logic processing (complex deduplication)  
    - Mart tables (final dashboard consumption)
    """
    
    def __init__(self):
        super().__init__()
        
        # Initialize all transformer layers - NO CIRCULAR IMPORTS
        self.raw_transformer: RawTransformerRegistry = get_raw_transformer_registry()
        self.business_transformer: BusinessLogicTransformer = get_business_transformer()
        self.mart_transformer: MartTransformerRegistry = MartTransformerRegistry()  # ✅ Local class
        
        # Combined mapping of all supported tables
        self.all_transformers = {
            # Raw tables (BigQuery → PostgreSQL staging)
            **{table: 'raw' for table in self.raw_transformer.get_supported_raw_tables()},
            
            # Mart tables (Staging → Final dashboard tables)
            **{table: 'mart' for table in self.mart_transformer.get_supported_tables()}
        }
        
        self.logger.info(f"Unified transformer initialized with {len(self.all_transformers)} table mappings")
    
    def transform_table_data(self, table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform data for any table using the appropriate transformer layer
        
        Args:
            table_name: Name of the target table
            raw_data: Raw data from BigQuery
            
        Returns:
            Transformed data ready for PostgreSQL
        """
        
        if table_name not in self.all_transformers:
            raise ValueError(f"No transformer found for table: {table_name}")
        
        transformer_type = self.all_transformers[table_name]
        
        try:
            if transformer_type == 'raw':
                # Raw table transformation (minimal)
                self.logger.debug(f"Using RawDataTransformer for {table_name}")
                return self.raw_transformer.transform_raw_table_data(table_name, raw_data)
                
            elif transformer_type == 'mart':
                # Mart table transformation (final KPIs)
                self.logger.debug(f"Using DataTransformer for {table_name}")
                return self.mart_transformer.transform_table_data(table_name, raw_data)
                
            else:
                raise ValueError(f"Unknown transformer type: {transformer_type}")
                
        except Exception as e:
            self.logger.error(f"Transformation failed for {table_name}: {str(e)}")
            raise
    
    def get_supported_tables(self) -> List[str]:
        """Get all supported table names across all transformer layers"""
        return list(self.all_transformers.keys())
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """Get combined transformation statistics from all layers"""
        return {
            'raw_stats': self.raw_transformer.get_transformation_stats(),
            'mart_stats': self.mart_transformer.get_transformation_stats(),
            'business_stats': self.business_transformer.get_processing_stats(),
            'supported_tables': len(self.all_transformers),
            'raw_tables': len([t for t, type in self.all_transformers.items() if type == 'raw']),
            'mart_tables': len([t for t, type in self.all_transformers.items() if type == 'mart'])
        }
    
    def reset_all_stats(self):
        """Reset statistics across all transformer layers"""
        self.raw_transformer.transformer.reset_stats()
        self.mart_transformer.transformer.reset_stats()
        self.business_transformer.reset_stats()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a specific table and its transformer"""
        if table_name not in self.all_transformers:
            return {"error": f"Table {table_name} not supported"}
        
        transformer_type = self.all_transformers[table_name]
        
        return {
            "table_name": table_name,
            "transformer_type": transformer_type,
            "transformer_layer": {
                'raw': 'BigQuery → Raw Tables (staging)',
                'mart': 'Raw/Auxiliary → Mart Tables (dashboard)'
            }.get(transformer_type, 'unknown'),
            "supported": True
        }
    
    async def process_campaign_business_logic(self, archivo: str) -> Dict[str, Any]:
        """
        Process complete business logic for a campaign
        
        This method orchestrates the complex business logic transformation:
        1. Build cuenta_campana_state
        2. Map gestiones to accounts  
        3. Deduplicate payments
        4. Calculate accurate KPIs
        
        Args:
            archivo: Campaign identifier
            
        Returns:
            Processing result with statistics
        """
        self.logger.info(f"🎯 Starting business logic processing for campaign: {archivo}")
        
        try:
            # Process complete campaign window with business logic
            result = await self.business_transformer.process_campaign_window(archivo)
            
            # Calculate final KPIs using auxiliary tables
            kpis = await self.business_transformer.calculate_campaign_kpis(archivo)
            
            # Combine results
            result['calculated_kpis'] = kpis
            result['processing_stats'] = self.business_transformer.get_processing_stats()
            
            self.logger.info(f"✅ Business logic completed for {archivo}: {result}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Business logic failed for {archivo}: {str(e)}")
            raise
    
    def supports_business_logic(self) -> bool:
        """Check if business logic transformer is available"""
        return self.business_transformer is not None
    
    def get_business_logic_status(self) -> Dict[str, Any]:
        """Get status of business logic transformer"""
        return {
            'available': self.supports_business_logic(),
            'stats': self.business_transformer.get_processing_stats() if self.supports_business_logic() else {},
            'description': 'Campaign window deduplication and KPI calculation'
        }

        # En: app/etl/transformers/unified_transformer.py
        # Dentro de la clase UnifiedTransformerRegistry

    async def transform_stream(
            self,
            table_name: str,
            data_stream: AsyncGenerator[List[Dict[str, Any]], None]
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Transforms a stream of data batches.

        Takes an async generator of raw data batches and yields
        an async generator of transformed data batches. This is the
        memory-efficient way to handle transformations.

        Args:
            table_name: The name of the target table.
            data_stream: An async generator yielding batches of raw data.

        Yields:
            A batch of transformed data.
        """
        self.logger.debug(f"Initiating stream transformation for {table_name}...")
        self.reset_all_stats()  # Reset stats before starting a new stream

        async for raw_batch in data_stream:
            if not raw_batch:
                continue

            # Reuse the existing batch transformation logic
            # This is efficient as it operates on one batch at a time
            transformed_batch = self.transform_table_data(table_name, raw_batch)

            if transformed_batch:
                yield transformed_batch


# 🎯 Global unified transformer instance
_unified_transformer: Optional[UnifiedTransformerRegistry] = None

def get_unified_transformer_registry() -> UnifiedTransformerRegistry:
    """Get singleton unified transformer registry instance - NO CIRCULAR IMPORTS"""
    global _unified_transformer
    
    if _unified_transformer is None:
        # ✅ FIXED: No import from data_transformer - create directly
        _unified_transformer = UnifiedTransformerRegistry()
    
    return _unified_transformer


# 🔄 Update the main transformer registry to use unified version
def get_transformer_registry() -> UnifiedTransformerRegistry:
    """
    Get transformer registry - now returns unified version
    
    COMPATIBILITY: This maintains the same interface as before
    but now supports all table types (raw + mart)
    """
    return get_unified_transformer_registry()


# 🚀 Convenience functions for specific transformer layers
def transform_raw_data(table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw BigQuery data for staging tables"""
    registry = get_unified_transformer_registry()
    return registry.transform_table_data(table_name, raw_data)

def transform_mart_data(table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform data for final mart tables"""
    registry = get_unified_transformer_registry()
    return registry.transform_table_data(table_name, raw_data)

async def process_campaign_window(archivo: str) -> Dict[str, Any]:
    """Process complete business logic for a campaign window"""
    registry = get_unified_transformer_registry()
    return await registry.process_campaign_business_logic(archivo)
