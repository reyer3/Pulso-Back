"""
🏭 Mart Transformer Factory
DRY: Reutiliza patrón de RawTransformerRegistry existente
SRP: Solo responsable de crear y registrar transformers
"""

from typing import Dict, Type, List, Optional

from etl.transformers.mart_transformer_base import MartTransformerBase
from etl.transformers.dashboard_data_transformer import DashboardDataTransformer


class MartTransformerRegistry:
    """
    Registry para mart transformers siguiendo patrón de RawTransformerRegistry
    """
    
    def __init__(self, project_uid: str = None):
        self.project_uid = project_uid
        
        # Map mart table names to transformer classes
        self._transformer_mapping: Dict[str, Type[MartTransformerBase]] = {
            'dashboard_data': DashboardDataTransformer,
            # Agregar más transformers aquí cuando se implementen:
            # 'evolution_data': EvolutionDataTransformer,
            # 'productivity_data': ProductivityDataTransformer,
            # 'operation_data': OperationDataTransformer,
            # 'assignment_data': AssignmentDataTransformer,
        }
    
    def create_transformer(self, mart_type: str) -> MartTransformerBase:
        """
        Crea transformer específico para tipo de mart
        
        Args:
            mart_type: Nombre de la tabla mart (ej: 'dashboard_data')
            
        Returns:
            Instancia del transformer correspondiente
            
        Raises:
            ValueError: Si no existe transformer para el tipo especificado
        """
        if mart_type not in self._transformer_mapping:
            available = list(self._transformer_mapping.keys())
            raise ValueError(f"No transformer found for mart type: {mart_type}. Available: {available}")
        
        transformer_class = self._transformer_mapping[mart_type]
        return transformer_class(project_uid=self.project_uid)
    
    def get_supported_marts(self) -> List[str]:
        """Retorna lista de marts soportados"""
        return list(self._transformer_mapping.keys())
    
    def has_transformer(self, mart_type: str) -> bool:
        """Verifica si existe transformer para el tipo de mart"""
        return mart_type in self._transformer_mapping


# Singleton pattern siguiendo el patrón de raw_data_transformer.py
_mart_transformer_registry: Optional[MartTransformerRegistry] = None


def get_mart_transformer_registry(project_uid: str = None) -> MartTransformerRegistry:
    """
    Get singleton mart transformer registry instance
    Sigue patrón de get_raw_transformer_registry()
    """
    global _mart_transformer_registry
    
    if _mart_transformer_registry is None:
        _mart_transformer_registry = MartTransformerRegistry(project_uid=project_uid)
    
    return _mart_transformer_registry


class MartTransformerFactory:
    """
    Factory class para crear transformers
    KISS: Simple factory pattern
    """
    
    @staticmethod
    def create_transformer(mart_type: str, project_uid: str = None) -> MartTransformerBase:
        """
        Factory method para crear transformer
        
        Args:
            mart_type: Tipo de tabla mart
            project_uid: UID del proyecto (opcional)
            
        Returns:
            Transformer instance
        """
        registry = get_mart_transformer_registry(project_uid)
        return registry.create_transformer(mart_type)
    
    @staticmethod
    def get_available_transformers() -> List[str]:
        """Retorna lista de transformers disponibles"""
        registry = get_mart_transformer_registry()
        return registry.get_supported_marts()
