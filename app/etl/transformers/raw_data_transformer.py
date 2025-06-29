"""
🔄 Raw Data Transformers - BigQuery to PostgreSQL Raw Tables
Minimal transformation for staging BigQuery data into PostgreSQL raw tables

APPROACH: Light transformation, preserve original data for business logic layer
PURPOSE: Create staging layer for complex business transformations
"""

from datetime import datetime, date, timezone
from typing import List, Dict, Any, Optional
import re

from app.core.logging import LoggerMixin


class RawDataTransformer(LoggerMixin):
    """
    Raw data transformer for BigQuery → PostgreSQL raw staging tables
    
    PRINCIPLE: Minimal transformation - just type conversion and basic cleaning
    PRESERVE: Original BigQuery data structure for business logic layer
    """
    
    def __init__(self):
        super().__init__()
        self.transformation_stats = {
            'records_processed': 0,
            'records_transformed': 0,
            'records_skipped': 0,
            'validation_errors': 0
        }
    
    def transform_raw_calendario(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery calendario to PostgreSQL raw_calendario
        
        MINIMAL: Type conversion + basic validation only
        """
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                # Validate required primary key
                archivo = self._safe_string(record.get('ARCHIVO'))
                if not archivo:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed = {
                    # Primary key
                    'ARCHIVO': archivo,
                    
                    # Campaign metadata - preserve original names
                    'TIPO_CARTERA': self._safe_string(record.get('TIPO_CARTERA')),
                    
                    # Business dates - critical for campaign logic
                    'fecha_apertura': self._safe_date(record.get('fecha_apertura')),
                    'fecha_trandeuda': self._safe_date(record.get('fecha_trandeuda')),
                    'fecha_cierre': self._safe_date(record.get('fecha_cierre')),
                    'FECHA_CIERRE_PLANIFICADA': self._safe_date(record.get('FECHA_CIERRE_PLANIFICADA')),
                    
                    # Campaign characteristics
                    'DURACION_CAMPANA_DIAS_HABILES': self._safe_int(record.get('DURACION_CAMPANA_DIAS_HABILES')),
                    'ANNO_ASIGNACION': self._safe_int(record.get('ANNO_ASIGNACION')),
                    'PERIODO_ASIGNACION': self._safe_string(record.get('PERIODO_ASIGNACION')),
                    'ES_CARTERA_ABIERTA': self._safe_bool(record.get('ES_CARTERA_ABIERTA')),
                    'RANGO_VENCIMIENTO': self._safe_string(record.get('RANGO_VENCIMIENTO')),
                    'ESTADO_CARTERA': self._safe_string(record.get('ESTADO_CARTERA')),
                    
                    # Time partitioning
                    'periodo_mes': self._safe_string(record.get('periodo_mes')),
                    'periodo_date': self._safe_date(record.get('periodo_date')),
                    
                    # Campaign classification
                    'tipo_ciclo_campana': self._safe_string(record.get('tipo_ciclo_campana')),
                    'categoria_duracion': self._safe_string(record.get('categoria_duracion')),
                    
                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }
                
                # Validate required business date
                if not transformed['fecha_apertura']:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming raw_calendario record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_raw_asignaciones(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery asignaciones to PostgreSQL raw_asignaciones
        
        MINIMAL: Type conversion + key field validation
        """
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                # Validate primary key components
                cod_luna = self._safe_string(record.get('cod_luna'))
                cuenta = self._safe_string(record.get('cuenta'))
                archivo = self._safe_string(record.get('archivo'))
                
                if not cod_luna or not cuenta or not archivo:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed = {
                    # Primary key components
                    'cod_luna': cod_luna,
                    'cuenta': cuenta,
                    'archivo': archivo,
                    
                    # Client information
                    'cliente': self._safe_string(record.get('cliente')),
                    'telefono': self._safe_string(record.get('telefono')),
                    
                    # Business classification
                    'tramo_gestion': self._safe_string(record.get('tramo_gestion')),
                    'negocio': self._safe_string(record.get('negocio')),
                    'dias_sin_trafico': self._safe_string(record.get('dias_sin_trafico')),
                    
                    # Risk scoring
                    'decil_contacto': self._safe_int(record.get('decil_contacto')),
                    'decil_pago': self._safe_int(record.get('decil_pago')),
                    
                    # Account details
                    'min_vto': self._safe_date(record.get('min_vto')),
                    'zona': self._safe_string(record.get('zona')),
                    'rango_renta': self._safe_int(record.get('rango_renta')),
                    'campania_act': self._safe_string(record.get('campania_act')),
                    
                    # Payment arrangement
                    'fraccionamiento': self._safe_string(record.get('fraccionamiento')),
                    'cuota_fracc_act': self._safe_string(record.get('cuota_fracc_act')),
                    'fecha_corte': self._safe_date(record.get('fecha_corte')),
                    'priorizado': self._safe_string(record.get('priorizado')),
                    'inscripcion': self._safe_string(record.get('inscripcion')),
                    'incrementa_velocidad': self._safe_string(record.get('incrementa_velocidad')),
                    'detalle_dscto_futuro': self._safe_string(record.get('detalle_dscto_futuro')),
                    'cargo_fijo': self._safe_string(record.get('cargo_fijo')),
                    
                    # Client identification
                    'dni': self._safe_string(record.get('dni')),
                    'estado_pc': self._safe_string(record.get('estado_pc')),
                    'tipo_linea': self._safe_string(record.get('tipo_linea')),
                    'cod_sistema': self._safe_int(record.get('cod_sistema')),
                    'tipo_alta': self._safe_string(record.get('tipo_alta')),
                    
                    # Technical metadata
                    'creado_el': self._safe_datetime(record.get('creado_el')),
                    'fecha_asignacion': self._safe_date(record.get('fecha_asignacion')),
                    'motivo_rechazo': self._safe_string(record.get('motivo_rechazo')),
                    
                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming raw_asignaciones record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_raw_trandeuda(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery trandeuda to PostgreSQL raw_trandeuda
        
        MINIMAL: Type conversion + debt validation
        """
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                # Validate primary key components
                cod_cuenta = self._safe_string(record.get('cod_cuenta'))
                nro_documento = self._safe_string(record.get('nro_documento'))
                archivo = self._safe_string(record.get('archivo'))
                monto_exigible = self._safe_decimal(record.get('monto_exigible'))
                
                if not cod_cuenta or not nro_documento or not archivo:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                # Skip zero debt records
                if monto_exigible <= 0:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed = {
                    # Primary key components
                    'cod_cuenta': cod_cuenta,
                    'nro_documento': nro_documento,
                    'archivo': archivo,
                    
                    # Debt information
                    'fecha_vencimiento': self._safe_date(record.get('fecha_vencimiento')),
                    'monto_exigible': monto_exigible,
                    
                    # Technical metadata
                    'creado_el': self._safe_datetime(record.get('creado_el')),
                    'fecha_proceso': self._safe_date(record.get('fecha_proceso')),
                    'motivo_rechazo': self._safe_string(record.get('motivo_rechazo')),
                    
                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming raw_trandeuda record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_raw_pagos(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery pagos to PostgreSQL raw_pagos
        
        MINIMAL: Type conversion + payment validation  
        """
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                # Validate primary key components (for deduplication)
                nro_documento = self._safe_string(record.get('nro_documento'))
                fecha_pago = self._safe_date(record.get('fecha_pago'))
                monto_cancelado = self._safe_decimal(record.get('monto_cancelado'))
                
                if not nro_documento or not fecha_pago or monto_cancelado <= 0:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed = {
                    # Primary key components (deduplication key)
                    'nro_documento': nro_documento,
                    'fecha_pago': fecha_pago,
                    'monto_cancelado': monto_cancelado,
                    
                    # System identification
                    'cod_sistema': self._safe_string(record.get('cod_sistema')),
                    'archivo': self._safe_string(record.get('archivo')),
                    
                    # Technical metadata
                    'creado_el': self._safe_datetime(record.get('creado_el')),
                    'motivo_rechazo': self._safe_string(record.get('motivo_rechazo')),
                    
                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming raw_pagos record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_gestiones_unificadas(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery unified gestiones to PostgreSQL gestiones_unificadas
        
        PRESERVE: Homologated business flags for KPI calculation
        """
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                # Validate primary key components
                cod_luna = self._safe_string(record.get('cod_luna'))
                timestamp_gestion = self._safe_datetime(record.get('timestamp_gestion'))
                
                if not cod_luna or not timestamp_gestion:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed = {
                    # Primary key components
                    'cod_luna': cod_luna,
                    'timestamp_gestion': timestamp_gestion,
                    'fecha_gestion': self._safe_date(record.get('fecha_gestion')) or timestamp_gestion.date(),
                    
                    # Channel information
                    'canal_origen': self._standardize_canal(record.get('canal_origen')),
                    
                    # Original management data
                    'management_original': self._safe_string(record.get('management_original')),
                    'sub_management_original': self._safe_string(record.get('sub_management_original')),
                    'compromiso_original': self._safe_string(record.get('compromiso_original')),
                    
                    # Homologated classification (CRITICAL FOR KPIs)
                    'nivel_1': self._safe_string(record.get('nivel_1')),
                    'nivel_2': self._safe_string(record.get('nivel_2')),
                    'contactabilidad': self._safe_string(record.get('contactabilidad')),
                    
                    # Business flags for KPI calculation
                    'es_contacto_efectivo': self._safe_bool(record.get('es_contacto_efectivo')),
                    'es_contacto_no_efectivo': self._safe_bool(record.get('es_contacto_no_efectivo')),
                    'es_compromiso': self._safe_bool(record.get('es_compromiso')),
                    
                    # Weighting for business logic
                    'peso_gestion': self._safe_int(record.get('peso_gestion'), default=1),
                    
                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming gestiones_unificadas record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    # =============================================================================
    # HELPER METHODS - Safe Type Conversion
    # =============================================================================
    
    def _safe_string(self, value: Any, max_length: int = None) -> Optional[str]:
        """Safe string conversion with length validation"""
        if value is None or value == '':
            return None
        
        try:
            result = str(value).strip()
            if max_length and len(result) > max_length:
                result = result[:max_length]
            return result if result else None
        except:
            return None
    
    def _safe_int(self, value: Any, default: Optional[int] = None) -> Optional[int]:
        """Safe integer conversion"""
        if value is None or value == '':
            return default
        
        try:
            if isinstance(value, str):
                # Remove non-numeric characters except minus
                cleaned = re.sub(r'[^\d\-]', '', value)
                return int(cleaned) if cleaned else default
            return int(float(value))
        except:
            return default
    
    def _safe_decimal(self, value: Any, default: float = 0.0) -> float:
        """Safe decimal conversion"""
        if value is None or value == '':
            return default
        
        try:
            return float(value)
        except:
            return default
    
    def _safe_bool(self, value: Any) -> bool:
        """Safe boolean conversion"""
        if value is None:
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'si', 'sí')
        
        try:
            return bool(int(value))
        except:
            return False
    
    def _safe_date(self, value: Any) -> Optional[date]:
        """Safe date conversion"""
        if value is None:
            return None
        
        if isinstance(value, date):
            return value
        
        if isinstance(value, datetime):
            return value.date()
        
        if isinstance(value, str):
            try:
                # Try ISO format
                return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
            except:
                try:
                    # Try common format
                    return datetime.strptime(value, '%Y-%m-%d').date()
                except:
                    return None
        
        return None
    
    def _safe_datetime(self, value: Any) -> Optional[datetime]:
        """Safe datetime conversion"""
        if value is None:
            return None
        
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                return None
        
        return None
    
    def _standardize_canal(self, value: Any) -> str:
        """Standardize channel values"""
        canal = self._safe_string(value, max_length=20)
        if not canal:
            return 'BOT'  # Default
        
        canal_upper = canal.upper()
        if canal_upper in ['BOT', 'VOICEBOT']:
            return 'BOT'
        elif canal_upper in ['HUMANO', 'HUMAN', 'CALL_CENTER', 'CALL CENTER']:
            return 'HUMANO'
        else:
            return 'BOT'  # Default fallback
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """Get transformation statistics"""
        return self.transformation_stats.copy()
    
    def reset_stats(self):
        """Reset transformation statistics"""
        self.transformation_stats = {
            'records_processed': 0,
            'records_transformed': 0,
            'records_skipped': 0,
            'validation_errors': 0
        }


# =============================================================================
# RAW TABLE TRANSFORMER REGISTRY  
# =============================================================================

class RawTransformerRegistry:
    """Registry for raw table transformers"""
    
    def __init__(self):
        self.transformer = RawDataTransformer()
        
        # Map raw table names to transformation methods
        self.raw_transformer_mapping = {
            'raw_calendario': self.transformer.transform_raw_calendario,
            'raw_asignaciones': self.transformer.transform_raw_asignaciones,
            'raw_trandeuda': self.transformer.transform_raw_trandeuda,
            'raw_pagos': self.transformer.transform_raw_pagos,
            'gestiones_unificadas': self.transformer.transform_gestiones_unificadas
        }
    
    def transform_raw_table_data(self, table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for a specific raw table"""
        if table_name not in self.raw_transformer_mapping:
            raise ValueError(f"No raw transformer found for table: {table_name}")
        
        transformer_func = self.raw_transformer_mapping[table_name]
        return transformer_func(raw_data)
    
    def get_supported_raw_tables(self) -> List[str]:
        """Get list of supported raw table transformations"""
        return list(self.raw_transformer_mapping.keys())
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """Get transformation statistics"""
        return self.transformer.get_transformation_stats()


# 🎯 Global raw transformer registry instance
_raw_transformer_registry: Optional[RawTransformerRegistry] = None

def get_raw_transformer_registry() -> RawTransformerRegistry:
    """Get singleton raw transformer registry instance"""
    global _raw_transformer_registry
    
    if _raw_transformer_registry is None:
        _raw_transformer_registry = RawTransformerRegistry()
    
    return _raw_transformer_registry
