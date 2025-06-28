"""
🔄 Data Transformer - BigQuery to PostgreSQL/TimescaleDB
Production transformer that converts BigQuery raw data to PostgreSQL format

Features:
- Type conversion and validation
- Business logic application
- Data quality checks and cleaning
- Schema mapping between BigQuery views and PostgreSQL models
"""

import re
from datetime import datetime, date, timezone
from typing import List, Dict, Any, Optional, Union, Callable
from decimal import Decimal
import logging

from app.core.logging import LoggerMixin


class DataTransformer(LoggerMixin):
    """
    Production data transformer for BigQuery → PostgreSQL ETL
    
    Handles:
    - Type conversion (BigQuery types → PostgreSQL types)
    - Data validation and cleaning
    - Business logic application
    - Schema mapping and field renaming
    """
    
    def __init__(self):
        super().__init__()
        self.validation_errors = []
        self.transformation_stats = {
            'records_processed': 0,
            'records_transformed': 0,
            'records_skipped': 0,
            'validation_errors': 0
        }
    
    def transform_dashboard_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery dashboard data to PostgreSQL dashboard_data format
        
        Maps from: BigQuery view bi_P3fV4dWNeMkN5RJMhV8e_vw_dashboard_cobranzas
        To: PostgreSQL dashboard_data table (DashboardDataModel)
        """
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                # Clean and validate required fields
                if not self._validate_dashboard_record(record):
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                # Transform record
                transformed = {
                    # Primary key fields
                    'fecha_foto': self._parse_date(record.get('fecha_foto')),
                    'archivo': self._clean_string(record.get('archivo', ''), max_length=100),
                    'cartera': self._standardize_cartera(record.get('cartera', '')),
                    'servicio': self._standardize_servicio(record.get('servicio', '')),
                    
                    # Volume metrics
                    'cuentas': self._parse_int(record.get('cuentas', 0)),
                    'clientes': self._parse_int(record.get('clientes', 0)),
                    'deuda_asig': self._parse_float(record.get('deuda_asig', 0.0)),
                    'deuda_act': self._parse_float(record.get('deuda_act', 0.0)),
                    
                    # Management metrics
                    'cuentas_gestionadas': self._parse_int(record.get('cuentas_gestionadas', 0)),
                    'cuentas_cd': self._parse_int(record.get('cuentas_cd', 0)),
                    'cuentas_ci': self._parse_int(record.get('cuentas_ci', 0)),
                    'cuentas_sc': self._parse_int(record.get('cuentas_sc', 0)),
                    'cuentas_sg': self._parse_int(record.get('cuentas_sg', 0)),
                    'cuentas_pdp': self._parse_int(record.get('cuentas_pdp', 0)),
                    
                    # Recovery metrics
                    'recupero': self._parse_float(record.get('recupero', 0.0)),
                    
                    # KPI percentages (convert to 0-100 scale if needed)
                    'pct_cober': self._normalize_percentage(record.get('pct_cober', 0.0)),
                    'pct_contac': self._normalize_percentage(record.get('pct_contac', 0.0)),
                    'pct_cd': self._normalize_percentage(record.get('pct_cd', 0.0)),
                    'pct_ci': self._normalize_percentage(record.get('pct_ci', 0.0)),
                    'pct_conversion': self._normalize_percentage(record.get('pct_conversion', 0.0)),
                    'pct_efectividad': self._normalize_percentage(record.get('pct_efectividad', 0.0)),
                    'pct_cierre': self._normalize_percentage(record.get('pct_cierre', 0.0)),
                    'inten': self._parse_float(record.get('inten', 0.0)),
                    
                    # Metadata
                    'fecha_procesamiento': self._parse_datetime(record.get('fecha_procesamiento')) or datetime.now(timezone.utc)
                }
                
                # Apply business rules
                transformed = self._apply_dashboard_business_rules(transformed)
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming dashboard record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_evolution_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform BigQuery evolution data to PostgreSQL format"""
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                transformed = {
                    # Primary key
                    'fecha_foto': self._parse_date(record.get('fecha_foto')),
                    'archivo': self._clean_string(record.get('archivo', ''), max_length=100),
                    
                    # Dimensions
                    'cartera': self._standardize_cartera(record.get('cartera', '')),
                    'servicio': self._standardize_servicio(record.get('servicio', '')),
                    
                    # Evolution metrics
                    'pct_cober': self._normalize_percentage(record.get('pct_cober', 0.0)),
                    'pct_contac': self._normalize_percentage(record.get('pct_contac', 0.0)),
                    'pct_efectividad': self._normalize_percentage(record.get('pct_efectividad', 0.0)),
                    'pct_cierre': self._normalize_percentage(record.get('pct_cierre', 0.0)),
                    'recupero': self._parse_float(record.get('recupero', 0.0)),
                    'cuentas': self._parse_int(record.get('cuentas', 0)),
                    
                    # Metadata
                    'fecha_procesamiento': self._parse_datetime(record.get('fecha_procesamiento')) or datetime.now(timezone.utc)
                }
                
                # Validation
                if not transformed['fecha_foto'] or not transformed['archivo']:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming evolution record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_assignment_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform BigQuery assignment data to PostgreSQL format"""
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                transformed = {
                    # Primary key
                    'periodo': self._standardize_periodo(record.get('periodo', '')),
                    'archivo': self._clean_string(record.get('archivo', ''), max_length=100),
                    'cartera': self._standardize_cartera(record.get('cartera', '')),
                    
                    # Volume metrics
                    'clientes': self._parse_int(record.get('clientes', 0)),
                    'cuentas': self._parse_int(record.get('cuentas', 0)),
                    'deuda_asig': self._parse_float(record.get('deuda_asig', 0.0)),
                    'deuda_actual': self._parse_float(record.get('deuda_actual', 0.0)),
                    
                    # Calculated fields
                    'ticket_promedio': self._calculate_ticket_promedio(
                        record.get('deuda_asig', 0.0),
                        record.get('cuentas', 0)
                    ),
                    
                    # Metadata
                    'fecha_procesamiento': self._parse_datetime(record.get('fecha_procesamiento')) or datetime.now(timezone.utc)
                }
                
                # Validation
                if not transformed['periodo'] or not transformed['archivo']:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming assignment record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_operation_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform BigQuery operation data to PostgreSQL format"""
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                transformed = {
                    # Primary key
                    'fecha_foto': self._parse_date(record.get('fecha_foto')),
                    'hora': self._parse_int(record.get('hora', 0)),
                    'canal': self._standardize_canal(record.get('canal', '')),
                    'archivo': self._clean_string(record.get('archivo', 'GENERAL'), max_length=100),
                    
                    # Operation metrics
                    'total_gestiones': self._parse_int(record.get('total_gestiones', 0)),
                    'contactos_efectivos': self._parse_int(record.get('contactos_efectivos', 0)),
                    'contactos_no_efectivos': self._parse_int(record.get('contactos_no_efectivos', 0)),
                    'total_pdp': self._parse_int(record.get('total_pdp', 0)),
                    
                    # Calculated rates
                    'tasa_contacto': self._calculate_contact_rate(
                        record.get('contactos_efectivos', 0),
                        record.get('total_gestiones', 0)
                    ),
                    'tasa_conversion': self._calculate_conversion_rate(
                        record.get('total_pdp', 0),
                        record.get('contactos_efectivos', 0)
                    ),
                    
                    # Metadata
                    'fecha_procesamiento': self._parse_datetime(record.get('fecha_procesamiento')) or datetime.now(timezone.utc)
                }
                
                # Validation
                if not transformed['fecha_foto'] or transformed['hora'] < 0 or transformed['hora'] > 23:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming operation record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    def transform_productivity_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform BigQuery productivity data to PostgreSQL format"""
        transformed_records = []
        
        for record in raw_data:
            try:
                self.transformation_stats['records_processed'] += 1
                
                correo_agente = self._clean_email(record.get('correo_agente', ''))
                total_gestiones = self._parse_int(record.get('total_gestiones', 0))
                contactos_efectivos = self._parse_int(record.get('contactos_efectivos', 0))
                total_pdp = self._parse_int(record.get('total_pdp', 0))
                
                transformed = {
                    # Primary key
                    'fecha_foto': self._parse_date(record.get('fecha_foto')),
                    'correo_agente': correo_agente,
                    'archivo': self._clean_string(record.get('archivo', 'GENERAL'), max_length=100),
                    
                    # Performance metrics
                    'total_gestiones': total_gestiones,
                    'contactos_efectivos': contactos_efectivos,
                    'total_pdp': total_pdp,
                    'peso_total': self._parse_float(record.get('peso_total', 0.0)),
                    
                    # Calculated rates
                    'tasa_contacto': self._calculate_contact_rate(contactos_efectivos, total_gestiones),
                    'tasa_conversion': self._calculate_conversion_rate(total_pdp, contactos_efectivos),
                    'score_productividad': self._calculate_productivity_score(
                        total_gestiones, contactos_efectivos, total_pdp
                    ),
                    
                    # Agent info (denormalized from joins)
                    'nombre_agente': self._extract_agent_name(record),
                    'dni_agente': self._clean_string(record.get('dni_agente', ''), max_length=20),
                    'equipo': self._clean_string(record.get('equipo', ''), max_length=50),
                    
                    # Metadata
                    'fecha_procesamiento': self._parse_datetime(record.get('fecha_procesamiento')) or datetime.now(timezone.utc)
                }
                
                # Validation
                if not transformed['fecha_foto'] or not correo_agente:
                    self.transformation_stats['records_skipped'] += 1
                    continue
                
                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming productivity record: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue
        
        return transformed_records
    
    # =============================================================================
    # HELPER METHODS - Type Conversion & Validation
    # =============================================================================
    
    def _parse_date(self, value: Any) -> Optional[date]:
        """Parse date from various formats"""
        if value is None:
            return None
        
        if isinstance(value, date):
            return value
        
        if isinstance(value, datetime):
            return value.date()
        
        if isinstance(value, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
            except:
                try:
                    # Try other common formats
                    return datetime.strptime(value, '%Y-%m-%d').date()
                except:
                    return None
        
        return None
    
    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """Parse datetime from various formats"""
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
    
    def _parse_int(self, value: Any) -> int:
        """Parse integer with fallback to 0"""
        if value is None or value == '':
            return 0
        
        try:
            if isinstance(value, str):
                # Remove any non-numeric characters except minus
                cleaned = re.sub(r'[^\d\-]', '', value)
                return int(cleaned) if cleaned else 0
            return int(float(value))  # Handle decimal strings
        except (ValueError, TypeError):
            return 0
    
    def _parse_float(self, value: Any) -> float:
        """Parse float with fallback to 0.0"""
        if value is None or value == '':
            return 0.0
        
        try:
            if isinstance(value, Decimal):
                return float(value)
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _clean_string(self, value: Any, max_length: int = None) -> str:
        """Clean and validate string fields"""
        if value is None:
            return ''
        
        cleaned = str(value).strip()
        
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        
        return cleaned
    
    def _clean_email(self, value: Any) -> str:
        """Clean and validate email addresses"""
        email = self._clean_string(value, max_length=100)
        
        # Basic email validation
        if '@' in email and '.' in email.split('@')[-1]:
            return email.lower()
        
        # If not a valid email, return as-is (might be user ID)
        return email
    
    def _normalize_percentage(self, value: Any) -> float:
        """Normalize percentage values to 0-100 scale"""
        pct = self._parse_float(value)
        
        # If value is between 0-1, assume it's in decimal format
        if 0 <= pct <= 1:
            return pct * 100
        
        # Otherwise assume it's already in percentage format
        return max(0, min(100, pct))
    
    # =============================================================================
    # BUSINESS LOGIC HELPERS
    # =============================================================================
    
    def _standardize_cartera(self, value: str) -> str:
        """Standardize portfolio names"""
        cartera = self._clean_string(value, max_length=50).upper()
        
        # Mapping rules for portfolio standardization
        mapping = {
            'TEMPRANA': 'TEMPRANA',
            'ALTAS_NUEVAS': 'ALTAS_NUEVAS',
            'ALTAS NUEVAS': 'ALTAS_NUEVAS',
            'CUOTA_FRACCIONAMIENTO': 'CUOTA_FRACCIONAMIENTO',
            'CF_ANN': 'CUOTA_FRACCIONAMIENTO',
            'OTRAS': 'OTRAS'
        }
        
        return mapping.get(cartera, cartera)
    
    def _standardize_servicio(self, value: str) -> str:
        """Standardize service names"""
        servicio = self._clean_string(value, max_length=20).upper()
        
        # Mapping rules for service standardization
        if servicio in ['MOVIL', 'MÓVIL', 'MOBILE']:
            return 'MOVIL'
        elif servicio in ['FIJA', 'FIJO', 'FIXED']:
            return 'FIJA'
        else:
            return 'FIJA'  # Default fallback
    
    def _standardize_canal(self, value: str) -> str:
        """Standardize channel names"""
        canal = self._clean_string(value, max_length=20).upper()
        
        # Mapping rules for channel standardization
        mapping = {
            'BOT': 'BOT',
            'VOICEBOT': 'BOT',
            'HUMANO': 'HUMANO',
            'HUMAN': 'HUMANO',
            'CALL_CENTER': 'HUMANO',
            'CALL CENTER': 'HUMANO'
        }
        
        return mapping.get(canal, 'BOT')  # Default to BOT
    
    def _standardize_periodo(self, value: str) -> str:
        """Standardize period format to YYYY-MM"""
        periodo = self._clean_string(value, max_length=7)
        
        # Validate YYYY-MM format
        if re.match(r'^\d{4}-\d{2}$', periodo):
            return periodo
        
        # Try to extract from longer strings
        match = re.search(r'(\d{4})-(\d{2})', periodo)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        
        # Fallback to current month
        return datetime.now().strftime('%Y-%m')
    
    def _calculate_ticket_promedio(self, deuda: float, cuentas: int) -> float:
        """Calculate average ticket"""
        if cuentas == 0:
            return 0.0
        return round(deuda / cuentas, 2)
    
    def _calculate_contact_rate(self, contactos: int, total: int) -> float:
        """Calculate contact rate percentage"""
        if total == 0:
            return 0.0
        return round((contactos / total) * 100, 2)
    
    def _calculate_conversion_rate(self, pdp: int, contactos: int) -> float:
        """Calculate conversion rate percentage"""
        if contactos == 0:
            return 0.0
        return round((pdp / contactos) * 100, 2)
    
    def _calculate_productivity_score(self, gestiones: int, contactos: int, pdp: int) -> float:
        """Calculate productivity score (weighted average)"""
        if gestiones == 0:
            return 0.0
        
        # Weighted score: 40% gestiones, 35% contactos, 25% pdp
        score = (gestiones * 0.4) + (contactos * 0.35) + (pdp * 0.25)
        return round(score, 2)
    
    def _extract_agent_name(self, record: Dict[str, Any]) -> Optional[str]:
        """Extract agent name from various possible fields"""
        # Try multiple possible fields
        name_fields = ['nombre_agente', 'agent_name', 'usuario', 'correo_agente']
        
        for field in name_fields:
            if field in record and record[field]:
                name = self._clean_string(record[field], max_length=100)
                if name and '@' not in name:  # Avoid using email as name
                    return name
        
        return None
    
    def _validate_dashboard_record(self, record: Dict[str, Any]) -> bool:
        """Validate dashboard record has required fields"""
        required_fields = ['fecha_foto', 'archivo', 'cartera', 'servicio']
        
        for field in required_fields:
            if not record.get(field):
                self.validation_errors.append(f"Missing required field: {field}")
                return False
        
        return True
    
    def _apply_dashboard_business_rules(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply business rules to dashboard data"""
        
        # Ensure percentages are within valid ranges
        percentage_fields = ['pct_cober', 'pct_contac', 'pct_cd', 'pct_ci', 'pct_conversion', 'pct_efectividad', 'pct_cierre']
        for field in percentage_fields:
            if field in record:
                record[field] = max(0, min(100, record[field]))
        
        # Ensure count consistency
        total_contacts = record.get('cuentas_cd', 0) + record.get('cuentas_ci', 0)
        if total_contacts > record.get('cuentas_gestionadas', 0):
            record['cuentas_gestionadas'] = total_contacts
        
        # Calculate missing SC if not provided
        if 'cuentas_sc' not in record or record['cuentas_sc'] == 0:
            record['cuentas_sc'] = max(0, record.get('cuentas_gestionadas', 0) - total_contacts)
        
        return record
    
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
        self.validation_errors = []


# =============================================================================
# TABLE-SPECIFIC TRANSFORMER MAPPING
# =============================================================================

class TransformerRegistry:
    """Registry of transformers for different table types"""
    
    def __init__(self):
        self.transformer = DataTransformer()
        
        # Map table names to their transformation methods
        self.transformer_mapping = {
            'dashboard_data': self.transformer.transform_dashboard_data,
            'evolution_data': self.transformer.transform_evolution_data,
            'assignment_data': self.transformer.transform_assignment_data,
            'operation_data': self.transformer.transform_operation_data,
            'productivity_data': self.transformer.transform_productivity_data
        }
    
    def transform_table_data(self, table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for a specific table"""
        if table_name not in self.transformer_mapping:
            raise ValueError(f"No transformer found for table: {table_name}")
        
        transformer_func = self.transformer_mapping[table_name]
        return transformer_func(raw_data)
    
    def get_supported_tables(self) -> List[str]:
        """Get list of supported table transformations"""
        return list(self.transformer_mapping.keys())
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """Get transformation statistics"""
        return self.transformer.get_transformation_stats()


# 🎯 Global transformer registry instance
_transformer_registry: Optional[TransformerRegistry] = None

def get_transformer_registry() -> TransformerRegistry:
    """Get singleton transformer registry instance"""
    global _transformer_registry
    
    if _transformer_registry is None:
        _transformer_registry = TransformerRegistry()
    
    return _transformer_registry


# 🚀 Convenience function for ETL pipeline
def transform_data(table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw BigQuery data for a specific table"""
    registry = get_transformer_registry()
    return registry.transform_table_data(table_name, raw_data)
