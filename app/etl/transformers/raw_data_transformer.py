"""
🔄 Raw Data Transformers - BigQuery to PostgreSQL Raw Tables
FIXED: Column case sensitivity mapping for PostgreSQL compatibility
FIXED: Removed field duplications that caused INSERT column mismatches
FIXED: Inconsistencias en métodos helper y manejo de errores
ADDED: Debug logging for NULL primary key investigation

ISSUE: 58 extracted → 58 transformed → 0 loaded (null primary key)
DEBUG: Added logging to investigate NULL archivo or periodo_date values
"""

from datetime import datetime, date, timezone
from typing import List, Dict, Any, Optional, Union
import re

from app.core.logging import LoggerMixin


class RawDataTransformer(LoggerMixin):
    """
    Raw data transformer for BigQuery → PostgreSQL raw staging tables

    PRINCIPLE: Minimal transformation - just type conversion and basic cleaning
    PRESERVE: Original BigQuery data structure for business logic layer
    FIXED: Column name mapping for case sensitivity compatibility
    FIXED: Removed field duplications that caused INSERT errors
    FIXED: Inconsistencias en métodos helper y manejo de errores
    DEBUG: Added logging for NULL primary key investigation
    """
    ISO_DATE_FORMAT = '%Y-%m-%d'

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

        FIXED: Column names mapped to PostgreSQL lowercase convention
        FIXED: Removed field duplications that caused INSERT column mismatches
        DEBUG: Added logging for NULL primary key investigation
        """
        transformed_records = []

        # 🔍 DEBUG: Log sample of raw data to investigate NULL primary keys
        if raw_data:
            self.logger.info(f"🔍 DEBUG: First raw record keys: {list(raw_data[0].keys())}")
            self.logger.info(f"🔍 DEBUG: First raw record sample: {dict(list(raw_data[0].items())[:5])}")

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # 🔍 DEBUG: Log primary key values for first few records
                raw_archivo = record.get('ARCHIVO')
                raw_periodo_date = record.get('periodo_date')

                if i < 3:  # Log first 3 records
                    self.logger.info(
                        f"🔍 DEBUG Record {i}: "
                        f"ARCHIVO='{raw_archivo}' (type: {type(raw_archivo)}), "
                        f"periodo_date='{raw_periodo_date}' (type: {type(raw_periodo_date)})"
                    )

                # Validate required primary key
                archivo = self._safe_string(record.get('ARCHIVO'))
                if not archivo:
                    self.logger.warning(f"⚠️ Skipping record {i}: archivo is null/empty (raw: '{raw_archivo}')")
                    self.transformation_stats['records_skipped'] += 1
                    continue

                # Validate periodo_date
                periodo_date = self._safe_date(record.get('periodo_date'))
                if not periodo_date:
                    self.logger.warning(f"⚠️ Skipping record {i}: periodo_date is null/empty (raw: '{raw_periodo_date}')")
                    self.transformation_stats['records_skipped'] += 1
                    continue

                # ✅ FIXED: Removed field duplications
                transformed = {
                    # Primary key - ✅ FIXED: Single definition only
                    'archivo': archivo,

                    # Campaign metadata - ✅ FIXED: Single definition only
                    'tipo_cartera': self._safe_string(record.get('TIPO_CARTERA')),

                    # Business dates - critical for campaign logic
                    'fecha_apertura': self._safe_date(record.get('fecha_apertura')),
                    'fecha_trandeuda': self._safe_date(record.get('fecha_trandeuda')),
                    'fecha_cierre': self._safe_date(record.get('fecha_cierre')),
                    'fecha_cierre_planificada': self._safe_date(record.get('FECHA_CIERRE_PLANIFICADA')),

                    # Campaign characteristics
                    'duracion_campana_dias_habiles': self._safe_int(record.get('DURACION_CAMPANA_DIAS_HABILES')),
                    'anno_asignacion': self._safe_int(record.get('ANNO_ASIGNACION')),
                    'periodo_asignacion': self._safe_string(record.get('PERIODO_ASIGNACION')),
                    'es_cartera_abierta': self._safe_bool(record.get('ES_CARTERA_ABIERTA')),
                    'rango_vencimiento': self._safe_string(record.get('RANGO_VENCIMIENTO')),
                    'estado_cartera': self._safe_string(record.get('ESTADO_CARTERA')),

                    # Time partitioning - ✅ FIXED: Use validated periodo_date
                    'periodo_mes': self._safe_string(record.get('periodo_mes')),
                    'periodo_date': periodo_date,  # Already validated above

                    # Campaign classification
                    'tipo_ciclo_campana': self._safe_string(record.get('tipo_ciclo_campana')),
                    'categoria_duracion': self._safe_string(record.get('categoria_duracion')),

                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }

                # Validate required business date
                if not transformed['fecha_apertura']:
                    self.logger.warning(f"⚠️ Skipping record {i}: fecha_apertura is null/empty")
                    self.transformation_stats['records_skipped'] += 1
                    continue

                # 🔍 DEBUG: Log successful transformation for first few records
                if i < 3:
                    self.logger.info(
                        f"✅ DEBUG Record {i} transformed: "
                        f"archivo='{transformed['archivo']}', "
                        f"periodo_date='{transformed['periodo_date']}', "
                        f"fecha_apertura='{transformed['fecha_apertura']}'"
                    )

                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1

            except Exception as e:
                self.logger.error(f"Error transforming raw_calendario record {i}: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue

        # 🔍 DEBUG: Final transformation summary
        self.logger.info(
            f"🔍 DEBUG Summary: {len(raw_data)} raw → {len(transformed_records)} transformed "
            f"(skipped: {self.transformation_stats['records_skipped']}, "
            f"errors: {self.transformation_stats['validation_errors']})"
        )

        return transformed_records

    def transform_raw_asignaciones(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery asignaciones to PostgreSQL raw_asignaciones

        FIXED: Column names mapped to PostgreSQL lowercase convention
        """
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Validate primary key components
                cod_luna = self._safe_string(record.get('cod_luna'))
                cuenta = self._safe_string(record.get('cuenta'))
                archivo = self._safe_string(record.get('archivo'))

                if not cod_luna or not cuenta or not archivo:
                    self.logger.warning(f"⚠️ Skipping asignaciones record {i}: missing primary key components")
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
                self.logger.error(f"Error transforming raw_asignaciones record {i}: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue

        return transformed_records

    def transform_raw_trandeuda(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery trandeuda to PostgreSQL raw_trandeuda

        MINIMAL: Type conversion + debt validation
        """
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Validate primary key components
                cod_cuenta = self._safe_string(record.get('cod_cuenta'))
                nro_documento = self._safe_string(record.get('nro_documento'))
                archivo = self._safe_string(record.get('archivo'))
                monto_exigible = self._safe_decimal(record.get('monto_exigible'))

                if not cod_cuenta or not nro_documento or not archivo:
                    self.logger.warning(f"⚠️ Skipping trandeuda record {i}: missing primary key components")
                    self.transformation_stats['records_skipped'] += 1
                    continue

                # Skip zero debt records
                if monto_exigible is None or monto_exigible <= 0:
                    self.logger.warning(f"⚠️ Skipping trandeuda record {i}: invalid monto_exigible ({monto_exigible})")
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
                self.logger.error(f"Error transforming raw_trandeuda record {i}: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue

        return transformed_records

    def transform_raw_pagos(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery pagos to PostgreSQL raw_pagos

        MINIMAL: Type conversion + payment validation
        """
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Validate primary key components (for deduplication)
                nro_documento = self._safe_string(record.get('nro_documento'))
                fecha_pago = self._safe_date(record.get('fecha_pago'))
                monto_cancelado = self._safe_decimal(record.get('monto_cancelado'))

                if not nro_documento or not fecha_pago or monto_cancelado is None or monto_cancelado <= 0:
                    self.logger.warning(f"⚠️ Skipping pagos record {i}: invalid primary key or amount")
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
                self.logger.error(f"Error transforming raw_pagos record {i}: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue

        return transformed_records

    def transform_gestiones_unificadas(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform BigQuery unified gestiones to PostgreSQL gestiones_unificadas

        PRESERVE: Homologated business flags for KPI calculation
        """
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Validate primary key components
                cod_luna = self._safe_string(record.get('cod_luna'))
                timestamp_gestion = self._safe_datetime(record.get('timestamp_gestion'))

                if not cod_luna or not timestamp_gestion:
                    self.logger.warning(f"⚠️ Skipping gestiones record {i}: missing primary key components")
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

                    # Weighting for business logic - FIXED: usar default_value en lugar de default
                    'peso_gestion': self._safe_int(record.get('peso_gestion'), default_value=1),

                    # ETL metadata
                    'extraction_timestamp': self._safe_datetime(record.get('extraction_timestamp')) or datetime.now(timezone.utc)
                }

                transformed_records.append(transformed)
                self.transformation_stats['records_transformed'] += 1

            except Exception as e:
                self.logger.error(f"Error transforming gestiones_unificadas record {i}: {str(e)}")
                self.transformation_stats['validation_errors'] += 1
                continue

        return transformed_records

    def transform_raw_homologacion_mibotair(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforms mibotair homologation data, casting 'peso' to integer."""
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Convierte 'peso' a integer. Si falla o es nulo, pone 1.
                peso_str = record.get('peso')
                record['peso'] = int(peso_str) if peso_str and str(peso_str).isdigit() else 1

                transformed_records.append(record)
                self.transformation_stats['records_transformed'] += 1

            except (ValueError, TypeError) as e:
                self.logger.warning(f"⚠️ Error processing mibotair record {i}: {str(e)}, setting peso=1")
                record['peso'] = 1
                transformed_records.append(record)
                self.transformation_stats['records_transformed'] += 1

        return transformed_records

    def transform_raw_homologacion_voicebot(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforms voicebot homologation data, converting es_pdp to boolean."""
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Convierte es_pdp_homologado (0 o 1) a un booleano
                record['es_pdp_homologado'] = bool(record.get('es_pdp_homologado', 0))

                transformed_records.append(record)
                self.transformation_stats['records_transformed'] += 1

            except Exception as e:
                self.logger.warning(f"⚠️ Error processing voicebot record {i}: {str(e)}")
                record['es_pdp_homologado'] = False
                transformed_records.append(record)
                self.transformation_stats['records_transformed'] += 1

        return transformed_records

    def transform_raw_ejecutivos(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforms agent data, handling null/empty documents."""
        transformed_records = []

        for i, record in enumerate(raw_data):
            try:
                self.transformation_stats['records_processed'] += 1

                # Si el documento es nulo o está vacío, lo reemplaza por "SIN DNI"
                if not record.get('document'):
                    record['document'] = 'SIN DNI'

                # Asegurarse de que el nombre sea un string
                record['nombre'] = str(record.get('nombre', '')).strip()

                transformed_records.append(record)
                self.transformation_stats['records_transformed'] += 1

            except Exception as e:
                self.logger.warning(f"⚠️ Error processing ejecutivos record {i}: {str(e)}")
                record['document'] = 'SIN DNI'
                record['nombre'] = ''
                transformed_records.append(record)
                self.transformation_stats['records_transformed'] += 1

        return transformed_records

    # =============================================================================
    # HELPER METHODS - Safe Type Conversion
    # =============================================================================

    @staticmethod
    def _safe_string(
            value: Union[str, Any],
            max_length: Optional[int] = None
    ) -> Optional[str]:
        """Safely converts input value to string with optional length validation.

        Args:
            value: Value to convert to string
            max_length: Optional maximum length limit for the resulting string

        Returns:
            Cleaned string value or None if conversion fails or result is empty

        Raises:
            ValueError: If max_length is negative
        """
        empty_values = (None, '')

        if value in empty_values:
            return None

        if max_length is not None and max_length < 0:
            raise ValueError("max_length must be a non-negative integer")

        try:
            result = str(value).strip()
            if not result:
                return None

            if max_length is not None:
                result = result[:max_length]

            return result

        except (ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def _safe_int(
            input_value: Union[str, int, float, None],
            default_value: Optional[int] = None
    ) -> Optional[int]:
        """Safely converts various input types to integer.

        Args:
            input_value: Value to convert (str, int, float, or None)
            default_value: Value to return if conversion fails (default: None)

        Returns:
            Converted integer value or default_value if conversion fails

        Examples:
            > _safe_int("123") == 123
            > _safe_int("12.3") == 12
            > _safe_int("12,300") == 12300
            > _safe_int(None) is None
        """
        # Early return for None or empty string
        if input_value is None or input_value == '':
            return default_value

        try:
            # Handle string input
            if isinstance(input_value, str):
                # Remove all non-numeric characters except minus sign
                cleaned_value = re.sub(r'[^\d\-]', '', input_value)
                return int(cleaned_value) if cleaned_value else default_value

            # Handle numeric input
            return int(float(input_value))

        except (ValueError, TypeError, OverflowError):
            return default_value

    @staticmethod
    def _safe_decimal(value: Any, default: Optional[float] = None) -> Optional[float]:
        """Safe decimal conversion - FIXED: Consistente con otros métodos helper

        Args:
            value: Value to convert to float
            default: Default value to return if conversion fails (default: None)

        Returns:
            Converted float value or default if conversion fails
        """
        if value is None or value == '':
            return default

        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_bool(value: Any) -> bool:
        """Safe boolean conversion"""
        if value is None:
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'si', 'sí')

        try:
            return bool(int(value))
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _safe_date(value: Any) -> Optional[date]:
        """Safe date conversion with enhanced error handling"""
        if value is None:
            return None

        if isinstance(value, date):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            return RawDataTransformer._parse_date_string(value)

        return None

    @staticmethod
    def _parse_date_string(date_string: str) -> Optional[date]:
        """Helper method to parse date strings in various formats.

        Supports:
        - ISO format (YYYY-MM-DD)
        - ISO datetime format with timezone
        - Common date formats
        """
        if not date_string or not date_string.strip():
            return None

        date_string = date_string.strip()

        try:
            # Try ISO format first (most common)
            return datetime.fromisoformat(date_string.replace('Z', '+00:00')).date()
        except ValueError:
            try:
                # Try common YYYY-MM-DD format
                return datetime.strptime(date_string, RawDataTransformer.ISO_DATE_FORMAT).date()
            except ValueError:
                # Could add more date formats here if needed
                return None

    @staticmethod
    def _safe_datetime(value: Any) -> Optional[datetime]:
        """Safe datetime conversion with consistent timezone handling"""
        if value is None:
            return None

        if isinstance(value, datetime):
            # Ensure timezone awareness - assume UTC if none specified
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if isinstance(value, str):
            if not value.strip():
                return None
            try:
                # Handle ISO format with or without timezone
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                # Ensure timezone awareness
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                return None

        return None

    def _standardize_canal(self, value: Any) -> str:
        """Standardize channel values with consistent defaults"""
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
            'raw_calendario': lambda raw_data: self.transformer.transform_raw_calendario,
            'raw_asignaciones': self.transformer.transform_raw_asignaciones,
            'raw_trandeuda': self.transformer.transform_raw_trandeuda,
            'raw_pagos': self.transformer.transform_raw_pagos,
            'gestiones_unificadas': self.transformer.transform_gestiones_unificadas,
            "raw_homologacion_mibotair": self.transformer.transform_raw_homologacion_mibotair,
            "raw_homologacion_voicebot": self.transformer.transform_raw_homologacion_voicebot,
            "raw_ejecutivos": self.transformer.transform_raw_ejecutivos,
        }

    def transform_raw_table_data(self, table_name: str, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data for a specific raw table"""
        if table_name not in self.raw_transformer_mapping:
            raise ValueError(f"No raw transformer found for table: {table_name}")

        transformer_func = self.raw_transformer_mapping[table_name]
        return transformer_func(raw_data)

    def get_supported_raw_tables(self) -> List[str]:
        """Get list of supported raw table transformations  """
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