"""
🔄 Data processing service
Python-based data transformations and business logic
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import pandas as pd

from app.core.logging import LoggerMixin


class DataProcessor(LoggerMixin):
    """
    Core data processing logic in Python
    Clean, flexible, and database-agnostic
    """
    
    def __init__(self):
        # Business rules configuration
        self.cartera_mapping = {
            'TEMPRANA': ['TEMPRANA'],
            'ALTAS_NUEVAS': ['AN', 'ALTAS'],
            'CUOTA_FRACCIONAMIENTO': ['CF_ANN', 'CUOTA'],
            'OTRAS': ['OTRAS']
        }
        
        self.vencimiento_groups = {
            'GRUPO_1_5': [1, 5],
            'GRUPO_9_13': [9, 13], 
            'GRUPO_17_25': [17, 21, 25]
        }
    
    # =============================================================================
    # ASIGNACIONES PROCESSING
    # =============================================================================
    
    def process_asignaciones(self, raw_asignaciones: List[Dict]) -> pd.DataFrame:
        """
        Process raw asignaciones into clean DataFrame
        """
        if not raw_asignaciones:
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_asignaciones)
        
        # Clean and derive fields
        df['archivo_clean'] = df['archivo'].str.replace('.txt', '')
        df['cartera'] = df['archivo'].apply(self._classify_cartera)
        df['servicio'] = df['negocio'].apply(lambda x: 'FIJA' if x != 'MOVIL' else 'MOVIL')
        df['fecha_asignacion'] = pd.to_datetime(df['creado_el']).dt.date
        df['vencimiento_grupo'] = df['min_vto'].apply(self._classify_vencimiento)
        
        # Extract day from min_vto
        df['dia_vencimiento'] = pd.to_datetime(df['min_vto']).dt.day
        
        # Account-level unique identifier
        df['cuenta_id'] = df['archivo_clean'] + '_' + df['cuenta'].astype(str)
        
        self.logger.info(f"Processed {len(df)} asignaciones")
        return df
    
    def _classify_cartera(self, archivo: str) -> str:
        """
        Classify portfolio type from filename
        """
        archivo_upper = archivo.upper()
        
        for cartera, patterns in self.cartera_mapping.items():
            if any(pattern in archivo_upper for pattern in patterns):
                return cartera
        
        return 'OTRAS'
    
    def _classify_vencimiento(self, min_vto: Any) -> str:
        """
        Classify vencimiento into groups
        """
        try:
            day = pd.to_datetime(min_vto).day
            
            for group, days in self.vencimiento_groups.items():
                if day in days:
                    return group
            
            return f'DIA_{day}'
        except:
            return 'UNKNOWN'
    
    # =============================================================================
    # TRAN_DEUDA PROCESSING
    # =============================================================================
    
    def process_tran_deuda(self, raw_tran_deuda: List[Dict]) -> pd.DataFrame:
        """
        Process raw tran_deuda into clean DataFrame
        """
        if not raw_tran_deuda:
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_tran_deuda)
        
        # Clean data types
        df['monto_exigible'] = pd.to_numeric(df['monto_exigible'], errors='coerce').fillna(0)
        df['fecha_trandeuda'] = pd.to_datetime(df['creado_el']).dt.date
        df['fecha_vencimiento'] = pd.to_datetime(df['fecha_vencimiento']).dt.date
        
        # Calculate days overdue
        df['dias_mora'] = (df['fecha_trandeuda'] - df['fecha_vencimiento']).dt.days
        df['dias_mora'] = df['dias_mora'].clip(lower=0)  # No negative days
        
        # Debt ranges
        df['rango_deuda'] = df['monto_exigible'].apply(self._classify_debt_range)
        
        # Status classification
        df['estado_deuda'] = df.apply(self._classify_debt_status, axis=1)
        
        self.logger.info(f"Processed {len(df)} tran_deuda records")
        return df
    
    def _classify_debt_range(self, monto: float) -> str:
        """
        Classify debt amount into ranges
        """
        if monto <= 0:
            return 'SIN_DEUDA'
        elif monto <= 1:
            return 'MENOR_1'
        elif monto <= 100:
            return 'RANGO_1_100'
        elif monto <= 500:
            return 'RANGO_100_500'
        else:
            return 'MAYOR_500'
    
    def _classify_debt_status(self, row) -> str:
        """
        Classify debt status
        """
        if row['monto_exigible'] <= 0:
            return 'SIN_MORA'
        elif row['dias_mora'] == 0:
            return 'NO_VENCIDO'
        else:
            return 'GESTIONABLE'
    
    # =============================================================================
    # GESTIONES PROCESSING
    # =============================================================================
    
    def process_gestiones(
        self, 
        gestiones_bot: List[Dict], 
        gestiones_humano: List[Dict]
    ) -> pd.DataFrame:
        """
        Combine and process bot + human gestiones
        """
        # Combine both sources
        all_gestiones = []
        
        # Process bot gestiones
        for gestion in gestiones_bot:
            all_gestiones.append({
                'cod_luna': gestion['cod_luna'],
                'fecha_gestion': pd.to_datetime(gestion['fecha_gestion']).date(),
                'canal': 'BOT',
                'management': gestion.get('management', ''),
                'sub_management': gestion.get('sub_management', ''),
                'compromiso': gestion.get('compromiso', ''),
                'timestamp_gestion': gestion['fecha_gestion']
            })
        
        # Process human gestiones
        for gestion in gestiones_humano:
            all_gestiones.append({
                'cod_luna': gestion['cod_luna'],
                'fecha_gestion': pd.to_datetime(gestion['fecha_gestion']).date(),
                'canal': 'HUMANO',
                'management': gestion.get('management', ''),
                'sub_management': gestion.get('sub_management', ''),
                'compromiso': gestion.get('compromiso', ''),
                'timestamp_gestion': gestion['fecha_gestion']
            })
        
        if not all_gestiones:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_gestiones)
        
        # Classify gestiones
        df['tipo_contacto'] = df.apply(self._classify_contact_type, axis=1)
        df['es_compromiso'] = df['compromiso'].apply(self._is_compromiso)
        df['peso_gestion'] = df.apply(self._calculate_weight, axis=1)
        
        # Sort by cod_luna and timestamp
        df = df.sort_values(['cod_luna', 'timestamp_gestion'])
        
        self.logger.info(f"Processed {len(df)} gestiones")
        return df
    
    def _classify_contact_type(self, row) -> str:
        """
        Classify contact type based on management codes
        Simple rules - can be enhanced with homologation tables
        """
        management = str(row['management']).upper()
        
        # Simple classification - enhance as needed
        if 'CONTACTO' in management or 'HABLO' in management:
            return 'DIRECTO'
        elif 'BUZON' in management or 'TERCERO' in management:
            return 'INDIRECTO' 
        elif 'NO_CONTACT' in management or 'SIN_CONTACTO' in management:
            return 'NO_CONTACTO'
        else:
            return 'NO_CONTACTO'  # Default
    
    def _is_compromiso(self, compromiso: str) -> bool:
        """
        Determine if gestion includes a promise to pay
        """
        if not compromiso:
            return False
        
        compromiso_upper = str(compromiso).upper()
        pdp_indicators = ['PROMESA', 'PAGO', 'COMPROMISO', 'SI', '1']
        
        return any(indicator in compromiso_upper for indicator in pdp_indicators)
    
    def _calculate_weight(self, row) -> int:
        """
        Calculate gestion weight for prioritization
        """
        weight = 1  # Base weight
        
        # Higher weight for direct contact
        if row['tipo_contacto'] == 'DIRECTO':
            weight += 2
        elif row['tipo_contacto'] == 'INDIRECTO':
            weight += 1
        
        # Higher weight for commitments
        if row['es_compromiso']:
            weight += 3
        
        return weight
    
    # =============================================================================
    # PAGOS PROCESSING
    # =============================================================================
    
    def process_pagos(self, raw_pagos: List[Dict]) -> pd.DataFrame:
        """
        Process raw pagos into clean DataFrame
        """
        if not raw_pagos:
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_pagos)
        
        # Clean data types
        df['monto_cancelado'] = pd.to_numeric(df['monto_cancelado'], errors='coerce').fillna(0)
        df['fecha_pago'] = pd.to_datetime(df['fecha_pago']).dt.date
        df['fecha_batch'] = pd.to_datetime(df['creado_el']).dt.date
        
        # Payment ranges
        df['rango_pago'] = df['monto_cancelado'].apply(self._classify_payment_range)
        
        # Day of week analysis
        df['dia_semana'] = pd.to_datetime(df['fecha_pago']).dt.day_name()
        
        # Remove duplicates (same document, date, amount)
        df = df.drop_duplicates(
            subset=['nro_documento', 'fecha_pago', 'monto_cancelado'],
            keep='last'
        )
        
        self.logger.info(f"Processed {len(df)} pagos")
        return df
    
    def _classify_payment_range(self, monto: float) -> str:
        """
        Classify payment amount into ranges
        """
        if monto <= 0:
            return 'INVALID'
        elif monto <= 10:
            return 'MENOR_10'
        elif monto <= 100:
            return 'RANGO_10_100'
        elif monto <= 500:
            return 'RANGO_100_500'
        else:
            return 'MAYOR_500'
    
    # =============================================================================
    # BUSINESS LOGIC AGGREGATIONS
    # =============================================================================
    
    def calculate_account_metrics(
        self,
        asignaciones_df: pd.DataFrame,
        gestiones_df: pd.DataFrame,
        pagos_df: pd.DataFrame,
        deuda_df: pd.DataFrame,
        fecha_corte: date
    ) -> pd.DataFrame:
        """
        Calculate account-level metrics
        Core business logic in Python
        """
        if asignaciones_df.empty:
            return pd.DataFrame()
        
        # Start with asignaciones as base
        metrics = asignaciones_df.copy()
        
        # Filter data up to fecha_corte
        gestiones_filtered = gestiones_df[
            gestiones_df['fecha_gestion'] <= fecha_corte
        ] if not gestiones_df.empty else pd.DataFrame()
        
        pagos_filtered = pagos_df[
            pagos_df['fecha_pago'] <= fecha_corte
        ] if not pagos_df.empty else pd.DataFrame()
        
        # Calculate gestiones metrics per account
        if not gestiones_filtered.empty:
            gestion_metrics = self._calculate_gestion_metrics(
                gestiones_filtered, asignaciones_df
            )
            metrics = metrics.merge(gestion_metrics, on='cod_luna', how='left')
        
        # Calculate payment metrics per account
        if not pagos_filtered.empty:
            payment_metrics = self._calculate_payment_metrics(
                pagos_filtered, asignaciones_df
            )
            metrics = metrics.merge(payment_metrics, on='nro_documento', how='left')
        
        # Add debt information
        if not deuda_df.empty:
            debt_metrics = self._calculate_debt_metrics(deuda_df, fecha_corte)
            metrics = metrics.merge(
                debt_metrics, 
                left_on='cuenta', 
                right_on='cod_cuenta', 
                how='left'
            )
        
        # Fill NaN values
        metrics = metrics.fillna({
            'total_gestiones': 0,
            'contacto_directo': 0,
            'contacto_indirecto': 0,
            'sin_contacto': 0,
            'compromisos': 0,
            'recupero_total': 0,
            'cantidad_pagos': 0,
            'deuda_actual': 0
        })
        
        # Calculate derived metrics
        metrics['gestionado'] = metrics['total_gestiones'] > 0
        metrics['contactado'] = (metrics['contacto_directo'] + metrics['contacto_indirecto']) > 0
        metrics['pagador'] = metrics['cantidad_pagos'] > 0
        
        return metrics
    
    def _calculate_gestion_metrics(
        self, 
        gestiones_df: pd.DataFrame, 
        asignaciones_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate gestion metrics per cod_luna
        """
        # Merge with asignaciones to get account context
        gestiones_with_accounts = gestiones_df.merge(
            asignaciones_df[['cod_luna', 'cuenta', 'archivo']],
            on='cod_luna',
            how='inner'
        )
        
        # Aggregate by cod_luna
        gestion_agg = gestiones_with_accounts.groupby('cod_luna').agg({
            'fecha_gestion': 'count',  # total gestiones
            'tipo_contacto': lambda x: (x == 'DIRECTO').sum(),  # contacto directo
            'es_compromiso': 'sum'  # compromisos
        }).rename(columns={
            'fecha_gestion': 'total_gestiones',
            'tipo_contacto': 'contacto_directo',
            'es_compromiso': 'compromisos'
        })
        
        # Calculate additional metrics
        contact_counts = gestiones_with_accounts.groupby('cod_luna')['tipo_contacto'].value_counts().unstack(fill_value=0)
        
        if 'INDIRECTO' in contact_counts.columns:
            gestion_agg['contacto_indirecto'] = contact_counts['INDIRECTO']
        else:
            gestion_agg['contacto_indirecto'] = 0
            
        if 'NO_CONTACTO' in contact_counts.columns:
            gestion_agg['sin_contacto'] = contact_counts['NO_CONTACTO']
        else:
            gestion_agg['sin_contacto'] = 0
        
        return gestion_agg.reset_index()
    
    def _calculate_payment_metrics(
        self, 
        pagos_df: pd.DataFrame, 
        asignaciones_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate payment metrics per nro_documento
        """
        payment_agg = pagos_df.groupby('nro_documento').agg({
            'monto_cancelado': ['sum', 'count'],
            'fecha_pago': 'min'  # First payment date
        })
        
        # Flatten column names
        payment_agg.columns = ['recupero_total', 'cantidad_pagos', 'primera_fecha_pago']
        
        return payment_agg.reset_index()
    
    def _calculate_debt_metrics(
        self, 
        deuda_df: pd.DataFrame, 
        fecha_corte: date
    ) -> pd.DataFrame:
        """
        Get latest debt snapshot per account
        """
        # Filter by fecha_corte
        deuda_filtered = deuda_df[
            deuda_df['fecha_trandeuda'] <= fecha_corte
        ]
        
        if deuda_filtered.empty:
            return pd.DataFrame()
        
        # Get latest record per account
        latest_debt = deuda_filtered.loc[
            deuda_filtered.groupby('cod_cuenta')['fecha_trandeuda'].idxmax()
        ]
        
        return latest_debt[['cod_cuenta', 'monto_exigible']].rename(
            columns={'monto_exigible': 'deuda_actual'}
        )