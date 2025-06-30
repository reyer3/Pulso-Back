"""
📊 Dashboard Data Transformer
SRP: Solo responsable de transformar dashboard_data
Implementa lógica de negocio compleja con pandas siguiendo documentación
"""

from datetime import date, datetime, timezone
from typing import Dict, Any
import pandas as pd

from etl.transformers.mart_transformer_base import MartTransformerBase


class DashboardDataTransformer(MartTransformerBase):
    """
    Transformer específico para dashboard_data
    Implementa reglas de negocio complejas con pandas
    Alineado con app/models/dashboard.py
    """
    
    def get_sql_filename(self) -> str:
        return "build_dashboard_data_source.sql"
    
    def get_mart_table_name(self) -> str:
        return "dashboard_data"
    
    def transform_with_pandas(self, df: pd.DataFrame, fecha_proceso: date, **kwargs) -> pd.DataFrame:
        """
        Implementa lógica de negocio compleja con pandas
        Calcula KPIs siguiendo reglas del modelo de negocio documentado
        """
        if df.empty:
            return df
        
        self.logger.info(f"📊 Processing {len(df)} campaigns for dashboard KPIs")
        
        # Aplicar transformaciones secuenciales
        df = self._calculate_coverage_metrics(df)
        df = self._calculate_contact_metrics(df)  
        df = self._calculate_effectiveness_metrics(df)
        df = self._calculate_closure_metrics(df)
        df = self._calculate_derived_counts(df)
        df = self._add_metadata_columns(df, fecha_proceso)
        df = self._apply_business_validations(df)
        
        return df
    
    def _calculate_coverage_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas de cobertura
        Cobertura = % de cuentas asignadas que fueron gestionadas
        """
        df['pct_cober'] = self._calculate_percentage(df['cuentas_gestionadas'], df['cuentas'])
        return df
    
    def _calculate_contact_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas de contacto basado en gestiones unificadas
        """
        # Tasa de contacto = contactos efectivos / total gestiones
        df['pct_contac'] = self._calculate_percentage(df['contactos_efectivos'], df['total_gestiones'])
        
        # Distribución de tipos de contacto
        df['pct_cd'] = self._calculate_percentage(df['cuentas_cd'], df['total_gestiones'])
        df['pct_ci'] = self._calculate_percentage(df['cuentas_ci'], df['total_gestiones'])
        
        return df
    
    def _calculate_effectiveness_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula efectividad con lógica de negocio compleja
        Combina tasa de contacto + conversión + factores de cartera
        """
        # Conversión = PDPs / contactos efectivos
        df['pct_conversion'] = self._calculate_percentage(df['cuentas_pdp'], df['contactos_efectivos'])
        
        # Efectividad = scoring complejo (AQUÍ brilla Python vs SQL)
        df['pct_efectividad'] = df.apply(self._calculate_effectiveness_score, axis=1)
        
        return df
    
    def _calculate_effectiveness_score(self, row) -> float:
        """
        Scoring de efectividad con reglas de negocio específicas
        Implementa lógica documentada de homologación y canales
        """
        # Base: tasa de contacto
        base_score = row.get('pct_contac', 0)
        
        # Bonus por conversión (siguiendo umbrales de negocio)
        conversion_rate = row.get('pct_conversion', 0)
        if conversion_rate >= 25:  # Excelencia en conversión
            conversion_bonus = 15
        elif conversion_rate >= 15:  # Buena conversión
            conversion_bonus = 10
        elif conversion_rate >= 10:  # Conversión aceptable
            conversion_bonus = 5
        else:
            conversion_bonus = 0
        
        # Factor por tipo de cartera (reglas específicas del negocio)
        cartera_factor = self._get_cartera_adjustment_factor(row.get('tipo_cartera', ''))
        
        # Factor por mix de canales (bot vs humano)
        channel_factor = self._get_channel_mix_factor(row)
        
        # Penalización por baja intensidad
        intensidad = self._safe_divide(row.get('total_gestiones', 0), row.get('cuentas_gestionadas', 0))
        intensity_penalty = 0 if intensidad >= 1.5 else 5
        
        # Score final con aplicación de factores
        final_score = (base_score + conversion_bonus) * cartera_factor * channel_factor - intensity_penalty
        
        return round(max(0, min(100, final_score)), 2)
    
    def _get_cartera_adjustment_factor(self, cartera: str) -> float:
        """
        Factor de ajuste por tipo de cartera siguiendo reglas de negocio
        """
        if not cartera:
            return 1.0
        
        cartera_upper = cartera.upper()
        
        # Reglas basadas en dificultad de gestión por vertical
        if 'ENERGIA' in cartera_upper:
            return 1.1  # Energía más difícil, bonus 10%
        elif 'TELEFONIA' in cartera_upper or 'MOVIL' in cartera_upper:
            return 0.95  # Telefonía más fácil, leve penalty
        elif 'INTERNET' in cartera_upper:
            return 1.05  # Internet dificultad intermedia
        else:
            return 1.0  # Otros neutral
    
    def _get_channel_mix_factor(self, row) -> float:
        """
        Factor basado en mix de canales bot vs humano
        Implementa lógica de homologación documentada
        """
        total_gestiones = row.get('total_gestiones', 0)
        if total_gestiones == 0:
            return 1.0
        
        gestiones_bot = row.get('gestiones_bot', 0)
        gestiones_humano = row.get('gestiones_humano', 0)
        
        # % de gestiones por bot
        bot_percentage = (gestiones_bot / total_gestiones) * 100
        
        # Optimal mix: 60-80% bot, 20-40% humano
        if 60 <= bot_percentage <= 80:
            return 1.05  # Mix óptimo, bonus 5%
        elif bot_percentage > 90:
            return 0.95  # Demasiado bot, puede perder personalización
        elif bot_percentage < 30:
            return 0.98  # Demasiado humano, menos eficiente
        else:
            return 1.0  # Mix aceptable
    
    def _calculate_closure_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas de cierre basado en reducción de deuda + recupero
        """
        # Reducción de deuda (no negativa)
        debt_reduction = (df['deuda_asig'] - df['deuda_act']).clip(lower=0)
        
        # Impacto total = reducción + recupero
        total_impact = debt_reduction + df['recupero']
        
        # % de cierre = impacto total / deuda inicial
        df['pct_cierre'] = self._calculate_percentage(total_impact, df['deuda_asig'])
        
        # Intensidad = gestiones promedio por cuenta gestionada
        df['inten'] = df.apply(
            lambda row: round(self._safe_divide(row['total_gestiones'], row['cuentas_gestionadas']), 2),
            axis=1
        )
        
        return df
    
    def _calculate_derived_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula conteos derivados para completar modelo de dashboard
        """
        # Cuentas sin gestión = total - gestionadas
        df['cuentas_sg'] = df['cuentas'] - df['cuentas_gestionadas']
        df['cuentas_sg'] = df['cuentas_sg'].clip(lower=0)  # No negativos
        
        return df
    
    def _add_metadata_columns(self, df: pd.DataFrame, fecha_proceso: date) -> pd.DataFrame:
        """
        Añade columnas de metadata siguiendo esquema de tabla mart
        """
        current_time = datetime.now(timezone.utc)
        
        df['fecha_foto'] = fecha_proceso
        df['fecha_procesamiento'] = current_time
        df['created_at'] = current_time
        df['updated_at'] = current_time
        
        return df
    
    def _apply_business_validations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica validaciones de negocio para mantener calidad de datos
        """
        initial_count = len(df)
        
        # Validar porcentajes (0-100%)
        percentage_cols = ['pct_cober', 'pct_contac', 'pct_cd', 'pct_ci', 'pct_conversion', 'pct_efectividad', 'pct_cierre']
        for col in percentage_cols:
            if col in df.columns:
                df = df[(df[col] >= 0) & (df[col] <= 100)]
        
        # Validar valores financieros no negativos
        financial_cols = ['deuda_asig', 'deuda_act', 'recupero']
        for col in financial_cols:
            if col in df.columns:
                df = df[df[col] >= 0]
        
        # Validar conteos no negativos
        count_cols = ['cuentas', 'clientes', 'cuentas_gestionadas', 'cuentas_cd', 'cuentas_ci', 'cuentas_sc', 'cuentas_sg', 'cuentas_pdp']
        for col in count_cols:
            if col in df.columns:
                df = df[df[col] >= 0]
        
        # Validar lógica de negocio: cuentas gestionadas <= cuentas totales
        df = df[df['cuentas_gestionadas'] <= df['cuentas']]
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            self.logger.warning(f"📊 Filtered {filtered_count} invalid records from dashboard data")
        
        return df
