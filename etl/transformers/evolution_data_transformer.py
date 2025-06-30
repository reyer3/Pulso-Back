"""
📈 Evolution Data Transformer
SRP: Solo responsable de transformar evolution_data
Implementa análisis de series temporales y detección de tendencias
"""

from datetime import date, datetime, timezone
from typing import Dict, Any
import pandas as pd
import numpy as np

from etl.transformers.mart_transformer_base import MartTransformerBase


class EvolutionDataTransformer(MartTransformerBase):
    """
    Transformer específico para evolution_data
    Implementa análisis de series temporales con pandas/numpy
    Alineado con app/models/evolution.py
    """
    
    def get_sql_filename(self) -> str:
        return "build_evolution_data_source.sql"
    
    def get_mart_table_name(self) -> str:
        return "evolution_data"
    
    def _replace_sql_parameters(self, query: str, fecha_proceso: date, archivo: str = None, **kwargs) -> str:
        """Override para añadir parámetro lookback_days"""
        lookback_days = kwargs.get('lookback_days', 30)
        
        # Handle archivo filter
        archivo_value = f"'{archivo}'" if archivo else "NULL"
        
        replacements = {
            "{fecha_proceso}": f"'{fecha_proceso}'",
            "{archivo}": archivo_value,
            "{lookback_days}": str(lookback_days)
        }
        
        for key, value in replacements.items():
            query = query.replace(key, value)
        
        return query
    
    def transform_with_pandas(self, df: pd.DataFrame, fecha_proceso: date, **kwargs) -> pd.DataFrame:
        """
        Análisis de series temporales con pandas/numpy
        Calcula tendencias, volatilidad y patrones temporales
        """
        if df.empty:
            return df
        
        self.logger.info(f"📈 Processing {len(df)} time series records for evolution analysis")
        
        # Convertir fecha_foto a datetime para cálculos temporales
        df['fecha_foto'] = pd.to_datetime(df['fecha_foto'])
        
        # Procesar por grupo (archivo, cartera, servicio)
        evolution_records = []
        
        grouped = df.groupby(['archivo', 'cartera', 'servicio'])
        
        for (archivo, cartera, servicio), group in grouped:
            try:
                # Ordenar por fecha
                group = group.sort_values('fecha_foto').reset_index(drop=True)
                
                if len(group) < 2:
                    continue  # Necesitamos al menos 2 puntos para tendencias
                
                # Obtener valores más recientes (para fecha_proceso)
                latest = group.iloc[-1]
                
                # Calcular tendencias usando regresión lineal
                trends = self._calculate_trends(group)
                
                # Obtener promedios móviles del SQL o calcular si no están
                moving_avgs = self._get_moving_averages(group)
                
                # Calcular volatilidad
                volatility = self._calculate_volatility_metrics(group)
                
                # Detectar patrones temporales
                patterns = self._detect_temporal_patterns(group)
                
                record = {
                    'fecha_foto': fecha_proceso,
                    'archivo': archivo,
                    'cartera': cartera,
                    'servicio': servicio,
                    
                    # Valores actuales
                    'pct_cober': round(float(latest['pct_cober']), 2),
                    'pct_contac': round(float(latest['pct_contac']), 2),
                    'pct_efectividad': round(float(latest['pct_efectividad']), 2),
                    'pct_cierre': round(float(latest['pct_cierre']), 2),
                    'recupero': round(float(latest['recupero']), 2),
                    'cuentas': int(latest['cuentas']),
                    
                    # Tendencias (pendiente por día)
                    'trend_cober': round(trends['pct_cober'], 4),
                    'trend_contac': round(trends['pct_contac'], 4),
                    'trend_efectividad': round(trends['pct_efectividad'], 4),
                    'trend_cierre': round(trends['pct_cierre'], 4),
                    'trend_recupero': round(trends['recupero'], 4),
                    
                    # Promedios móviles
                    'avg_cober_7d': round(moving_avgs['pct_cober'], 2),
                    'avg_contac_7d': round(moving_avgs['pct_contac'], 2),
                    'avg_efectividad_7d': round(moving_avgs['pct_efectividad'], 2),
                    
                    # Volatilidad
                    'volatility_cober': round(volatility['pct_cober'], 4),
                    'volatility_contac': round(volatility['pct_contac'], 4),
                    'volatility_efectividad': round(volatility['pct_efectividad'], 4),
                    
                    # Patrones detectados
                    'trend_direction': patterns['trend_direction'],
                    'stability_score': round(patterns['stability_score'], 2),
                    'momentum_score': round(patterns['momentum_score'], 2),
                    
                    # Metadata
                    'days_analyzed': len(group),
                    'fecha_procesamiento': datetime.now(timezone.utc),
                    'created_at': datetime.now(timezone.utc),
                    'updated_at': datetime.now(timezone.utc)
                }
                
                evolution_records.append(record)
                
            except Exception as e:
                self.logger.error(f"Error processing evolution for {archivo}-{cartera}: {str(e)}")
                continue
        
        if not evolution_records:
            return pd.DataFrame()
        
        result_df = pd.DataFrame(evolution_records)
        
        # Validaciones de negocio
        result_df = self._apply_evolution_validations(result_df)
        
        return result_df
    
    def _calculate_trends(self, group: pd.DataFrame) -> Dict[str, float]:
        """
        Calcula tendencias lineales para métricas clave usando numpy
        AQUÍ es donde pandas/numpy brillan vs SQL
        """
        trends = {}
        
        # Crear variable independiente (días desde el inicio)
        x = group['days_from_start'].values
        
        # Métricas para analizar tendencias
        metrics = ['pct_cober', 'pct_contac', 'pct_efectividad', 'pct_cierre', 'recupero']
        
        for metric in metrics:
            y = group[metric].values
            
            # Filtrar valores válidos (no NaN)
            valid_mask = ~(pd.isna(x) | pd.isna(y))
            if valid_mask.sum() < 2:
                trends[metric] = 0.0
                continue
            
            x_valid = x[valid_mask]
            y_valid = y[valid_mask]
            
            try:
                # Regresión lineal: y = mx + b, queremos la pendiente (m)
                slope, _ = np.polyfit(x_valid, y_valid, 1)
                trends[metric] = float(slope)
            except:
                trends[metric] = 0.0
        
        return trends
    
    def _get_moving_averages(self, group: pd.DataFrame) -> Dict[str, float]:
        """
        Obtiene promedios móviles (desde SQL o calcula)
        """
        # Si están disponibles desde SQL, usar esos
        if 'pct_cober_7d_avg' in group.columns:
            latest = group.iloc[-1]
            return {
                'pct_cober': float(latest.get('pct_cober_7d_avg', 0)),
                'pct_contac': float(latest.get('pct_contac_7d_avg', 0)),
                'pct_efectividad': float(latest.get('pct_efectividad_7d_avg', 0))
            }
        
        # Si no, calcular con pandas
        window = min(7, len(group))
        recent_data = group.tail(window)
        
        return {
            'pct_cober': float(recent_data['pct_cober'].mean()),
            'pct_contac': float(recent_data['pct_contac'].mean()),
            'pct_efectividad': float(recent_data['pct_efectividad'].mean())
        }
    
    def _calculate_volatility_metrics(self, group: pd.DataFrame) -> Dict[str, float]:
        """
        Calcula volatilidad como desviación estándar
        """
        # Si están disponibles desde SQL, usar esos
        if 'pct_cober_volatility' in group.columns:
            latest = group.iloc[-1]
            return {
                'pct_cober': float(latest.get('pct_cober_volatility', 0) or 0),
                'pct_contac': float(latest.get('pct_contac_volatility', 0) or 0),
                'pct_efectividad': float(latest.get('pct_efectividad_volatility', 0) or 0)
            }
        
        # Calcular con pandas (últimos 7 días)
        window = min(7, len(group))
        recent_data = group.tail(window)
        
        return {
            'pct_cober': float(recent_data['pct_cober'].std() if len(recent_data) > 1 else 0),
            'pct_contac': float(recent_data['pct_contac'].std() if len(recent_data) > 1 else 0),
            'pct_efectividad': float(recent_data['pct_efectividad'].std() if len(recent_data) > 1 else 0)
        }
    
    def _detect_temporal_patterns(self, group: pd.DataFrame) -> Dict[str, Any]:
        """
        Detecta patrones temporales complejos
        AQUÍ es donde Python brilla para lógica compleja
        """
        if len(group) < 3:
            return {
                'trend_direction': 'INSUFFICIENT_DATA',
                'stability_score': 0.0,
                'momentum_score': 0.0
            }
        
        # Analizar tendencia general en efectividad
        efectividad_values = group['pct_efectividad'].values
        recent_trend = efectividad_values[-3:] if len(efectividad_values) >= 3 else efectividad_values
        
        # Determinar dirección de tendencia
        if len(recent_trend) >= 2:
            trend_slope = (recent_trend[-1] - recent_trend[0]) / len(recent_trend)
            
            if trend_slope > 1.0:
                trend_direction = 'IMPROVING_STRONG'
            elif trend_slope > 0.2:
                trend_direction = 'IMPROVING'
            elif trend_slope > -0.2:
                trend_direction = 'STABLE'
            elif trend_slope > -1.0:
                trend_direction = 'DECLINING'
            else:
                trend_direction = 'DECLINING_STRONG'
        else:
            trend_direction = 'STABLE'
        
        # Score de estabilidad (basado en coeficiente de variación)
        cv_efectividad = (efectividad_values.std() / efectividad_values.mean()) if efectividad_values.mean() > 0 else 1.0
        stability_score = max(0, 100 - (cv_efectividad * 100))  # 100 = muy estable, 0 = muy volátil
        
        # Score de momentum (velocidad de cambio reciente)
        if len(efectividad_values) >= 5:
            recent_change = efectividad_values[-1] - efectividad_values[-5]
            momentum_score = min(100, max(-100, recent_change * 5))  # Normalizado a -100/+100
        else:
            momentum_score = 0.0
        
        return {
            'trend_direction': trend_direction,
            'stability_score': stability_score,
            'momentum_score': momentum_score
        }
    
    def _apply_evolution_validations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica validaciones específicas para evolution_data
        """
        initial_count = len(df)
        
        # Validar que días analizados sea razonable
        df = df[df['days_analyzed'] >= 2]
        
        # Validar que las tendencias no sean extremas
        trend_cols = ['trend_cober', 'trend_contac', 'trend_efectividad', 'trend_cierre', 'trend_recupero']
        for col in trend_cols:
            if col in df.columns:
                # Filtrar tendencias extremas (más de ±10 puntos por día)
                df = df[(df[col] >= -10) & (df[col] <= 10)]
        
        # Validar scores
        df = df[
            (df['stability_score'] >= 0) & (df['stability_score'] <= 100) &
            (df['momentum_score'] >= -100) & (df['momentum_score'] <= 100)
        ]
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            self.logger.warning(f"📈 Filtered {filtered_count} invalid evolution records")
        
        return df
