"""
📊 Source tables repository
Direct access to BigQuery source tables (asignacion, tran_deuda, pagos, gestiones)
"""

from typing import Any, Dict, List, Optional, Union
from datetime import date, datetime, timedelta

from app.core.config import settings
from app.repositories.bigquery_repo import BigQueryRepository


class SourceTablesRepository(BigQueryRepository):
    """
    Repository for accessing source tables directly
    Simple queries, Python processing
    """
    
    def __init__(self):
        super().__init__()
        # Source table names
        self.tables = {
            'asignacion': 'batch_P3fV4dWNeMkN5RJMhV8e_asignacion',
            'tran_deuda': 'batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda', 
            'pagos': 'batch_P3fV4dWNeMkN5RJMhV8e_pagos',
            'voicebot': 'voicebot_P3fV4dWNeMkN5RJMhV8e',
            'mibotair': 'mibotair_P3fV4dWNeMkN5RJMhV8e',
            'calendario': 'bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5'
        }
    
    # =============================================================================
    # ASIGNACIONES
    # =============================================================================
    
    async def get_asignaciones(
        self,
        archivos: Optional[List[str]] = None,
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        limit: int = 100000
    ) -> List[Dict[str, Any]]:
        """
        Get asignaciones (assignments) data
        Simple query - processing in Python
        """
        conditions = []
        params = {}
        
        # Filter by files
        if archivos:
            placeholders = [f"@archivo_{i}" for i in range(len(archivos))]
            conditions.append(f"archivo IN ({', '.join(placeholders)})")
            for i, archivo in enumerate(archivos):
                params[f"archivo_{i}"] = archivo
        
        # Filter by date range
        if fecha_desde:
            conditions.append("DATE(creado_el) >= @fecha_desde")
            params["fecha_desde"] = fecha_desde.isoformat()
        
        if fecha_hasta:
            conditions.append("DATE(creado_el) <= @fecha_hasta")
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            archivo,
            cod_luna,
            cuenta,
            min_vto,
            negocio,
            telefono,
            tramo_gestion,
            decil_contacto,
            decil_pago,
            creado_el
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['asignacion']}`
        WHERE {where_clause}
        ORDER BY creado_el DESC
        LIMIT {limit}
        """
        
        return await self.execute_query(query, params)
    
    async def get_asignaciones_by_archivo(self, archivo: str) -> List[Dict[str, Any]]:
        """
        Get all asignaciones for a specific file
        """
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['asignacion']}`
        WHERE archivo = @archivo
        ORDER BY creado_el DESC
        """
        
        return await self.execute_query(query, {"archivo": archivo})
    
    # =============================================================================
    # TRAN_DEUDA 
    # =============================================================================
    
    async def get_tran_deuda(
        self,
        cuentas: Optional[List[str]] = None,
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        limit: int = 500000
    ) -> List[Dict[str, Any]]:
        """
        Get tran_deuda (debt transactions) data
        """
        conditions = []
        params = {}
        
        # Filter by accounts
        if cuentas:
            placeholders = [f"@cuenta_{i}" for i in range(len(cuentas))]
            conditions.append(f"cod_cuenta IN ({', '.join(placeholders)})")
            for i, cuenta in enumerate(cuentas):
                params[f"cuenta_{i}"] = str(cuenta)
        
        # Filter by date range
        if fecha_desde:
            conditions.append("DATE(creado_el) >= @fecha_desde")
            params["fecha_desde"] = fecha_desde.isoformat()
        
        if fecha_hasta:
            conditions.append("DATE(creado_el) <= @fecha_hasta")
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            cod_cuenta,
            nro_documento,
            fecha_vencimiento,
            monto_exigible,
            creado_el
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['tran_deuda']}`
        WHERE {where_clause}
        ORDER BY creado_el DESC, cod_cuenta
        LIMIT {limit}
        """
        
        return await self.execute_query(query, params)
    
    async def get_deuda_by_fecha(
        self, 
        fecha: date,
        cuentas: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get debt snapshot for specific date
        """
        conditions = ["DATE(creado_el) = @fecha"]
        params = {"fecha": fecha.isoformat()}
        
        if cuentas:
            placeholders = [f"@cuenta_{i}" for i in range(len(cuentas))]
            conditions.append(f"cod_cuenta IN ({', '.join(placeholders)})")
            for i, cuenta in enumerate(cuentas):
                params[f"cuenta_{i}"] = str(cuenta)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
        SELECT 
            cod_cuenta,
            nro_documento, 
            fecha_vencimiento,
            monto_exigible,
            creado_el
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['tran_deuda']}`
        WHERE {where_clause}
        ORDER BY cod_cuenta
        """
        
        return await self.execute_query(query, params)
    
    # =============================================================================
    # PAGOS
    # =============================================================================
    
    async def get_pagos(
        self,
        documentos: Optional[List[str]] = None,
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        limit: int = 200000
    ) -> List[Dict[str, Any]]:
        """
        Get pagos (payments) data
        """
        conditions = []
        params = {}
        
        # Filter by documents
        if documentos:
            placeholders = [f"@doc_{i}" for i in range(len(documentos))]
            conditions.append(f"nro_documento IN ({', '.join(placeholders)})")
            for i, doc in enumerate(documentos):
                params[f"doc_{i}"] = doc
        
        # Filter by payment date
        if fecha_desde:
            conditions.append("fecha_pago >= @fecha_desde")
            params["fecha_desde"] = fecha_desde.isoformat()
        
        if fecha_hasta:
            conditions.append("fecha_pago <= @fecha_hasta")
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            nro_documento,
            fecha_pago,
            monto_cancelado,
            creado_el
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['pagos']}`
        WHERE {where_clause}
        ORDER BY fecha_pago DESC, creado_el DESC
        LIMIT {limit}
        """
        
        return await self.execute_query(query, params)
    
    # =============================================================================
    # GESTIONES (BOT + HUMANO)
    # =============================================================================
    
    async def get_gestiones_bot(
        self,
        cod_lunas: Optional[List[int]] = None,
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        limit: int = 300000
    ) -> List[Dict[str, Any]]:
        """
        Get voicebot gestiones
        """
        conditions = []
        params = {}
        
        # Filter by cod_luna
        if cod_lunas:
            placeholders = [f"@luna_{i}" for i in range(len(cod_lunas))]
            conditions.append(f"SAFE_CAST(document AS INT64) IN ({', '.join(placeholders)})")
            for i, luna in enumerate(cod_lunas):
                params[f"luna_{i}"] = luna
        
        # Filter by date
        if fecha_desde:
            conditions.append("DATE(date) >= @fecha_desde")
            params["fecha_desde"] = fecha_desde.isoformat()
        
        if fecha_hasta:
            conditions.append("DATE(date) <= @fecha_hasta")
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            SAFE_CAST(document AS INT64) as cod_luna,
            date as fecha_gestion,
            management,
            sub_management,
            compromiso,
            'BOT' as canal
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['voicebot']}`
        WHERE {where_clause}
        ORDER BY date DESC
        LIMIT {limit}
        """
        
        return await self.execute_query(query, params)
    
    async def get_gestiones_humano(
        self,
        cod_lunas: Optional[List[int]] = None,
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        limit: int = 300000
    ) -> List[Dict[str, Any]]:
        """
        Get human gestiones (mibotair)
        """
        conditions = []
        params = {}
        
        # Filter by cod_luna
        if cod_lunas:
            placeholders = [f"@luna_{i}" for i in range(len(cod_lunas))]
            conditions.append(f"SAFE_CAST(document AS INT64) IN ({', '.join(placeholders)})")
            for i, luna in enumerate(cod_lunas):
                params[f"luna_{i}"] = luna
        
        # Filter by date
        if fecha_desde:
            conditions.append("DATE(date) >= @fecha_desde")
            params["fecha_desde"] = fecha_desde.isoformat()
        
        if fecha_hasta:
            conditions.append("DATE(date) <= @fecha_hasta")
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            SAFE_CAST(document AS INT64) as cod_luna,
            date as fecha_gestion,
            management,
            sub_management,
            n3 as compromiso,
            'HUMANO' as canal
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['mibotair']}`
        WHERE {where_clause}
        ORDER BY date DESC
        LIMIT {limit}
        """
        
        return await self.execute_query(query, params)
    
    # =============================================================================
    # CALENDARIO (METADATA DE CARTERAS)
    # =============================================================================
    
    async def get_calendario(
        self,
        archivos: Optional[List[str]] = None,
        estado: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get calendario (portfolio metadata)
        """
        conditions = []
        params = {}
        
        if archivos:
            placeholders = [f"@archivo_{i}" for i in range(len(archivos))]
            conditions.append(f"ARCHIVO IN ({', '.join(placeholders)})")
            for i, archivo in enumerate(archivos):
                params[f"archivo_{i}"] = archivo
        
        if estado:
            conditions.append("ESTADO_CARTERA = @estado")
            params["estado"] = estado
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            ARCHIVO,
            TIPO_CARTERA,
            FECHA_ASIGNACION,
            FECHA_TRANDEUDA,
            FECHA_CIERRE,
            FECHA_CIERRE_PLANIFICADA,
            DURACION_CAMPANA_DIAS_HABILES,
            ANNO_ASIGNACION,
            PERIODO_ASIGNACION,
            ES_CARTERA_ABIERTA,
            RANGO_VENCIMIENTO,
            ESTADO_CARTERA
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['calendario']}`
        WHERE {where_clause}
        ORDER BY FECHA_ASIGNACION DESC
        """
        
        return await self.execute_query(query, params)
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    async def get_archivos_activos(self, periodo: Optional[str] = None) -> List[str]:
        """
        Get list of active portfolio files
        """
        conditions = []
        params = {}
        
        if periodo:
            conditions.append("PERIODO_ASIGNACION = @periodo")
            params["periodo"] = periodo
        
        # Default to current and previous month
        if not periodo:
            current_date = datetime.now().date()
            current_period = current_date.strftime("%Y-%m")
            prev_month = (current_date.replace(day=1) - timedelta(days=1))
            prev_period = prev_month.strftime("%Y-%m")
            
            conditions.append("PERIODO_ASIGNACION IN (@current_period, @prev_period)")
            params["current_period"] = current_period
            params["prev_period"] = prev_period
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT DISTINCT ARCHIVO
        FROM `{self.project_id}.{self.dataset_id}.{self.tables['calendario']}`
        WHERE {where_clause}
        ORDER BY ARCHIVO
        """
        
        results = await self.execute_query(query, params)
        return [row['ARCHIVO'] for row in results]
    
    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get basic info about a source table
        """
        if table_name not in self.tables:
            raise ValueError(f"Unknown table: {table_name}")
        
        table_id = self.tables[table_name]
        
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT DATE(creado_el)) as days_with_data,
            MIN(DATE(creado_el)) as earliest_date,
            MAX(DATE(creado_el)) as latest_date
        FROM `{self.project_id}.{self.dataset_id}.{table_id}`
        WHERE creado_el IS NOT NULL
        """
        
        try:
            results = await self.execute_query(query)
            return results[0] if results else {}
        except Exception:
            # Fallback for tables without creado_el
            query = f"""
            SELECT COUNT(*) as total_rows
            FROM `{self.project_id}.{self.dataset_id}.{table_id}`
            """
            results = await self.execute_query(query)
            return results[0] if results else {}