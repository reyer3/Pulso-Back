# app/etl/builders/mart_builder.py

"""
MartBuilder: Constructs auxiliary and mart tables using SQL transformations.
FIXED: Added missing asyncio import for placeholder methods
"""

import asyncio
import logging
from typing import Type
from app.database.connection import DatabaseManager
from app.core.logging import LoggerMixin
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from typing import Optional

@dataclass
class CampaignWindow:
    """Minimal definition for type hinting - shared with pipeline"""
    archivo: str
    fecha_apertura: date
    fecha_cierre: Optional[date] = None

class MartBuilder(LoggerMixin):
    def __init__(self, db_manager: DatabaseManager, project_uid: str):
        super().__init__()
        self.db_manager = db_manager
        self.project_uid = project_uid
        self.logger = logging.getLogger(__name__)

    async def _execute_sql(self, sql_query: str, *args):
        """Helper to execute SQL queries."""
        self.logger.debug(f"Executing SQL in MartBuilder: {sql_query[:200]}... with args: {args}")
        try:
            return await self.db_manager.execute_query(sql_query, *args, fetch="none")
        except Exception as e:
            self.logger.error(f"SQL execution error in MartBuilder: {e}\\nQuery: {sql_query}\\nArgs: {args}", exc_info=True)
            raise

    async def _build_unified_gestiones(self, campaign: CampaignWindow):
        """
        Unifies Voicebot and MibotAir data into aux_P3fV4dWNeMkN5RJMhV8e.gestiones_unificadas.
        """
        self.logger.info(f"Building unified gestiones for campaign '{campaign.archivo}' in project '{self.project_uid}'.")

        target_table = f"aux_{self.project_uid}.gestiones_unificadas"
        raw_voicebot = f"raw_{self.project_uid}.voicebot_gestiones"
        raw_mibotair = f"raw_{self.project_uid}.mibotair_gestiones"
        raw_asignaciones = f"raw_{self.project_uid}.asignaciones"
        raw_homo_voicebot = f"raw_{self.project_uid}.homologacion_voicebot"
        raw_homo_mibotair = f"raw_{self.project_uid}.homologacion_mibotair"

        # Idempotency: Delete existing data for this campaign
        delete_sql = f"DELETE FROM {target_table} WHERE archivo_campana = $1;"
        await self._execute_sql(delete_sql, campaign.archivo)
        self.logger.debug(f"Deleted existing unified gestiones for campaign '{campaign.archivo}'.")

        unification_query = f"""
        INSERT INTO {target_table} (
            gestion_uid, cod_luna, timestamp_gestion, fecha_gestion, canal_origen,
            nivel_1, nivel_2, nivel_3, contactabilidad,
            es_contacto_efectivo, es_compromiso,
            monto_compromiso, fecha_compromiso, archivo_campana
        )
        -- Voicebot Gestiones
        SELECT
            vb.uid AS gestion_uid,
            a.cod_luna,
            vb."date" AS timestamp_gestion,
            DATE(vb."date") AS fecha_gestion,
            'BOT' AS canal_origen,
            hv.n1_homologado AS nivel_1,
            hv.n2_homologado AS nivel_2,
            hv.n3_homologado AS nivel_3,
            COALESCE(hv.contactabilidad_homologada, 'SIN_CLASIFICAR') AS contactabilidad,
            (hv.contactabilidad_homologada = 'Contacto Efectivo') AS es_contacto_efectivo,
            COALESCE(hv.es_pdp_homologado, FALSE) AS es_compromiso,
            NULLIF(SPLIT_PART(vb.compromiso, '|', 2), '')::NUMERIC AS monto_compromiso,
            vb.fecha_compromiso AS fecha_compromiso,
            a.archivo AS archivo_campana
        FROM {raw_voicebot} vb
        JOIN {raw_asignaciones} a ON vb.document = a.dni AND a.archivo = $1
        LEFT JOIN {raw_homo_voicebot} hv
            ON vb.management = hv.bot_management
            AND vb.sub_management = hv.bot_sub_management
            AND vb.compromiso = hv.bot_compromiso

        UNION ALL

        -- MibotAir Gestiones
        SELECT
            ma.uid AS gestion_uid,
            a.cod_luna,
            ma."date" AS timestamp_gestion,
            DATE(ma."date") AS fecha_gestion,
            'HUMANO' AS canal_origen,
            hm.n_1 AS nivel_1,
            hm.n_2 AS nivel_2,
            hm.n_3 AS nivel_3,
            COALESCE(hm.contactabilidad, 'SIN_CLASIFICAR') AS contactabilidad,
            (hm.contactabilidad = 'Contacto Efectivo') AS es_contacto_efectivo,
            (UPPER(hm.pdp) = 'SI') AS es_compromiso,
            ma.monto_compromiso,
            ma.fecha_compromiso,
            a.archivo AS archivo_campana
        FROM {raw_mibotair} ma
        JOIN {raw_asignaciones} a ON ma.document = a.dni AND a.archivo = $1
        LEFT JOIN {raw_homo_mibotair} hm
            ON ma.n1 = hm.n_1
            AND ma.n2 = hm.n_2
            AND ma.n3 = hm.n_3;
        """

        await self._execute_sql(unification_query, campaign.archivo)
        self.logger.info(f"Successfully built unified gestiones for campaign '{campaign.archivo}'.")

    async def _build_aux_cuenta_campana_state(self, campaign: CampaignWindow):
        """Build cuenta_campana_state auxiliary table for the campaign"""
        self.logger.info(f"Building cuenta_campana_state for campaign '{campaign.archivo}'.")
        
        target_table = f"aux_{self.project_uid}.cuenta_campana_state"
        raw_asignaciones = f"raw_{self.project_uid}.asignaciones"
        raw_trandeuda = f"raw_{self.project_uid}.trandeuda"
        
        # Delete existing data for this campaign
        delete_sql = f"DELETE FROM {target_table} WHERE archivo = $1;"
        await self._execute_sql(delete_sql, campaign.archivo)
        
        # Insert cuenta state for this campaign
        insert_sql = f"""
        INSERT INTO {target_table} (
            cod_luna, cuenta, archivo, dni, telefono, 
            tramo_gestion, negocio, decil_contacto, decil_pago,
            saldo_inicial, saldo_actual, fecha_apertura_campana,
            created_at, updated_at
        )
        SELECT 
            a.cod_luna,
            a.cuenta,
            a.archivo,
            a.dni,
            a.telefono,
            a.tramo_gestion,
            a.negocio,
            a.decil_contacto,
            a.decil_pago,
            COALESCE(td.monto_exigible, 0) as saldo_inicial,
            COALESCE(td.monto_exigible, 0) as saldo_actual,
            c.fecha_apertura as fecha_apertura_campana,
            NOW() as created_at,
            NOW() as updated_at
        FROM {raw_asignaciones} a
        LEFT JOIN {raw_trandeuda} td ON a.cuenta = td.cod_cuenta AND a.archivo = td.archivo
        LEFT JOIN raw_{self.project_uid}.calendario c ON a.archivo = c.archivo
        WHERE a.archivo = $1;
        """
        
        await self._execute_sql(insert_sql, campaign.archivo)
        self.logger.info(f"Successfully built cuenta_campana_state for '{campaign.archivo}'.")

    async def _build_aux_gestion_cuenta_impact(self, campaign: CampaignWindow):
        """Build gestion impact analysis for the campaign"""
        self.logger.info(f"Building gestion_cuenta_impact for campaign '{campaign.archivo}'.")
        
        target_table = f"aux_{self.project_uid}.gestion_cuenta_impact"
        gestiones_table = f"aux_{self.project_uid}.gestiones_unificadas"
        cuentas_table = f"aux_{self.project_uid}.cuenta_campana_state"
        
        # Delete existing data for this campaign
        delete_sql = f"DELETE FROM {target_table} WHERE archivo_campana = $1;"
        await self._execute_sql(delete_sql, campaign.archivo)
        
        # Insert gestion impact analysis
        insert_sql = f"""
        INSERT INTO {target_table} (
            gestion_uid, cod_luna, canal_origen, contactabilidad,
            es_contacto_efectivo, es_compromiso, monto_compromiso,
            fecha_gestion, archivo_campana, saldo_cuenta,
            impacto_contactabilidad, peso_gestion,
            created_at, updated_at
        )
        SELECT 
            g.gestion_uid,
            g.cod_luna,
            g.canal_origen,
            g.contactabilidad,
            g.es_contacto_efectivo,
            g.es_compromiso,
            g.monto_compromiso,
            g.fecha_gestion,
            g.archivo_campana,
            c.saldo_actual as saldo_cuenta,
            CASE 
                WHEN g.es_contacto_efectivo THEN 'ALTO'
                WHEN g.contactabilidad = 'Contacto No Efectivo' THEN 'MEDIO'
                ELSE 'BAJO'
            END as impacto_contactabilidad,
            CASE g.canal_origen
                WHEN 'HUMANO' THEN 3
                WHEN 'BOT' THEN 1
                ELSE 1
            END as peso_gestion,
            NOW() as created_at,
            NOW() as updated_at
        FROM {gestiones_table} g
        LEFT JOIN {cuentas_table} c ON g.cod_luna = c.cod_luna AND g.archivo_campana = c.archivo
        WHERE g.archivo_campana = $1;
        """
        
        await self._execute_sql(insert_sql, campaign.archivo)
        self.logger.info(f"Successfully built gestion_cuenta_impact for '{campaign.archivo}'.")

    async def _build_aux_pago_deduplication(self, campaign: CampaignWindow):
        """Build payment deduplication for the campaign"""
        self.logger.info(f"Building pago_deduplication for campaign '{campaign.archivo}'.")
        
        target_table = f"aux_{self.project_uid}.pago_deduplication"
        raw_pagos = f"raw_{self.project_uid}.pagos"
        cuentas_table = f"aux_{self.project_uid}.cuenta_campana_state"
        
        # Delete existing data for this campaign
        delete_sql = f"DELETE FROM {target_table} WHERE archivo_campana = $1;"
        await self._execute_sql(delete_sql, campaign.archivo)
        
        # Insert deduplicated payments
        insert_sql = f"""
        INSERT INTO {target_table} (
            nro_documento, fecha_pago, monto_cancelado, cod_sistema,
            archivo_pago, cod_luna_asociado, archivo_campana,
            es_pago_unico, ranking_pago, created_at, updated_at
        )
        WITH pagos_ranked AS (
            SELECT 
                p.*,
                c.cod_luna as cod_luna_asociado,
                c.archivo as archivo_campana,
                ROW_NUMBER() OVER (
                    PARTITION BY p.nro_documento, p.fecha_pago, p.monto_cancelado 
                    ORDER BY p.created_at
                ) as ranking_pago
            FROM {raw_pagos} p
            LEFT JOIN {cuentas_table} c ON p.nro_documento = c.dni
            WHERE c.archivo = $1
        )
        SELECT 
            nro_documento,
            fecha_pago,
            monto_cancelado,
            cod_sistema,
            archivo as archivo_pago,
            cod_luna_asociado,
            archivo_campana,
            (ranking_pago = 1) as es_pago_unico,
            ranking_pago,
            NOW() as created_at,
            NOW() as updated_at
        FROM pagos_ranked;
        """
        
        await self._execute_sql(insert_sql, campaign.archivo)
        self.logger.info(f"Successfully built pago_deduplication for '{campaign.archivo}'.")

    async def _build_mart_dashboard_data(self, campaign: CampaignWindow):
        """Build dashboard data mart for the campaign"""
        self.logger.info(f"Building dashboard_data for campaign '{campaign.archivo}'.")
        
        target_table = f"mart_{self.project_uid}.dashboard_data"
        gestiones_table = f"aux_{self.project_uid}.gestion_cuenta_impact"
        pagos_table = f"aux_{self.project_uid}.pago_deduplication"
        cuentas_table = f"aux_{self.project_uid}.cuenta_campana_state"
        
        # Delete existing data for this campaign
        delete_sql = f"DELETE FROM {target_table} WHERE archivo = $1;"
        await self._execute_sql(delete_sql, campaign.archivo)
        
        # Insert dashboard aggregations
        insert_sql = f"""
        INSERT INTO {target_table} (
            archivo, fecha_proceso, total_cuentas, total_gestiones,
            gestiones_efectivas, gestiones_bot, gestiones_humano,
            total_compromisos, monto_compromisos, total_pagos,
            monto_pagos, tasa_contactabilidad, tasa_efectividad,
            created_at, updated_at
        )
        SELECT 
            c.archivo,
            CURRENT_DATE as fecha_proceso,
            COUNT(DISTINCT c.cod_luna) as total_cuentas,
            COUNT(g.gestion_uid) as total_gestiones,
            COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END) as gestiones_efectivas,
            COUNT(CASE WHEN g.canal_origen = 'BOT' THEN 1 END) as gestiones_bot,
            COUNT(CASE WHEN g.canal_origen = 'HUMANO' THEN 1 END) as gestiones_humano,
            COUNT(CASE WHEN g.es_compromiso THEN 1 END) as total_compromisos,
            COALESCE(SUM(g.monto_compromiso), 0) as monto_compromisos,
            COUNT(CASE WHEN p.es_pago_unico THEN 1 END) as total_pagos,
            COALESCE(SUM(CASE WHEN p.es_pago_unico THEN p.monto_cancelado END), 0) as monto_pagos,
            CASE WHEN COUNT(g.gestion_uid) > 0 
                 THEN ROUND(COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END) * 100.0 / COUNT(g.gestion_uid), 2)
                 ELSE 0 
            END as tasa_contactabilidad,
            CASE WHEN COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END) > 0
                 THEN ROUND(COUNT(CASE WHEN g.es_compromiso THEN 1 END) * 100.0 / COUNT(CASE WHEN g.es_contacto_efectivo THEN 1 END), 2)
                 ELSE 0
            END as tasa_efectividad,
            NOW() as created_at,
            NOW() as updated_at
        FROM {cuentas_table} c
        LEFT JOIN {gestiones_table} g ON c.cod_luna = g.cod_luna AND c.archivo = g.archivo_campana
        LEFT JOIN {pagos_table} p ON c.dni = p.nro_documento AND c.archivo = p.archivo_campana
        WHERE c.archivo = $1
        GROUP BY c.archivo;
        """
        
        await self._execute_sql(insert_sql, campaign.archivo)
        self.logger.info(f"Successfully built dashboard_data for '{campaign.archivo}'.")

    async def _build_mart_evolution_data(self, campaign: CampaignWindow):
        """Build evolution data mart - daily progression tracking"""
        self.logger.info(f"Building evolution_data for campaign '{campaign.archivo}'.")
        # Implementation would track daily KPI evolution
        await asyncio.sleep(0.1)  # Placeholder
        self.logger.debug(f"Placeholder: evolution_data built for '{campaign.archivo}'.")

    async def _build_mart_assignment_data(self, campaign: CampaignWindow):
        """Build assignment data mart - campaign assignment analysis"""
        self.logger.info(f"Building assignment_data for campaign '{campaign.archivo}'.")
        # Implementation would analyze assignment patterns and effectiveness
        await asyncio.sleep(0.1)  # Placeholder
        self.logger.debug(f"Placeholder: assignment_data built for '{campaign.archivo}'.")

    async def _build_mart_operation_data(self, campaign: CampaignWindow):
        """Build operation data mart - operational metrics"""
        self.logger.info(f"Building operation_data for campaign '{campaign.archivo}'.")
        # Implementation would calculate operational efficiency metrics
        await asyncio.sleep(0.1)  # Placeholder
        self.logger.debug(f"Placeholder: operation_data built for '{campaign.archivo}'.")

    async def _build_mart_productivity_data(self, campaign: CampaignWindow):
        """Build productivity data mart - agent and system productivity"""
        self.logger.info(f"Building productivity_data for campaign '{campaign.archivo}'.")
        # Implementation would analyze agent performance and system productivity
        await asyncio.sleep(0.1)  # Placeholder
        self.logger.debug(f"Placeholder: productivity_data built for '{campaign.archivo}'.")

    async def _build_mart_mibot_air_layout(self, campaign: CampaignWindow):
        """Build Mibot Air Layout mart - specific output format for MibotAir"""
        self.logger.info(f"Building Mibot Air Layout mart for campaign '{campaign.archivo}'.")
        # Implementation would create the complex layout as specified in original requirements
        await asyncio.sleep(0.1)  # Placeholder
        self.logger.debug(f"Placeholder: Mibot Air Layout mart built for '{campaign.archivo}'.")

    async def build_for_campaign(self, campaign: CampaignWindow):
        """
        Orchestrates the build process for auxiliary and mart tables for a specific campaign.
        COMPLETED: Core aux tables with real SQL, placeholders for remaining marts
        """
        self.logger.info(f"Starting MartBuilder full process for campaign '{campaign.archivo}'.")

        # Build auxiliary tables first (COMPLETED with real SQL)
        await self._build_unified_gestiones(campaign)
        await self._build_aux_cuenta_campana_state(campaign)
        await self._build_aux_gestion_cuenta_impact(campaign)
        await self._build_aux_pago_deduplication(campaign)

        # Build mart tables (CORE completed, others placeholders)
        await self._build_mart_dashboard_data(campaign)  # COMPLETED
        await self._build_mart_evolution_data(campaign)  # Placeholder
        await self._build_mart_assignment_data(campaign)  # Placeholder
        await self._build_mart_operation_data(campaign)  # Placeholder
        await self._build_mart_productivity_data(campaign)  # Placeholder
        await self._build_mart_mibot_air_layout(campaign)  # Placeholder

        self.logger.info(f"Successfully completed MartBuilder full process for campaign '{campaign.archivo}'.")
