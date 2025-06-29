"""
🎯 Business Logic Transformer - Campaign Window Deduplication
Implements the complex business logic for campaign window processing:

1. Build cuenta_campana_state (universe of manageable accounts per campaign)
2. Map gestiones to accounts (gestion_cuenta_impact) 
3. Deduplicate payments (pago_deduplication)
4. Calculate accurate KPIs for dashboard_data

This transformer handles the core business problem of deduplication and attribution.
"""

import logging
from datetime import datetime, date, timezone
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
import pandas as pd

from app.etl.extractors.bigquery_extractor import BigQueryExtractor
from app.database.connection import execute_query


@dataclass
class CampaignWindow:
    """Represents a campaign time window with its metadata"""
    archivo: str
    fecha_apertura: date
    fecha_cierre: Optional[date]
    tipo_cartera: str
    anno_asignacion: int
    estado_cartera: str


class BusinessLogicTransformer:
    """
    Implements complex business logic for campaign window processing
    
    Key responsibilities:
    1. Build clean campaign windows from calendario
    2. Map cod_luna -> active accounts per campaign  
    3. Attribute gestiones to correct accounts
    4. Deduplicate payments reliably
    """
    
    def __init__(self, extractor: Optional[BigQueryExtractor] = None):
        self.extractor = extractor or BigQueryExtractor()
        self.logger = logging.getLogger(__name__)
        
        # Processing statistics
        self.stats = {
            'campaigns_processed': 0,
            'accounts_mapped': 0,
            'gestiones_attributed': 0,
            'payments_deduplicated': 0,
            'errors': 0
        }
    
    async def process_campaign_window(self, archivo: str) -> Dict[str, Any]:
        """
        Complete processing for a single campaign window
        
        Args:
            archivo: Campaign identifier
            
        Returns:
            Processing summary with statistics
        """
        try:
            self.logger.info(f"🏁 Starting campaign window processing: {archivo}")
            
            # Step 1: Get campaign metadata
            campaign = await self._get_campaign_window(archivo)
            if not campaign:
                raise ValueError(f"Campaign not found: {archivo}")
            
            # Step 2: Build account states for this campaign
            accounts_processed = await self._build_cuenta_campana_state(campaign)
            
            # Step 3: Map gestiones to active accounts
            gestiones_processed = await self._map_gestiones_to_accounts(campaign)
            
            # Step 4: Deduplicate payments for this campaign
            payments_processed = await self._deduplicate_campaign_payments(campaign)
            
            # Update statistics
            self.stats['campaigns_processed'] += 1
            self.stats['accounts_mapped'] += accounts_processed
            self.stats['gestiones_attributed'] += gestiones_processed
            self.stats['payments_deduplicated'] += payments_processed
            
            self.logger.info(
                f"✅ Completed campaign {archivo}: "
                f"{accounts_processed} accounts, {gestiones_processed} gestiones, "
                f"{payments_processed} payments"
            )
            
            return {
                'archivo': archivo,
                'status': 'success',
                'accounts_processed': accounts_processed,
                'gestiones_processed': gestiones_processed,
                'payments_processed': payments_processed
            }
            
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"❌ Error processing campaign {archivo}: {str(e)}")
            raise
    
    async def _get_campaign_window(self, archivo: str) -> Optional[CampaignWindow]:
        """Get campaign window definition from calendario"""
        query = """
        SELECT 
            ARCHIVO,
            fecha_apertura,
            fecha_cierre,
            TIPO_CARTERA,
            ANNO_ASIGNACION,
            ESTADO_CARTERA
        FROM raw_calendario_cache
        WHERE ARCHIVO = $1
        """
        
        # Try from cached table first, fallback to BigQuery
        try:
            row = await execute_query(query, archivo, fetch="one")
            if row:
                return CampaignWindow(**dict(row))
        except:
            pass
        
        # Fallback: Extract from BigQuery
        calendario_data = await self.extractor.extract_table_data(
            "raw_calendario", 
            {"archivo": archivo}
        )
        
        if calendario_data:
            row = calendario_data[0]
            return CampaignWindow(
                archivo=row['ARCHIVO'],
                fecha_apertura=row['fecha_apertura'],
                fecha_cierre=row.get('fecha_cierre'),
                tipo_cartera=row['TIPO_CARTERA'],
                anno_asignacion=row['ANNO_ASIGNACION'],
                estado_cartera=row['ESTADO_CARTERA']
            )
        
        return None
    
    async def _build_cuenta_campana_state(self, campaign: CampaignWindow) -> int:
        """
        Build cuenta_campana_state table for this campaign
        
        Logic:
        1. Get all cod_luna assigned to this campaign
        2. For each cod_luna, get all their accounts from asignaciones
        3. Get debt state for each account at campaign start/current
        4. Mark accounts as gestionable based on business rules
        """
        
        # Step 1: Get asignaciones for this campaign
        asignaciones_query = """
        SELECT 
            CAST(cod_luna AS STRING) as cod_luna,
            CAST(cuenta AS STRING) as cuenta,
            archivo,
            fecha_archivo,
            tramo_gestion,
            negocio
        FROM raw_asignaciones_cache
        WHERE archivo = $1
        """
        
        asignaciones = await execute_query(asignaciones_query, campaign.archivo, fetch="all")
        
        if not asignaciones:
            self.logger.warning(f"No asignaciones found for campaign {campaign.archivo}")
            return 0
        
        # Step 2: Process each asignación
        cuenta_states = []
        
        for asignacion in asignaciones:
            cod_luna = asignacion['cod_luna']
            cuenta = asignacion['cuenta']
            
            # Get debt state for this account
            monto_inicial = await self._get_account_debt_at_date(
                cuenta, campaign.fecha_apertura
            )
            
            monto_actual = await self._get_account_debt_at_date(
                cuenta, campaign.fecha_cierre or date.today()
            )
            
            # Business rules for gestionable accounts
            es_cuenta_gestionable = (
                monto_inicial > 0 and  # Had debt at campaign start
                asignacion.get('tramo_gestion', '') != 'EXCLUIDO'  # Not excluded
            )
            
            cuenta_states.append({
                'archivo': campaign.archivo,
                'cod_luna': cod_luna,
                'cuenta': cuenta,
                'fecha_apertura': campaign.fecha_apertura,
                'fecha_cierre': campaign.fecha_cierre,
                'monto_inicial': monto_inicial,
                'monto_actual': monto_actual,
                'fecha_ultima_actualizacion': date.today(),
                'tiene_deuda_activa': monto_actual > 0,
                'es_cuenta_gestionable': es_cuenta_gestionable
            })
        
        # Step 3: Upsert to cuenta_campana_state table
        await self._upsert_cuenta_states(cuenta_states)
        
        return len(cuenta_states)
    
    async def _get_account_debt_at_date(self, cuenta: str, fecha: date) -> float:
        """Get account debt amount at a specific date"""
        
        # Get debt history for this account around the target date
        debt_query = """
        SELECT 
            monto_exigible,
            fecha_archivo,
            ABS(EXTRACT(EPOCH FROM (fecha_archivo - $2::date)) / 86400) as days_diff
        FROM raw_trandeuda_cache
        WHERE cod_cuenta = $1
          AND fecha_archivo <= $2::date
        ORDER BY days_diff ASC
        LIMIT 1
        """
        
        try:
            debt_row = await execute_query(debt_query, cuenta, fecha, fetch="one")
            if debt_row:
                return float(debt_row['monto_exigible'])
        except Exception as e:
            self.logger.warning(f"Could not get debt for account {cuenta} at {fecha}: {e}")
        
        return 0.0
    
    async def _upsert_cuenta_states(self, cuenta_states: List[Dict[str, Any]]) -> None:
        """Bulk upsert cuenta_campana_state records"""
        
        if not cuenta_states:
            return
        
        # Use pandas for efficient bulk operations
        df = pd.DataFrame(cuenta_states)
        
        # Prepare bulk upsert query
        upsert_query = """
        INSERT INTO cuenta_campana_state (
            archivo, cod_luna, cuenta, fecha_apertura, fecha_cierre,
            monto_inicial, monto_actual, fecha_ultima_actualizacion,
            tiene_deuda_activa, es_cuenta_gestionable, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
        ON CONFLICT (archivo, cod_luna, cuenta)
        DO UPDATE SET
            monto_inicial = EXCLUDED.monto_inicial,
            monto_actual = EXCLUDED.monto_actual,
            fecha_ultima_actualizacion = EXCLUDED.fecha_ultima_actualizacion,
            tiene_deuda_activa = EXCLUDED.tiene_deuda_activa,
            es_cuenta_gestionable = EXCLUDED.es_cuenta_gestionable,
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Execute bulk upsert
        for _, row in df.iterrows():
            await execute_query(
                upsert_query,
                row['archivo'], row['cod_luna'], row['cuenta'],
                row['fecha_apertura'], row['fecha_cierre'],
                row['monto_inicial'], row['monto_actual'],
                row['fecha_ultima_actualizacion'],
                row['tiene_deuda_activa'], row['es_cuenta_gestionable']
            )
    
    async def _map_gestiones_to_accounts(self, campaign: CampaignWindow) -> int:
        """
        Map gestiones to specific accounts that were impacted
        
        Logic:
        1. Get all gestiones for this campaign window
        2. For each gestión (cod_luna + timestamp), find active accounts
        3. Create impact records for each account
        """
        
        # Step 1: Get gestiones within campaign window
        gestiones_query = """
        SELECT 
            CAST(cod_luna AS STRING) as cod_luna,
            timestamp_gestion,
            fecha_gestion,
            canal_origen,
            contactabilidad,
            es_contacto_efectivo,
            es_compromiso,
            peso_gestion
        FROM gestiones_unificadas_cache
        WHERE fecha_gestion BETWEEN $1 AND $2
        ORDER BY timestamp_gestion
        """
        
        fecha_fin = campaign.fecha_cierre or date.today()
        gestiones = await execute_query(
            gestiones_query, 
            campaign.fecha_apertura, 
            fecha_fin, 
            fetch="all"
        )
        
        if not gestiones:
            self.logger.info(f"No gestiones found for campaign {campaign.archivo}")
            return 0
        
        # Step 2: For each gestión, find impacted accounts
        impact_records = []
        
        for gestion in gestiones:
            cod_luna = gestion['cod_luna']
            
            # Find active accounts for this cod_luna in this campaign
            accounts_query = """
            SELECT cuenta, monto_actual
            FROM cuenta_campana_state
            WHERE archivo = $1 AND cod_luna = $2 AND es_cuenta_gestionable = TRUE
            """
            
            active_accounts = await execute_query(
                accounts_query, 
                campaign.archivo, 
                cod_luna, 
                fetch="all"
            )
            
            # Create impact record for each active account
            for account in active_accounts:
                impact_records.append({
                    'archivo': campaign.archivo,
                    'cod_luna': cod_luna,
                    'timestamp_gestion': gestion['timestamp_gestion'],
                    'cuenta': account['cuenta'],
                    'canal_origen': gestion['canal_origen'],
                    'contactabilidad': gestion['contactabilidad'],
                    'es_contacto_efectivo': gestion['es_contacto_efectivo'],
                    'es_compromiso': gestion['es_compromiso'],
                    'peso_gestion': gestion['peso_gestion'],
                    'monto_deuda_momento': account['monto_actual'],
                    'es_cuenta_con_deuda': account['monto_actual'] > 0
                })
        
        # Step 3: Bulk insert impact records
        await self._insert_gestion_impacts(impact_records)
        
        return len(impact_records)
    
    async def _insert_gestion_impacts(self, impact_records: List[Dict[str, Any]]) -> None:
        """Bulk insert gestion_cuenta_impact records"""
        
        if not impact_records:
            return
        
        insert_query = """
        INSERT INTO gestion_cuenta_impact (
            archivo, cod_luna, timestamp_gestion, cuenta,
            canal_origen, contactabilidad, es_contacto_efectivo,
            es_compromiso, peso_gestion, monto_deuda_momento,
            es_cuenta_con_deuda, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)
        ON CONFLICT (archivo, cod_luna, timestamp_gestion, cuenta)
        DO UPDATE SET
            contactabilidad = EXCLUDED.contactabilidad,
            es_contacto_efectivo = EXCLUDED.es_contacto_efectivo,
            es_compromiso = EXCLUDED.es_compromiso,
            monto_deuda_momento = EXCLUDED.monto_deuda_momento,
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Execute bulk insert
        for record in impact_records:
            await execute_query(
                insert_query,
                record['archivo'], record['cod_luna'], record['timestamp_gestion'],
                record['cuenta'], record['canal_origen'], record['contactabilidad'],
                record['es_contacto_efectivo'], record['es_compromiso'],
                record['peso_gestion'], record['monto_deuda_momento'],
                record['es_cuenta_con_deuda']
            )
    
    async def _deduplicate_campaign_payments(self, campaign: CampaignWindow) -> int:
        """
        Deduplicate payments for this campaign window
        
        Logic:
        1. Get all payments for accounts in this campaign
        2. Group by (nro_documento, fecha_pago, monto_cancelado)
        3. Mark first occurrence (by fecha_archivo) as unique
        4. Track all occurrences for audit
        """
        
        # Step 1: Get payments for accounts in this campaign
        payments_query = """
        SELECT DISTINCT
            p.nro_documento,
            p.fecha_pago,
            p.monto_cancelado,
            p.fecha_archivo,
            p.cuenta,
            ccs.cod_luna
        FROM raw_pagos_cache p
        INNER JOIN cuenta_campana_state ccs 
            ON p.cuenta = ccs.cuenta 
            AND ccs.archivo = $1
        WHERE p.fecha_archivo >= $2::date
        ORDER BY p.nro_documento, p.fecha_pago, p.monto_cancelado, p.fecha_archivo
        """
        
        payments = await execute_query(
            payments_query,
            campaign.archivo,
            campaign.fecha_apertura,
            fetch="all"
        )
        
        if not payments:
            self.logger.info(f"No payments found for campaign {campaign.archivo}")
            return 0
        
        # Step 2: Process payments with deduplication logic
        df_payments = pd.DataFrame([dict(p) for p in payments])
        
        # Group by business key to identify duplicates
        payment_groups = df_payments.groupby(['nro_documento', 'fecha_pago', 'monto_cancelado'])
        
        dedup_records = []
        
        for (nro_doc, fecha_pago, monto), group in payment_groups:
            # Sort by fecha_archivo to find first occurrence
            group_sorted = group.sort_values('fecha_archivo')
            first_occurrence = group_sorted.iloc[0]
            
            # Check if payment is within campaign window
            esta_en_ventana = (
                campaign.fecha_apertura <= fecha_pago <= 
                (campaign.fecha_cierre or date.today())
            )
            
            dedup_records.append({
                'archivo': campaign.archivo,
                'cuenta': first_occurrence['cuenta'],
                'nro_documento': nro_doc,
                'fecha_pago': fecha_pago,
                'monto_cancelado': monto,
                'es_pago_unico': True,
                'fecha_primera_carga': group_sorted['fecha_archivo'].min(),
                'fecha_ultima_carga': group_sorted['fecha_archivo'].max(),
                'veces_visto': len(group),
                'esta_en_ventana': esta_en_ventana,
                'cod_luna': first_occurrence['cod_luna'],
                'es_pago_valido': True  # Apply business validation here
            })
        
        # Step 3: Bulk upsert dedup records
        await self._upsert_payment_dedup(dedup_records)
        
        return len(dedup_records)
    
    async def _upsert_payment_dedup(self, dedup_records: List[Dict[str, Any]]) -> None:
        """Bulk upsert pago_deduplication records"""
        
        if not dedup_records:
            return
        
        upsert_query = """
        INSERT INTO pago_deduplication (
            archivo, cuenta, nro_documento, fecha_pago, monto_cancelado,
            es_pago_unico, fecha_primera_carga, fecha_ultima_carga,
            veces_visto, esta_en_ventana, cod_luna, es_pago_valido,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP)
        ON CONFLICT (archivo, nro_documento, fecha_pago, monto_cancelado)
        DO UPDATE SET
            fecha_ultima_carga = EXCLUDED.fecha_ultima_carga,
            veces_visto = EXCLUDED.veces_visto,
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Execute bulk upsert
        for record in dedup_records:
            await execute_query(
                upsert_query,
                record['archivo'], record['cuenta'], record['nro_documento'],
                record['fecha_pago'], record['monto_cancelado'],
                record['es_pago_unico'], record['fecha_primera_carga'],
                record['fecha_ultima_carga'], record['veces_visto'],
                record['esta_en_ventana'], record['cod_luna'],
                record['es_pago_valido']
            )
    
    async def calculate_campaign_kpis(self, archivo: str) -> Dict[str, float]:
        """
        Calculate accurate KPIs for a campaign using auxiliary tables
        
        Returns:
            Dictionary with calculated KPI values
        """
        
        # PCT_COBER: % of gestionable accounts that were contacted
        cober_query = """
        SELECT 
            COUNT(DISTINCT ccs.cuenta) as cuentas_gestionables,
            COUNT(DISTINCT gci.cuenta) as cuentas_contactadas
        FROM cuenta_campana_state ccs
        LEFT JOIN gestion_cuenta_impact gci 
            ON ccs.archivo = gci.archivo 
            AND ccs.cuenta = gci.cuenta
            AND gci.es_contacto_efectivo = TRUE
        WHERE ccs.archivo = $1 AND ccs.es_cuenta_gestionable = TRUE
        """
        
        cober_result = await execute_query(cober_query, archivo, fetch="one")
        
        pct_cober = 0.0
        if cober_result and cober_result['cuentas_gestionables'] > 0:
            pct_cober = (cober_result['cuentas_contactadas'] / 
                        cober_result['cuentas_gestionables']) * 100
        
        # PCT_CONTAC: % of gestiones that were effective contact
        contac_query = """
        SELECT 
            COUNT(*) as total_gestiones,
            COUNT(*) FILTER (WHERE es_contacto_efectivo = TRUE) as contactos_efectivos
        FROM gestion_cuenta_impact
        WHERE archivo = $1
        """
        
        contac_result = await execute_query(contac_query, archivo, fetch="one")
        
        pct_contac = 0.0
        if contac_result and contac_result['total_gestiones'] > 0:
            pct_contac = (contac_result['contactos_efectivos'] / 
                         contac_result['total_gestiones']) * 100
        
        # PCT_EFECTIVIDAD: % of effective contacts that generated commitment
        efect_query = """
        SELECT 
            COUNT(*) FILTER (WHERE es_contacto_efectivo = TRUE) as contactos_efectivos,
            COUNT(*) FILTER (WHERE es_contacto_efectivo = TRUE AND es_compromiso = TRUE) as compromisos
        FROM gestion_cuenta_impact
        WHERE archivo = $1
        """
        
        efect_result = await execute_query(efect_query, archivo, fetch="one")
        
        pct_efectividad = 0.0
        if efect_result and efect_result['contactos_efectivos'] > 0:
            pct_efectividad = (efect_result['compromisos'] / 
                              efect_result['contactos_efectivos']) * 100
        
        # RECUPERO: Total unique payments within campaign window
        recupero_query = """
        SELECT COALESCE(SUM(monto_cancelado), 0) as total_recupero
        FROM pago_deduplication
        WHERE archivo = $1 
          AND es_pago_unico = TRUE 
          AND es_pago_valido = TRUE 
          AND esta_en_ventana = TRUE
        """
        
        recupero_result = await execute_query(recupero_query, archivo, fetch="one")
        recupero = float(recupero_result['total_recupero']) if recupero_result else 0.0
        
        return {
            'pct_cober': round(pct_cober, 2),
            'pct_contac': round(pct_contac, 2),
            'pct_efectividad': round(pct_efectividad, 2),
            'recupero': round(recupero, 2)
        }
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.stats.copy()
    
    def reset_stats(self) -> None:
        """Reset processing statistics"""
        self.stats = {
            'campaigns_processed': 0,
            'accounts_mapped': 0,
            'gestiones_attributed': 0,
            'payments_deduplicated': 0,
            'errors': 0
        }


# Global instance for reuse
_business_transformer: Optional[BusinessLogicTransformer] = None

def get_business_transformer() -> BusinessLogicTransformer:
    """Get singleton business transformer instance"""
    global _business_transformer
    
    if _business_transformer is None:
        _business_transformer = BusinessLogicTransformer()
    
    return _business_transformer
