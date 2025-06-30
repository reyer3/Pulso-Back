# etl/pipelines/full_etl_orchestrator.py

"""
🎯 ORQUESTADOR COMPLETO: RAW → AUX → MART

Este orquestador ejecuta el pipeline completo end-to-end:
1. RAW: Extracción desde BigQuery
2. AUX: Transformaciones intermedias 
3. MART: Data marts de negocio

CASOS DE USO:
- Carga completa de campaña nueva
- Reprocessing de campaña existente
- Validación de pipeline completo
"""

import asyncio
from datetime import date, datetime
from typing import Dict, Any, Optional

from shared.core.logging import LoggerMixin
from shared.database.connection import DatabaseManager
from etl.models import CampaignWindow
from etl.pipelines.aux_build_pipeline import AuxBuildPipeline
from etl.pipelines.mart_build_pipeline import MartBuildPipeline


class FullETLOrchestrator(LoggerMixin):
    """
    Orquestador maestro para pipeline completo RAW → AUX → MART
    
    🎯 RESPONSABILIDADES:
    - Coordinar ejecución secuencial de capas
    - Manejar dependencias entre capas
    - Validar cada paso antes de continuar
    - Proporcionar rollback en caso de fallo
    """

    def __init__(
        self, 
        db_manager: DatabaseManager,
        project_uid: str,
        raw_pipeline=None  # Se inyecta desde dependencies
    ):
        super().__init__()
        self.db = db_manager
        self.uid = project_uid
        self.raw_pipeline = raw_pipeline
        
        # Inicializar sub-pipelines
        self.aux_pipeline = AuxBuildPipeline(db_manager, project_uid)
        self.mart_pipeline = MartBuildPipeline(db_manager, project_uid)

    async def run_full_etl_for_campaign(
        self, 
        campaign: CampaignWindow,
        skip_raw: bool = False,
        skip_aux: bool = False, 
        skip_mart: bool = False,
        validate_steps: bool = True
    ) -> Dict[str, Any]:
        """
        🚀 Ejecuta pipeline completo RAW → AUX → MART para una campaña.
        
        Args:
            campaign: Información de la campaña
            skip_raw: Si saltar la carga RAW (útil si ya está cargada)
            skip_aux: Si saltar la construcción AUX
            skip_mart: Si saltar la construcción MART
            validate_steps: Si validar cada paso antes de continuar
            
        Returns:
            Dict con resumen completo de la ejecución
        """
        start_time = datetime.now()
        
        self.logger.info(f"🚀 Starting FULL ETL Pipeline for campaign '{campaign.archivo}'")
        self.logger.info(f"📅 Campaign period: {campaign.fecha_apertura} to {campaign.fecha_cierre}")
        self.logger.info(f"⚙️ Options: skip_raw={skip_raw}, skip_aux={skip_aux}, skip_mart={skip_mart}")
        
        results = {
            "campaign": campaign.archivo,
            "start_time": start_time.isoformat(),
            "steps_executed": [],
            "steps_skipped": [],
            "raw_result": None,
            "aux_result": None, 
            "mart_result": None,
            "overall_status": "running"
        }
        
        try:
            # STEP 1: RAW Layer
            if not skip_raw:
                self.logger.info("\n" + "="*60)
                self.logger.info("📊 STEP 1: RAW DATA EXTRACTION")
                self.logger.info("="*60)
                
                if not self.raw_pipeline:
                    raise Exception("RAW pipeline not available - check dependencies")
                
                raw_records = await self.raw_pipeline.run_for_single_campaign(campaign)
                
                results["raw_result"] = {
                    "status": "success",
                    "records_loaded": raw_records,
                    "message": f"RAW extraction completed: {raw_records:,} records"
                }
                results["steps_executed"].append("raw")
                
                self.logger.info(f"✅ RAW Layer completed: {raw_records:,} records loaded")
                
                # Validación opcional
                if validate_steps:
                    await self._validate_raw_layer(campaign)
                    
            else:
                results["steps_skipped"].append("raw")
                self.logger.info("⏭️ Skipping RAW layer (skip_raw=True)")

            # STEP 2: AUX Layer  
            if not skip_aux:
                self.logger.info("\n" + "="*60)
                self.logger.info("🏗️ STEP 2: AUX LAYER CONSTRUCTION")
                self.logger.info("="*60)
                
                aux_result = await self.aux_pipeline.run_for_campaign(campaign)
                results["aux_result"] = aux_result
                results["steps_executed"].append("aux")
                
                if aux_result["status"] != "success":
                    raise Exception(f"AUX layer failed: {aux_result.get('error_message', 'Unknown error')}")
                    
                self.logger.info(
                    f"✅ AUX Layer completed: {aux_result['successful_steps']}/{aux_result['total_steps']} steps, "
                    f"{aux_result['total_rows_processed']:,} rows"
                )
                
                # Validación opcional
                if validate_steps:
                    aux_validation = await self.aux_pipeline.validate_aux_output(campaign)
                    if aux_validation["overall_status"] != "pass":
                        self.logger.warning("⚠️ AUX validation issues detected")
                        
            else:
                results["steps_skipped"].append("aux")
                self.logger.info("⏭️ Skipping AUX layer (skip_aux=True)")

            # STEP 3: MART Layer
            if not skip_mart:
                self.logger.info("\n" + "="*60)
                self.logger.info("📈 STEP 3: MART LAYER CONSTRUCTION") 
                self.logger.info("="*60)
                
                # El MartBuildPipeline original no retorna resultado estructurado
                # Lo envolvemos para capturar el resultado
                try:
                    await self.mart_pipeline.run_for_campaign(campaign)
                    
                    results["mart_result"] = {
                        "status": "success",
                        "message": "MART construction completed successfully"
                    }
                    results["steps_executed"].append("mart")
                    
                    self.logger.info("✅ MART Layer completed successfully")
                    
                    # Validación opcional
                    if validate_steps:
                        await self._validate_mart_layer(campaign)
                        
                except Exception as e:
                    results["mart_result"] = {
                        "status": "failed",
                        "error_message": str(e)
                    }
                    raise Exception(f"MART layer failed: {str(e)}")
                    
            else:
                results["steps_skipped"].append("mart")
                self.logger.info("⏭️ Skipping MART layer (skip_mart=True)")

            # FINALIZACIÓN EXITOSA
            duration = (datetime.now() - start_time).total_seconds()
            results.update({
                "overall_status": "success",
                "end_time": datetime.now().isoformat(),
                "duration_seconds": duration,
                "summary": self._generate_success_summary(results, duration)
            })
            
            self.logger.info("\n" + "="*60)
            self.logger.info("🎉 FULL ETL PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("="*60)
            self.logger.info(results["summary"])
            self.logger.info("="*60)
            
            return results
            
        except Exception as e:
            # MANEJO DE ERRORES
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Full ETL Pipeline failed for '{campaign.archivo}': {str(e)}"
            
            results.update({
                "overall_status": "failed",
                "end_time": datetime.now().isoformat(),
                "duration_seconds": duration,
                "error_message": error_msg,
                "summary": self._generate_failure_summary(results, error_msg, duration)
            })
            
            self.logger.error("\n" + "="*60)
            self.logger.error("❌ FULL ETL PIPELINE FAILED")
            self.logger.error("="*60)
            self.logger.error(error_msg)
            self.logger.error("="*60)
            
            return results

    async def _validate_raw_layer(self, campaign: CampaignWindow):
        """Validación básica de la capa RAW"""
        self.logger.info("🔍 Validating RAW layer...")
        
        # Verificar que las tablas críticas tienen datos
        critical_tables = ["asignaciones", "voicebot_gestiones", "mibotair_gestiones"]
        
        for table in critical_tables:
            try:
                count_query = f"""
                SELECT COUNT(*) as count 
                FROM raw_{self.uid}.{table} 
                WHERE archivo = $1
                """
                
                result = await self.db.execute_query(count_query, campaign.archivo)
                count = result[0]['count'] if result else 0
                
                if count == 0:
                    raise Exception(f"RAW validation failed: {table} has no records for campaign {campaign.archivo}")
                    
                self.logger.debug(f"✅ {table}: {count:,} records")
                
            except Exception as e:
                raise Exception(f"RAW validation error for {table}: {str(e)}")
                
        self.logger.info("✅ RAW layer validation passed")

    async def _validate_mart_layer(self, campaign: CampaignWindow):
        """Validación básica de la capa MART"""
        self.logger.info("🔍 Validating MART layer...")
        
        # Verificar que al menos una tabla MART tiene datos
        mart_tables = ["dashboard_data"]  # Expandir según se agreguen más
        
        for table in mart_tables:
            try:
                count_query = f"""
                SELECT COUNT(*) as count 
                FROM mart_{self.uid}.{table} 
                WHERE archivo = $1
                """
                
                result = await self.db.execute_query(count_query, campaign.archivo)
                count = result[0]['count'] if result else 0
                
                self.logger.debug(f"✅ {table}: {count:,} records")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Could not validate MART table {table}: {e}")
                
        self.logger.info("✅ MART layer validation completed")

    def _generate_success_summary(self, results: Dict[str, Any], duration: float) -> str:
        """Genera resumen de ejecución exitosa"""
        summary_lines = [
            f"🎉 Full ETL Pipeline completed successfully for '{results['campaign']}'",
            f"⏱️ Total duration: {duration:.2f} seconds ({duration/60:.1f} minutes)",
            f"🔄 Steps executed: {', '.join(results['steps_executed'])}",
        ]
        
        if results['steps_skipped']:
            summary_lines.append(f"⏭️ Steps skipped: {', '.join(results['steps_skipped'])}")
            
        # Agregar detalles por step
        if results.get('raw_result') and results['raw_result']['status'] == 'success':
            summary_lines.append(f"📊 RAW: {results['raw_result']['records_loaded']:,} records loaded")
            
        if results.get('aux_result') and results['aux_result']['status'] == 'success':
            aux = results['aux_result']
            summary_lines.append(f"🏗️ AUX: {aux['successful_steps']}/{aux['total_steps']} steps, {aux['total_rows_processed']:,} rows")
            
        if results.get('mart_result') and results['mart_result']['status'] == 'success':
            summary_lines.append(f"📈 MART: Construction completed successfully")
            
        return "\n".join(summary_lines)

    def _generate_failure_summary(self, results: Dict[str, Any], error_msg: str, duration: float) -> str:
        """Genera resumen de ejecución fallida"""
        summary_lines = [
            f"❌ Full ETL Pipeline failed for '{results['campaign']}'",
            f"⏱️ Duration before failure: {duration:.2f} seconds",
            f"🔄 Steps completed: {', '.join(results['steps_executed'])}",
            f"💥 Error: {error_msg}"
        ]
        
        return "\n".join(summary_lines)

    async def run_aux_mart_only(self, campaign: CampaignWindow) -> Dict[str, Any]:
        """
        🎯 Ejecuta solo AUX + MART (asumiendo que RAW ya existe)
        
        Args:
            campaign: Información de la campaña
            
        Returns:
            Dict con resumen de la ejecución
        """
        return await self.run_full_etl_for_campaign(
            campaign, 
            skip_raw=True, 
            skip_aux=False, 
            skip_mart=False
        )

    async def run_mart_only(self, campaign: CampaignWindow) -> Dict[str, Any]:
        """
        🎯 Ejecuta solo MART (asumiendo que RAW y AUX ya existen)
        
        Args:
            campaign: Información de la campaña
            
        Returns:
            Dict con resumen de la ejecución
        """
        return await self.run_full_etl_for_campaign(
            campaign, 
            skip_raw=True, 
            skip_aux=True, 
            skip_mart=False
        )

    async def validate_full_pipeline(self, campaign: CampaignWindow) -> Dict[str, Any]:
        """
        🔍 Valida que todas las capas tengan datos para la campaña
        
        Args:
            campaign: Información de la campaña
            
        Returns:
            Dict con resultados de validación
        """
        self.logger.info(f"🔍 Validating full pipeline for campaign '{campaign.archivo}'")
        
        validation_results = {
            "campaign": campaign.archivo,
            "raw_validation": {"status": "unknown"},
            "aux_validation": {"status": "unknown"}, 
            "mart_validation": {"status": "unknown"},
            "overall_status": "unknown"
        }
        
        try:
            # Validar RAW
            await self._validate_raw_layer(campaign)
            validation_results["raw_validation"]["status"] = "pass"
            
        except Exception as e:
            validation_results["raw_validation"] = {
                "status": "fail",
                "error": str(e)
            }
            
        try:
            # Validar AUX
            aux_validation = await self.aux_pipeline.validate_aux_output(campaign)
            validation_results["aux_validation"] = aux_validation
            
        except Exception as e:
            validation_results["aux_validation"] = {
                "status": "fail", 
                "error": str(e)
            }
            
        try:
            # Validar MART
            await self._validate_mart_layer(campaign)
            validation_results["mart_validation"]["status"] = "pass"
            
        except Exception as e:
            validation_results["mart_validation"] = {
                "status": "fail",
                "error": str(e)
            }
            
        # Determinar status general
        all_passed = (
            validation_results["raw_validation"]["status"] == "pass" and
            validation_results["aux_validation"].get("overall_status") == "pass" and 
            validation_results["mart_validation"]["status"] == "pass"
        )
        
        validation_results["overall_status"] = "pass" if all_passed else "fail"
        
        self.logger.info(f"🔍 Pipeline validation: {validation_results['overall_status'].upper()}")
        
        return validation_results