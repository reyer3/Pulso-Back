# etl/pipelines/aux_build_pipeline.py

"""
🏗️ AUX BUILD PIPELINE: Capa intermedia de transformaciones

Este pipeline construye la capa AUX (auxiliar) que actúa como bridge entre RAW y MART.

FUNCIONALIDADES:
- Limpieza y normalización de datos RAW
- Joins y unificaciones de múltiples fuentes
- Agregaciones temporales (diarias, semanales)
- Deduplicación y validaciones de calidad

DEPENDENCIAS:
- RAW layer debe estar cargado
- Esquemas aux_P3fV4dWNeMkN5RJMhV8e deben existir
"""

import asyncio
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from shared.core.logging import LoggerMixin
from shared.database.connection import DatabaseManager
from etl.models import CampaignWindow, TableLoadResult


class AuxBuildPipeline(LoggerMixin):
    """
    Pipeline para construir capa AUX (auxiliar/intermedia)
    
    🎯 PROPÓSITO:
    - Unificar datos de múltiples tablas RAW
    - Limpiar y normalizar datos
    - Crear agregaciones temporales
    - Preparar datos para MART layer
    """

    def __init__(self, db_manager: DatabaseManager, project_uid: str):
        super().__init__().__init__()
        self.db = db_manager
        self.uid = project_uid
        self.sql_path = Path(__file__).parent.parent / "sql" / "aux"
        
        # Esquemas de trabajo
        self.schemas = {
            'raw': f"raw_{self.uid}",
            'aux': f"aux_{self.uid}",
            'mart': f"mart_{self.uid}"
        }
        
        # Mapeo de archivos AUX y sus dependencias
        self.aux_files = {
            "gestiones_unificadas": {
                "file": "build_gestiones_unificadas.sql",
                "dependencies": ["voicebot_gestiones", "mibotair_gestiones", "homologacion_voicebot", "homologacion_mibotair"],
                "description": "Unifica gestiones de voicebot y mibotair con homologaciones"
            },
            "gestiones_diarias": {
                "file": "build_gestiones_diarias.sql", 
                "dependencies": ["gestiones_unificadas"],
                "description": "Agregaciones diarias de gestiones unificadas"
            },
            "pagos_diarios": {
                "file": "build_pagos_diarios.sql",
                "dependencies": ["pagos"],
                "description": "Agregaciones diarias de pagos"
            }
        }

    async def _execute_sql_from_file(self, file_name: str, **params) -> Dict[str, Any]:
        """
        Ejecuta un archivo SQL con parámetros y manejo de errores robusto.
        
        Args:
            file_name: Nombre del archivo SQL en etl/sql/aux/
            **params: Parámetros adicionales para el SQL
            
        Returns:
            Dict con resultado de la ejecución
        """
        start_time = datetime.now()
        
        try:
            file_path = self.sql_path / file_name
            
            if not file_path.exists():
                raise FileNotFoundError(f"SQL file not found: {file_path}")
                
            # Leer y formatear SQL
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
                
            # Verificar que no esté vacío
            if not sql_content.strip():
                self.logger.warning(f"⚠️ SQL file {file_name} is empty")
                return {
                    "status": "skipped",
                    "message": "Empty SQL file",
                    "duration_seconds": 0,
                    "rows_affected": 0
                }
            
            # Formatear con esquemas y parámetros
            formatted_sql = sql_content.format(**self.schemas, **params)
            
            self.logger.info(f"🔧 Executing {file_name}...")
            
            # Ejecutar SQL
            result = await self.db.execute_query(formatted_sql, fetch="none")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"✅ {file_name} completed in {duration:.2f}s")
            
            return {
                "status": "success",
                "message": f"Executed successfully",
                "duration_seconds": duration,
                "rows_affected": getattr(result, 'rowcount', 0) if result else 0
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Failed to execute {file_name}: {str(e)}"
            
            self.logger.error(error_msg)
            
            return {
                "status": "failed",
                "message": error_msg,
                "duration_seconds": duration,
                "rows_affected": 0
            }

    async def _check_raw_dependencies(self, campaign: CampaignWindow) -> bool:
        """
        Verifica que las tablas RAW requeridas tengan datos para la campaña.
        
        Args:
            campaign: Información de la campaña
            
        Returns:
            True si todas las dependencias están satisfechas
        """
        self.logger.info(f"🔍 Checking RAW dependencies for campaign {campaign.archivo}")
        
        # Tablas RAW críticas
        critical_tables = [
            "asignaciones",
            "voicebot_gestiones", 
            "mibotair_gestiones",
            "homologacion_voicebot",
            "homologacion_mibotair",
            "pagos"
        ]
        
        missing_data = []
        
        for table in critical_tables:
            try:
                # Check si la tabla tiene datos para la campaña
                check_query = f"""
                SELECT COUNT(*) as count 
                FROM {self.schemas['raw']}.{table} 
                WHERE archivo = $1
                """
                
                result = await self.db.execute_query(check_query, campaign.archivo)
                count = result[0]['count'] if result else 0
                
                if count == 0:
                    missing_data.append(f"{table} (0 records)")
                else:
                    self.logger.debug(f"✅ {table}: {count:,} records")
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check {table}: {e}")
                missing_data.append(f"{table} (check failed)")
        
        if missing_data:
            self.logger.error(f"❌ Missing RAW data: {missing_data}")
            return False
            
        self.logger.info("✅ All RAW dependencies satisfied")
        return True

    async def run_for_campaign(self, campaign: CampaignWindow) -> Dict[str, Any]:
        """
        🎯 Ejecuta el pipeline AUX completo para una campaña.
        
        Args:
            campaign: Información de la campaña
            
        Returns:
            Dict con resumen de la ejecución
        """
        start_time = datetime.now()
        
        self.logger.info(f"🏗️ Starting AUX Build Pipeline for campaign '{campaign.archivo}'")
        
        try:
            # 1. Verificar dependencias RAW
            if not await self._check_raw_dependencies(campaign):
                raise Exception("RAW dependencies not satisfied")
            
            # 2. Limpiar datos existentes de AUX para la campaña (idempotencia)
            await self._cleanup_campaign_aux_data(campaign.archivo)
            
            # 3. Ejecutar archivos AUX en orden de dependencias
            aux_results = {}
            
            # Orden de ejecución basado en dependencias
            execution_order = [
                "gestiones_unificadas",  # Primero: unifica datos base
                "gestiones_diarias",     # Segundo: agrega gestiones
                "pagos_diarios"          # Tercero: agrega pagos
            ]
            
            for aux_name in execution_order:
                if aux_name in self.aux_files:
                    aux_config = self.aux_files[aux_name]
                    
                    self.logger.info(f"🔧 Building {aux_name}: {aux_config['description']}")
                    
                    result = await self._execute_sql_from_file(
                        aux_config["file"],
                        campaign_archivo=campaign.archivo,
                        fecha_inicio=campaign.fecha_apertura,
                        fecha_fin=campaign.fecha_cierre or date.today()
                    )
                    
                    aux_results[aux_name] = result
                    
                    if result["status"] == "failed":
                        raise Exception(f"AUX step {aux_name} failed: {result['message']}")
            
            # 4. Calcular estadísticas finales
            duration = (datetime.now() - start_time).total_seconds()
            successful_steps = sum(1 for r in aux_results.values() if r["status"] == "success")
            total_rows = sum(r["rows_affected"] for r in aux_results.values())
            
            summary = {
                "status": "success",
                "campaign": campaign.archivo,
                "duration_seconds": duration,
                "successful_steps": successful_steps,
                "total_steps": len(aux_results),
                "total_rows_processed": total_rows,
                "step_results": aux_results
            }
            
            self.logger.info(
                f"✅ AUX Pipeline completed for '{campaign.archivo}': "
                f"{successful_steps}/{len(aux_results)} steps, "
                f"{total_rows:,} rows, {duration:.2f}s"
            )
            
            return summary
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"AUX Pipeline failed for '{campaign.archivo}': {str(e)}"
            
            self.logger.error(error_msg)
            
            return {
                "status": "failed",
                "campaign": campaign.archivo,
                "duration_seconds": duration,
                "error_message": error_msg,
                "step_results": aux_results if 'aux_results' in locals() else {}
            }

    async def _cleanup_campaign_aux_data(self, campaign_archivo: str):
        """
        Limpia datos AUX existentes para la campaña (idempotencia).
        
        Args:
            campaign_archivo: Archivo de campaña a limpiar
        """
        self.logger.info(f"🧹 Cleaning existing AUX data for campaign {campaign_archivo}")
        
        # Tablas AUX que necesitan limpieza por campaña
        aux_tables_to_clean = [
            "gestiones_unificadas",
            "gestiones_diarias", 
            "pagos_diarios"
        ]
        
        for table in aux_tables_to_clean:
            try:
                cleanup_query = f"""
                DELETE FROM {self.schemas['aux']}.{table} 
                WHERE archivo = $1
                """
                
                result = await self.db.execute_query(cleanup_query, campaign_archivo, fetch="none")
                rows_deleted = getattr(result, 'rowcount', 0) if result else 0
                
                if rows_deleted > 0:
                    self.logger.debug(f"🧹 Cleaned {rows_deleted:,} rows from {table}")
                    
            except Exception as e:
                # Log warning but continue - table might not exist yet
                self.logger.warning(f"⚠️ Could not clean {table}: {e}")

    async def validate_aux_output(self, campaign: CampaignWindow) -> Dict[str, Any]:
        """
        🔍 Valida que la salida del pipeline AUX sea correcta.
        
        Args:
            campaign: Información de la campaña
            
        Returns:
            Dict con resultados de validación
        """
        self.logger.info(f"🔍 Validating AUX output for campaign {campaign.archivo}")
        
        validation_results = {}
        
        # Validaciones por tabla AUX
        validations = {
            "gestiones_unificadas": {
                "min_records": 1,
                "required_columns": ["archivo", "fecha_gestion", "cod_luna"],
                "description": "Unified management records"
            },
            "gestiones_diarias": {
                "min_records": 1,
                "required_columns": ["archivo", "fecha", "total_gestiones"],
                "description": "Daily management aggregations"
            },
            "pagos_diarios": {
                "min_records": 0,  # Puede ser 0 si no hay pagos
                "required_columns": ["archivo", "fecha", "total_pagos"],
                "description": "Daily payment aggregations"
            }
        }
        
        for table_name, validation_config in validations.items():
            try:
                # Count records
                count_query = f"""
                SELECT COUNT(*) as count 
                FROM {self.schemas['aux']}.{table_name} 
                WHERE archivo = $1
                """
                
                result = await self.db.execute_query(count_query, campaign.archivo)
                record_count = result[0]['count'] if result else 0
                
                # Check minimum records
                min_records_ok = record_count >= validation_config["min_records"]
                
                # Check column structure (sample first row)
                structure_ok = True
                missing_columns = []
                
                if record_count > 0:
                    structure_query = f"""
                    SELECT * FROM {self.schemas['aux']}.{table_name} 
                    WHERE archivo = $1 
                    LIMIT 1
                    """
                    
                    sample_result = await self.db.execute_query(structure_query, campaign.archivo)
                    
                    if sample_result:
                        available_columns = list(sample_result[0].keys())
                        missing_columns = [
                            col for col in validation_config["required_columns"] 
                            if col not in available_columns
                        ]
                        structure_ok = len(missing_columns) == 0
                
                validation_results[table_name] = {
                    "record_count": record_count,
                    "min_records_ok": min_records_ok,
                    "structure_ok": structure_ok,
                    "missing_columns": missing_columns,
                    "status": "pass" if min_records_ok and structure_ok else "fail"
                }
                
                status_emoji = "✅" if validation_results[table_name]["status"] == "pass" else "❌"
                self.logger.info(
                    f"{status_emoji} {table_name}: {record_count:,} records, "
                    f"structure {'OK' if structure_ok else 'FAIL'}"
                )
                
            except Exception as e:
                validation_results[table_name] = {
                    "status": "error",
                    "error_message": str(e)
                }
                self.logger.error(f"❌ Validation failed for {table_name}: {e}")
        
        # Summary
        passed_validations = sum(1 for v in validation_results.values() if v.get("status") == "pass")
        total_validations = len(validation_results)
        
        overall_status = "pass" if passed_validations == total_validations else "fail"
        
        summary = {
            "overall_status": overall_status,
            "passed_validations": passed_validations,
            "total_validations": total_validations,
            "table_validations": validation_results
        }
        
        self.logger.info(
            f"🔍 AUX Validation: {passed_validations}/{total_validations} passed, "
            f"status: {overall_status.upper()}"
        )
        
        return summary