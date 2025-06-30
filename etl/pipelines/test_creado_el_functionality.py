#!/usr/bin/env python3
# etl/pipelines/test_creado_el_functionality.py

"""
🧪 SCRIPT DE TESTING: Funcionalidad de filtrado por creado_el

Este script valida que los métodos de filtrado por creado_el funcionen correctamente
después de la integración manual.

EJECUCIÓN:
python etl/pipelines/test_creado_el_functionality.py

REQUISITOS:
- Métodos integrados en HybridRawDataPipeline
- Conexión a bases de datos configurada
- Variables de entorno configuradas
"""

import asyncio
import sys
from datetime import date, datetime, timedelta
from typing import List

# Añadir path si es necesario
sys.path.append('.')

from etl.dependencies import etl_dependencies
from shared.core.logging import get_logger

logger = get_logger(__name__)

class CreadoElTester:
    """Tester para funcionalidad de filtrado por creado_el"""
    
    def __init__(self):
        self.pipeline = None
        self.test_results = []
        
    async def setup(self):
        """Inicializar recursos de testing"""
        logger.info("🔧 Setting up test environment...")
        await etl_dependencies.init_resources()
        self.pipeline = await etl_dependencies.hybrid_raw_pipeline()
        logger.info("✅ Test environment ready")
        
    async def teardown(self):
        """Limpiar recursos de testing"""
        logger.info("🧹 Cleaning up test environment...")
        await etl_dependencies.shutdown_resources()
        logger.info("✅ Test environment cleaned")
        
    def log_test_result(self, test_name: str, success: bool, message: str = ""):
        """Log resultado de test"""
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} {test_name}: {message}")
        self.test_results.append({
            "test": test_name,
            "success": success,
            "message": message
        })
        
    async def test_1_enum_availability(self):
        """Test 1: Verificar que ExtractionStrategy.CREADO_EL_FILTER existe"""
        test_name = "ExtractionStrategy.CREADO_EL_FILTER availability"
        try:
            from etl.pipelines.raw_data_pipeline import ExtractionStrategy
            has_creado_el = hasattr(ExtractionStrategy, 'CREADO_EL_FILTER')
            
            if has_creado_el:
                value = ExtractionStrategy.CREADO_EL_FILTER.value
                self.log_test_result(test_name, True, f"Found with value: {value}")
            else:
                self.log_test_result(test_name, False, "CREADO_EL_FILTER not found in enum")
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Error: {e}")
            
    async def test_2_methods_availability(self):
        """Test 2: Verificar que los métodos existen en el pipeline"""
        test_name = "Pipeline methods availability"
        try:
            required_methods = [
                '_build_creado_el_filter_query',
                'extract_table_by_creado_el_range',
                'extract_multiple_tables_by_creado_el_range',
                'get_tables_with_creado_el'
            ]
            
            missing_methods = []
            for method in required_methods:
                if not hasattr(self.pipeline, method):
                    missing_methods.append(method)
                    
            if not missing_methods:
                self.log_test_result(test_name, True, f"All {len(required_methods)} methods found")
            else:
                self.log_test_result(test_name, False, f"Missing methods: {missing_methods}")
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Error: {e}")
            
    async def test_3_valid_tables_detection(self):
        """Test 3: Verificar detección de tablas válidas"""
        test_name = "Valid tables detection"
        try:
            if not hasattr(self.pipeline, 'get_tables_with_creado_el'):
                self.log_test_result(test_name, False, "Method get_tables_with_creado_el not found")
                return
                
            valid_tables = self.pipeline.get_tables_with_creado_el()
            expected_tables = ['asignaciones', 'trandeuda', 'pagos']
            
            found_expected = [t for t in expected_tables if t in valid_tables]
            
            if len(found_expected) >= 2:  # Al menos 2 de las 3 esperadas
                self.log_test_result(test_name, True, f"Found valid tables: {valid_tables}")
            else:
                self.log_test_result(test_name, False, f"Expected at least 2 of {expected_tables}, got: {valid_tables}")
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Error: {e}")
            
    async def test_4_query_building(self):
        """Test 4: Verificar construcción de queries"""
        test_name = "Query building"
        try:
            if not hasattr(self.pipeline, '_build_creado_el_filter_query'):
                self.log_test_result(test_name, False, "Method _build_creado_el_filter_query not found")
                return
                
            # Obtener una tabla válida
            valid_tables = self.pipeline.get_tables_with_creado_el()
            if not valid_tables:
                self.log_test_result(test_name, False, "No valid tables found for testing")
                return
                
            test_table = valid_tables[0]
            test_start = date(2024, 12, 1)
            test_end = date(2024, 12, 7)
            
            # Test con timestamps
            query_with_ts = await self.pipeline._build_creado_el_filter_query(
                test_table, test_start, test_end, include_timestamps=True
            )
            
            # Test sin timestamps  
            query_without_ts = await self.pipeline._build_creado_el_filter_query(
                test_table, test_start, test_end, include_timestamps=False
            )
            
            # Verificar que las queries contienen los filtros esperados
            has_timestamp_filter = "TIMESTAMP(" in query_with_ts
            has_date_filter = "DATE(creado_el)" in query_without_ts
            
            if has_timestamp_filter and has_date_filter:
                self.log_test_result(test_name, True, f"Both query types built successfully for {test_table}")
            else:
                self.log_test_result(test_name, False, f"Query filters not found correctly")
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Error: {e}")
            
    async def test_5_invalid_table_handling(self):
        """Test 5: Verificar manejo de tablas inválidas"""
        test_name = "Invalid table handling"
        try:
            if not hasattr(self.pipeline, '_build_creado_el_filter_query'):
                self.log_test_result(test_name, False, "Method not found")
                return
                
            # Intentar con tabla que no tiene creado_el
            invalid_table = "ejecutivos"  # Esta tabla usa otra columna incremental
            test_start = date(2024, 12, 1)
            test_end = date(2024, 12, 7)
            
            try:
                await self.pipeline._build_creado_el_filter_query(
                    invalid_table, test_start, test_end
                )
                # Si llega aquí, no detectó el error
                self.log_test_result(test_name, False, "Should have raised ValueError for invalid table")
                
            except ValueError as ve:
                # Esperado: debe dar error por tabla inválida
                if "does not use 'creado_el'" in str(ve):
                    self.log_test_result(test_name, True, "Correctly rejected invalid table")
                else:
                    self.log_test_result(test_name, False, f"Unexpected ValueError: {ve}")
                    
            except Exception as e:
                self.log_test_result(test_name, False, f"Unexpected error type: {e}")
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Setup error: {e}")
            
    async def test_6_dry_run_extraction(self):
        """Test 6: Dry run de extracción real (solo validación, sin cargar datos)"""
        test_name = "Dry run extraction validation"
        try:
            if not hasattr(self.pipeline, 'extract_table_by_creado_el_range'):
                self.log_test_result(test_name, False, "Method extract_table_by_creado_el_range not found")
                return
                
            # Obtener tabla válida
            valid_tables = self.pipeline.get_tables_with_creado_el()
            if not valid_tables:
                self.log_test_result(test_name, False, "No valid tables for dry run")
                return
                
            test_table = valid_tables[0]
            
            # Usar fechas muy recientes (último día) para minimizar datos
            yesterday = date.today() - timedelta(days=1)
            
            # Solo construir query, no ejecutar extracción completa
            query = await self.pipeline._build_creado_el_filter_query(
                test_table, yesterday, yesterday, include_timestamps=False
            )
            
            # Verificar que query se ve razonable
            required_elements = [
                "SELECT",
                "FROM", 
                "WHERE",
                "creado_el",
                test_table
            ]
            
            query_upper = query.upper()
            missing_elements = [elem for elem in required_elements if elem.upper() not in query_upper]
            
            if not missing_elements:
                self.log_test_result(test_name, True, f"Query structure validated for {test_table}")
            else:
                self.log_test_result(test_name, False, f"Query missing elements: {missing_elements}")
                
        except Exception as e:
            self.log_test_result(test_name, False, f"Error: {e}")
            
    async def run_all_tests(self):
        """Ejecutar todos los tests"""
        logger.info("🧪 Starting creado_el functionality tests...")
        logger.info("=" * 60)
        
        tests = [
            self.test_1_enum_availability,
            self.test_2_methods_availability, 
            self.test_3_valid_tables_detection,
            self.test_4_query_building,
            self.test_5_invalid_table_handling,
            self.test_6_dry_run_extraction
        ]
        
        for i, test in enumerate(tests, 1):
            logger.info(f"\n🧪 Running test {i}/{len(tests)}: {test.__name__}")
            await test()
            
        # Resumen final
        logger.info("\n" + "=" * 60)
        logger.info("📊 TEST RESULTS SUMMARY")
        logger.info("=" * 60)
        
        passed = sum(1 for r in self.test_results if r['success'])
        failed = len(self.test_results) - passed
        
        for result in self.test_results:
            status = "✅" if result['success'] else "❌"
            logger.info(f"{status} {result['test']}")
            if not result['success'] and result['message']:
                logger.info(f"    └─ {result['message']}")
                
        logger.info(f"\n📈 Summary: {passed}/{len(self.test_results)} tests passed")
        
        if failed > 0:
            logger.warning(f"⚠️ {failed} tests failed - check integration steps")
            return False
        else:
            logger.info("🎉 All tests passed! creado_el functionality is ready")
            return True

async def main():
    """Función principal del tester"""
    tester = CreadoElTester()
    
    try:
        await tester.setup()
        success = await tester.run_all_tests()
        
        if success:
            logger.info("\n✅ creado_el functionality validation completed successfully")
            logger.info("🚀 You can now use the new filtering methods")
            return 0
        else:
            logger.error("\n❌ Some tests failed - please check the integration")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Test runner failed: {e}", exc_info=True)
        return 1
        
    finally:
        try:
            await tester.teardown()
        except:
            pass

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)