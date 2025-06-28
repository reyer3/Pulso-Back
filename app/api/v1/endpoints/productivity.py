"""
⚡ Endpoints API V1 para Análisis de Productividad
Endpoints FastAPI para monitoreo de productividad y rendimiento de agentes.
"""

# Imports estándar
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

# Imports de terceros
from fastapi import APIRouter, Depends, HTTPException, Query
# BaseModel y Field ya no son necesarios aquí directamente si todos los modelos se importan.

# Imports internos
from app.core.dependencies import get_dashboard_service # Asegúrate que existe o crea get_productivity_service si es específico
from app.core.logging import LoggerMixin
# from app.repositories.data_adapters import DataSourceFactory, DataSourceAdapter # Comentado si no se usa directamente
from app.services.dashboard_service_v2 import DashboardServiceV2 # Asumiendo que este servicio maneja productividad
from app.models.productivity import (
    ProductivityRequest,
    ProductivityResponse,
    # Los siguientes modelos son componentes de ProductivityResponse y no necesitan ser importados
    # directamente en el endpoint si solo se usa ProductivityResponse como response_model.
    # AgentDailyPerformance, # Componente de AgentHeatmapRow
    # AgentHeatmapRow,       # Componente de ProductivityResponse
    # ProductivityTrendPoint,# Componente de ProductivityResponse
    # AgentRankingRow        # Componente de ProductivityResponse
)


# =============================================================================
# CONFIGURACIÓN DEL ROUTER
# =============================================================================

# El prefijo se cambia a solo "/productivity" si el "/api/v1" ya está en un router padre.
# Si este es el router principal para v1 de productividad, "/api/v1/productivity" podría ser más explícito.
# Por coherencia con el ticket original, se mantiene "/productivity" y se asume un router padre para "/api/v1".
router = APIRouter(prefix="/productivity", tags=["Productividad"])


class ProductivityAPI(LoggerMixin):
    """
    Endpoints de la API de Productividad para análisis de rendimiento de agentes.
    """
    
    @router.post("/", response_model=ProductivityResponse) # Ruta simplificada a "/" ya que el prefijo está en el router
    async def get_productivity_data_post_endpoint( # Renombrado para claridad
        request: ProductivityRequest,
        service: DashboardServiceV2 = Depends(get_dashboard_service) # Usar el servicio adecuado
    ) -> ProductivityResponse:
        """
        Obtiene datos de análisis de productividad.
        
        Proporciona métricas de productividad del agente, incluyendo tendencias diarias,
        patrones horarios, rankings de agentes y mapas de calor de rendimiento para
        el monitoreo del call center.
        """
        try:
            # Establecer rango de fechas predeterminado si no se proporciona
            # Asegurarse que request.fecha_fin y request.fecha_inicio son de tipo date
            current_fecha_fin = request.fecha_fin if request.fecha_fin else date.today()
            current_fecha_inicio = request.fecha_inicio if request.fecha_inicio else current_fecha_fin - timedelta(days=30)
            
            # Validar rango de fechas
            if current_fecha_fin < current_fecha_inicio:
                raise HTTPException(
                    status_code=400,
                    detail="La fecha de fin debe ser posterior a la fecha de inicio."
                )
            
            # Generar datos de productividad usando el servicio
            # El servicio debería devolver un diccionario que coincida con ProductivityResponse
            productivity_data_dict = await service.get_productivity_data(
                filters=request.filtros if request.filtros else {}, # Usar el nombre de campo unificado
                fecha_inicio=current_fecha_inicio,
                fecha_fin=current_fecha_fin,
                metric_type=request.metric_type if request.metric_type else "gestiones" # Usar el nombre de campo unificado
            )
            
            # Pydantic validará y convertirá el diccionario al modelo ProductivityResponse
            return ProductivityResponse(**productivity_data_dict)
            
        except HTTPException:
            raise  # Re-lanzar excepciones HTTP directamente
        except Exception as e:
            self.logger.error(f"Error al generar datos de productividad (POST): {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Fallo al generar datos de productividad: {str(e)}"
            )
    
    @router.get("/", response_model=ProductivityResponse) # Ruta simplificada
    async def get_productivity_data_get_endpoint( # Renombrado para claridad
        fecha_inicio: Optional[date] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
        fecha_fin: Optional[date] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
        # agente: Optional[str] = Query(None, description="ID o nombre del agente"), # Si se quiere filtrar por GET
        # metric_type: Optional[str] = Query("gestiones", description="Tipo de métrica para heatmap"), # Si se quiere filtrar por GET
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> ProductivityResponse:
        """
        Obtiene datos de productividad con método GET (para consultas simples).
        Nota: Para filtros más complejos, usar el endpoint POST.
        """
        try:
            current_fecha_fin = fecha_fin if fecha_fin else date.today()
            current_fecha_inicio = fecha_inicio if fecha_inicio else current_fecha_fin - timedelta(days=30)

            if current_fecha_fin < current_fecha_inicio:
                raise HTTPException(
                    status_code=400,
                    detail="La fecha de fin debe ser posterior a la fecha de inicio."
                )

            # Aquí se asume que el GET request no pasará filtros complejos ni metric_type específico
            # Si se necesitaran, deberían añadirse como Query params y pasarse al servicio.
            # Por ahora, se pasan filtros vacíos y metric_type por defecto.
            productivity_data_dict = await service.get_productivity_data(
                filters={}, # Filtros no soportados en este GET simple, o añadir como Query params
                fecha_inicio=current_fecha_inicio,
                fecha_fin=current_fecha_fin,
                metric_type="gestiones" # Metric type por defecto, o añadir como Query param
            )
            
            return ProductivityResponse(**productivity_data_dict)
            
        except Exception as e:
            self.logger.error(f"Error al generar datos de productividad (GET): {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Fallo al generar datos de productividad: {str(e)}"
            )
    
    @router.get("/agents/{agent_id}", response_model=Dict[str, Any]) # El response model podría ser más específico si se define uno
    async def get_agent_detail_endpoint( # Renombrado
        agent_id: str,
        fecha_inicio: Optional[date] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
        fecha_fin: Optional[date] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
        service: DashboardServiceV2 = Depends(get_dashboard_service)
    ) -> Dict[str, Any]: # Considerar crear un modelo AgentPerformanceDetailResponse
        """
        Obtiene datos detallados de rendimiento para un agente específico.
        """
        try:
            current_fecha_fin = fecha_fin if fecha_fin else date.today()
            current_fecha_inicio = fecha_inicio if fecha_inicio else current_fecha_fin - timedelta(days=30)

            if current_fecha_fin < current_fecha_inicio:
                raise HTTPException(
                    status_code=400,
                    detail="La fecha de fin debe ser posterior a la fecha de inicio."
                )
            
            agent_detail_data = await service.get_agent_detail( # Asumiendo que este método existe en el servicio
                agent_id=agent_id,
                fecha_inicio=current_fecha_inicio,
                fecha_fin=current_fecha_fin
            )
            
            if not agent_detail_data:
                raise HTTPException(status_code=404, detail="Detalles del agente no encontrados.")

            return agent_detail_data # Debería ser un dict serializable
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Error al obtener detalle del agente {agent_id}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Fallo al obtener detalle del agente: {str(e)}"
            )
    
    @router.get("/metrics", response_model=Dict[str, List[str]])
    async def get_productivity_metrics_endpoint() -> Dict[str, List[str]]: # Renombrado
        """
        Obtiene las métricas de productividad disponibles para filtrado o visualización.
        """
        # Esta lógica podría moverse a una clase de configuración o al servicio si es más compleja.
        try:
            metrics = {
                "heatmap_metrics": [
                    "gestiones",
                    "contactosEfectivos", 
                    "compromisos"
                ],
                "trend_metrics": [
                    "llamadas",
                    "compromisos",
                    "recupero"
                ],
                "ranking_metrics": [
                    "calls", # Nota: consistencia en snake_case vs camelCase sería ideal.
                    "directContacts",
                    "commitments",
                    "amountRecovered",
                    "closingRate",
                    "commitmentConversion"
                    # "quartile" podría ser también una métrica
                ]
            }
            return metrics
            
        except Exception as e:
            # Aunque es poco probable un error aquí, se mantiene por consistencia.
            # self.logger.error(f"Error al obtener métricas de productividad: {str(e)}") # Necesitaría instancia de LoggerMixin
            raise HTTPException(
                status_code=500,
                detail=f"Fallo al obtener métricas de productividad: {str(e)}"
            )

# =============================================================================
# REGISTRO DE RUTAS API (si es necesario explícitamente)
# =============================================================================

# Crear instancia de la API para registrar métodos si la decoración del router no es suficiente
# o si se usa un enfoque basado en clases para registrar rutas de forma diferente.
# En este caso, con @router.post y @router.get, las rutas ya están asociadas al 'router'.
# api_instance = ProductivityAPI() # No es estrictamente necesario si solo se usan decoradores en métodos estáticos o de clase.

# El 'router' se importará en el agregador de routers principal de la API (ej: app/api/v1/api.py)
# y se incluirá allí.
