"""
📞 Endpoints de Análisis de Operación
Monitoreo diario del rendimiento del call center y análisis GTR (Global Tasa de Resolución).
"""
# Imports estándar
from datetime import date, datetime
from typing import Any, Dict, List, Optional

# Imports de terceros
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

# Imports internos
from app.core.dependencies import get_cache_service, get_dashboard_service
from app.core.logging import LoggerMixin
from app.models.base import error_response, success_response
from app.models.operation import (  # ✅ USADO: POST endpoint
    AttemptEffectiveness,
    ChannelMetric,
    HourlyPerformance,
    OperationDayAnalysisData,  # ✅ USADO: Response model
    OperationDayKPI,
    OperationDayRequest,
    QueuePerformance)
from app.services.cache_service import CacheService
from app.services.dashboard_service_v2 import DashboardServiceV2

# Router para los endpoints de operación
router = APIRouter(prefix="/operation", tags=["operation"])


class OperationController(LoggerMixin):
    """Controlador para los endpoints de análisis de operación diaria."""

    def __init__(self, dashboard_service: DashboardServiceV2, cache_service: Optional[CacheService] = None):
        self.dashboard_service = dashboard_service
        self.cache_service = cache_service

    async def get_operation_analysis(
        self,
        fecha_analisis: Optional[date] = None,
        include_hourly: bool = True,
        include_attempts: bool = True,
        include_queues: bool = True,
    ) -> OperationDayAnalysisData:  # ✅ CORRECTO: Devuelve datos directos
        """
        Genera el análisis de operación diaria para el rendimiento del call center.

        Args:
            fecha_analisis: Fecha para el análisis de operación.
            include_hourly: Incluir desglose de rendimiento por hora.
            include_attempts: Incluir análisis de efectividad por intento.
            include_queues: Incluir métricas de rendimiento de cola.

        Returns:
            OperationDayAnalysisData: Datos directos como espera el Frontend.
        """
        if fecha_analisis is None:
            fecha_analisis = date.today()

        self.logger.info(f"Generando análisis de operación para {fecha_analisis}")

        try:
            # Verificar caché primero si el servicio de caché está disponible
            if self.cache_service:
                cache_key = f"operation:{fecha_analisis}:{include_hourly}:{include_attempts}:{include_queues}"
                cached_data = await self.cache_service.get(cache_key)
                if cached_data:
                    self.logger.info(f"Retornando datos de operación cacheados para la clave: {cache_key}")
                    return OperationDayAnalysisData.model_validate(cached_data)

            # Obtener datos del dashboard para la fecha de análisis
            dashboard_data = await self.dashboard_service.get_dashboard_data(
                filters={}, fecha_corte=fecha_analisis
            )

            # Generar componentes del análisis de operación
            analysis_data = await self._generate_operation_analysis(
                dashboard_data=dashboard_data,
                fecha_analisis=fecha_analisis,
                include_hourly=include_hourly,
                include_attempts=include_attempts,
                include_queues=include_queues,
            )

            # Cachear por 30 minutos si el servicio de caché está disponible
            if self.cache_service:
                await self.cache_service.set(cache_key, analysis_data.model_dump(), expire_in=1800)

            self.logger.info(
                f"Análisis de operación generado para {fecha_analisis} con "
                f"{len(analysis_data.kpis)} KPIs y "
                f"{len(analysis_data.channelPerformance)} canales"
            )

            return analysis_data

        except Exception as e:
            self.logger.error(f"Error generando análisis de operación: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Fallo al generar análisis de operación: {str(e)}"
            )

    async def _generate_operation_analysis(
        self,
        dashboard_data: Dict[str, Any],
        fecha_analisis: date, # No usado directamente, pero podría ser útil para lógica futura
        include_hourly: bool,
        include_attempts: bool,
        include_queues: bool,
    ) -> OperationDayAnalysisData:
        """
        Genera el análisis completo de la operación a partir de los datos del dashboard.

        Args:
            dashboard_data: Datos del dashboard para la fecha de análisis.
            fecha_analisis: Fecha que se está analizando.
            include_hourly: Si se incluye el desglose por hora.
            include_attempts: Si se incluye la efectividad por intento.
            include_queues: Si se incluye el rendimiento de la cola.

        Returns:
            OperationDayAnalysisData: Objeto de datos directos.
        """
        # Calcular KPIs diarios
        kpis = self._calculate_daily_kpis(dashboard_data)

        # Generar comparación de rendimiento de canal
        channel_performance = self._generate_channel_performance(dashboard_data)

        # Componentes opcionales basados en flags
        hourly_performance: List[HourlyPerformance] = []
        if include_hourly:
            hourly_performance = self._generate_hourly_performance(dashboard_data)

        attempt_effectiveness: List[AttemptEffectiveness] = []
        if include_attempts:
            attempt_effectiveness = self._generate_attempt_effectiveness(dashboard_data)

        queue_performance: List[QueuePerformance] = []
        if include_queues:
            queue_performance = self._generate_queue_performance(dashboard_data)

        return OperationDayAnalysisData(
            kpis=kpis,
            channelPerformance=channel_performance,
            hourlyPerformance=hourly_performance,
            attemptEffectiveness=attempt_effectiveness,
            queuePerformance=queue_performance,
        )

    def _calculate_daily_kpis(self, dashboard_data: Dict[str, Any]) -> List[OperationDayKPI]:
        """
        Calcula los KPIs clave de la operación diaria.

        Args:
            dashboard_data: Estructura de datos del dashboard.

        Returns:
            Lista de KPIs de operación diaria.
        """
        kpis: List[OperationDayKPI] = []

        # Extraer totales de los datos del dashboard
        totals = self._extract_operation_totals(dashboard_data)

        # KPI Total Llamadas
        kpis.append(
            OperationDayKPI(label="Total Llamadas", value=f"{totals.get('total_gestiones', 0):,}")
        )

        # KPI Tasa de Contacto
        contact_rate = totals.get("contacto", 0)
        kpis.append(OperationDayKPI(label="Tasa de Contacto", value=f"{contact_rate:.1f}%"))

        # KPI Contacto Directo
        cd_rate = totals.get("cd", 0)
        kpis.append(OperationDayKPI(label="Contacto Directo", value=f"{cd_rate:.1f}%"))

        # KPI Tasa de Cierre
        closure_rate = totals.get("cierre", 0)
        kpis.append(OperationDayKPI(label="Tasa de Cierre", value=f"{closure_rate:.1f}%"))

        # KPI Intensidad
        intensity = totals.get("inten", 0)
        kpis.append(OperationDayKPI(label="Intensidad", value=f"{intensity:.1f}"))

        # KPI Cuentas Gestionadas
        kpis.append(
            OperationDayKPI(
                label="Cuentas Gestionadas", value=f"{totals.get('cuentas_gestionadas', 0):,}"
            )
        )

        return kpis

    @staticmethod
    def _extract_operation_totals(dashboard_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Extrae los totales operativos de los datos del dashboard.

        Args:
            dashboard_data: Estructura de datos del dashboard.

        Returns:
            Diccionario con los totales operativos.
        """
        totals: Dict[str, float] = {
            "total_gestiones": 0,
            "cuentas_gestionadas": 0,
            "contacto": 0,
            "cd": 0, # Contacto Directo
            "ci": 0, # Contacto Indirecto
            "cierre": 0,
            "inten": 0, # Intensidad
        }

        # Sumar desde datos de segmento (excluyendo la fila de totales)
        total_cuentas = 0
        weighted_contacto = 0.0
        weighted_cd = 0.0
        weighted_ci = 0.0
        weighted_cierre = 0.0
        weighted_inten = 0.0

        for item in dashboard_data.get("segmentoData", []):
            if item.get("name") != "Total":
                cuentas = item.get("cuentas", 0)
                total_cuentas += cuentas

                # Promedios ponderados
                weighted_contacto += item.get("contacto", 0) * cuentas
                weighted_cd += item.get("cd", 0) * cuentas
                weighted_ci += item.get("ci", 0) * cuentas
                weighted_cierre += item.get("cierre", 0) * cuentas
                weighted_inten += item.get("inten", 0) * cuentas

                # Sumar métricas contables
                totals["total_gestiones"] += (
                    item.get("cdCount", 0) + item.get("ciCount", 0) + item.get("scCount", 0)
                )

        # Calcular promedios ponderados
        if total_cuentas > 0:
            totals["contacto"] = weighted_contacto / total_cuentas
            totals["cd"] = weighted_cd / total_cuentas
            totals["ci"] = weighted_ci / total_cuentas
            totals["cierre"] = weighted_cierre / total_cuentas
            totals["inten"] = weighted_inten / total_cuentas

        totals["cuentas_gestionadas"] = float(total_cuentas)

        return totals

    def _generate_channel_performance(
        self, dashboard_data: Dict[str, Any]
    ) -> List[ChannelMetric]:
        """
        Genera la comparación de rendimiento de canal (Bot vs Humano).

        Args:
            dashboard_data: Estructura de datos del dashboard.

        Returns:
            Lista de métricas de rendimiento de canal.
        """
        # Por ahora, crea un desglose de canal simulado
        # En el futuro, esto vendría de datos de gestiones_bot vs gestiones_humano

        totals = self._extract_operation_totals(dashboard_data)
        total_calls = int(totals.get("total_gestiones", 0))
        # Asumiendo que 'cd' (contacto directo) y 'ci' (contacto indirecto) son contactos efectivos
        total_effective_contacts = totals.get("cd", 0) + totals.get("ci", 0)


        # Simular división de canal (distribución típica 60% bot, 40% humano)
        voicebot_calls = int(total_calls * 0.6)
        callcenter_calls = total_calls - voicebot_calls # Asegura que la suma sea total_calls

        voicebot_effective_contacts = int(total_effective_contacts * 0.4) # Ej: Bot maneja 40% de contactos efectivos
        callcenter_effective_contacts = int(total_effective_contacts * 0.6) # Ej: Humano maneja 60%

        # Asegurar consistencia si los cálculos anteriores no suman
        # Esto es una simplificación, la lógica real podría ser más compleja
        if voicebot_calls > 0 :
            voicebot_effective_contacts = min(voicebot_effective_contacts, voicebot_calls)
        else:
            voicebot_effective_contacts = 0

        if callcenter_calls > 0:
            callcenter_effective_contacts = min(callcenter_effective_contacts, callcenter_calls)
        else:
            callcenter_effective_contacts = 0


        channels = [
            ChannelMetric(
                channel="Voicebot",
                calls=voicebot_calls,
                effectiveContacts=voicebot_effective_contacts,
                nonEffectiveContacts=voicebot_calls - voicebot_effective_contacts,
                pdp=int(voicebot_effective_contacts * 0.3),  # 30% conversión a Promesa De Pago
                cierreRate=25.0,  # 25% tasa de cierre de contactos efectivos
            ),
            ChannelMetric(
                channel="Call Center",
                calls=callcenter_calls,
                effectiveContacts=callcenter_effective_contacts,
                nonEffectiveContacts=callcenter_calls - callcenter_effective_contacts,
                pdp=int(callcenter_effective_contacts * 0.4),  # 40% conversión a PDP
                cierreRate=35.0,  # 35% tasa de cierre de contactos efectivos
            ),
        ]

        return channels

    @staticmethod
    def _generate_hourly_performance(
            dashboard_data: Dict[str, Any] # No usado directamente, datos simulados
    ) -> List[HourlyPerformance]:
        """
        Genera el desglose de rendimiento por hora.

        Args:
            dashboard_data: Estructura de datos del dashboard.

        Returns:
            Lista de métricas de rendimiento por hora.
        """
        # Simular distribución horaria basada en patrones típicos de call center
        hourly_data: List[HourlyPerformance] = []
        base_calls_per_hour = 100  # Llamadas base, ajustar según datos
        peak_hours = [10, 11, 14, 15, 16]  # Horas pico típicas

        for hour in range(8, 19):  # 8 AM a 6 PM (18:00)
            hour_str = f"{hour:02d}:00"
            calls_multiplier = 1.0

            if hour in peak_hours:
                calls_multiplier = 1.5
            elif hour in [8, 9, 17, 18]:  # Horas valle/hombro
                calls_multiplier = 0.8

            current_hour_calls = int(base_calls_per_hour * calls_multiplier)

            # Calcular métricas de rendimiento (simuladas)
            effective_rate = 0.65 if hour in peak_hours else 0.55 # Tasa de efectividad
            effective_contacts = int(current_hour_calls * effective_rate)
            non_effective_contacts = current_hour_calls - effective_contacts
            pdp = int(effective_contacts * 0.35) # Promesas de pago

            hourly_data.append(
                HourlyPerformance(
                    hour=hour_str,
                    effectiveContacts=effective_contacts,
                    nonEffectiveContacts=non_effective_contacts,
                    pdp=pdp,
                )
            )
        return hourly_data

    @staticmethod
    def _generate_attempt_effectiveness(
            dashboard_data: Dict[str, Any] # No usado directamente, datos simulados
    ) -> List[AttemptEffectiveness]:
        """
        Genera el análisis de efectividad por intento.

        Args:
            dashboard_data: Estructura de datos del dashboard.

        Returns:
            Lista de métricas de efectividad por intento.
        """
        # Simular efectividad por intento (típicamente disminuye con más intentos)
        attempt_data: List[AttemptEffectiveness] = []
        base_closure_rate = 45.0 # Tasa de cierre base para el primer intento

        for attempt_number in range(1, 6):  # Primeros 5 intentos
            # La tasa de cierre disminuye con cada intento (ejemplo exponencial)
            closure_rate = base_closure_rate * (0.8 ** (attempt_number - 1))
            attempt_data.append(
                AttemptEffectiveness(attempt=attempt_number, cierreRate=round(closure_rate, 1))
            )
        return attempt_data

    @staticmethod
    def _generate_queue_performance(
            dashboard_data: Dict[str, Any]
    ) -> List[QueuePerformance]:
        """
        Genera métricas de rendimiento de cola.

        Args:
            dashboard_data: Estructura de datos del dashboard.

        Returns:
            Lista de métricas de rendimiento de cola.
        """
        # Simular rendimiento de cola basado en tipos de cartera/segmento
        queue_data: List[QueuePerformance] = []

        for item in dashboard_data.get("segmentoData", []):
            if item.get("name") != "Total": # Excluir fila de total si existe
                queue_name = item.get("name", "Desconocida")
                cuentas = item.get("cuentas", 0)

                # Estimar llamadas y rendimiento (simulado)
                # Estos factores (1.5, 0.3) son ejemplos y deben ajustarse
                calls = int(cuentas * 1.5)  # Promedio de 1.5 llamadas por cuenta
                effective_contacts_rate = item.get("contacto", 0) / 100.0
                effective_contacts = int(calls * effective_contacts_rate)
                pdp = int(effective_contacts * 0.3) # 30% de contactos efectivos generan PDP
                closure_rate = item.get("cierre", 0) # Tasa de cierre del segmento

                queue_data.append(
                    QueuePerformance(
                        queueName=queue_name,
                        calls=calls,
                        effectiveContacts=effective_contacts,
                        pdp=pdp,
                        cierreRate=closure_rate,
                    )
                )

        # Ordenar por número de llamadas descendente
        queue_data.sort(key=lambda q: q.calls, reverse=True)
        return queue_data


# =============================================================================
# ENDPOINTS FASTAPI
# =============================================================================

@router.get("/", response_model=OperationDayAnalysisData)
async def get_operation_analysis_endpoint( # Renombrado para evitar conflicto con el método del controller
    fecha_analisis: Optional[date] = Query(None, description="Fecha de análisis (YYYY-MM-DD)"),
    include_hourly: bool = Query(True, description="Incluir desglose de rendimiento por hora"),
    include_attempts: bool = Query(True, description="Incluir análisis de efectividad por intento"),
    include_queues: bool = Query(True, description="Incluir métricas de rendimiento de cola"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> OperationDayAnalysisData:
    """
    Obtiene el análisis de operación diaria para el monitoreo del rendimiento del call center.

    Retorna `OperationDayAnalysisData` - datos DIRECTOS como espera el Frontend.
    Sin objeto wrapper, sin campo de metadatos extra.

    **Ejemplos de Uso:**

    - Operación de hoy: `/api/v1/operation/`
    - Fecha específica: `/api/v1/operation/?fecha_analisis=2025-06-27`
    - Análisis mínimo: `/api/v1/operation/?include_hourly=false&include_attempts=false`

    **El análisis incluye:**
    - KPIs diarios (llamadas, tasas de contacto, tasas de cierre)
    - Comparación de rendimiento de canal (Bot vs Humano)
    - Desglose de rendimiento por hora (opcional)
    - Análisis de efectividad por intento (opcional)
    - Rendimiento de cola por cartera (opcional)
    """
    controller = OperationController(dashboard_service, cache_service)
    return await controller.get_operation_analysis(
        fecha_analisis=fecha_analisis,
        include_hourly=include_hourly,
        include_attempts=include_attempts,
        include_queues=include_queues,
    )


@router.post("/", response_model=OperationDayAnalysisData)
async def get_operation_analysis_post_endpoint( # Renombrado para evitar conflicto
    request: OperationDayRequest,
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
    cache_service: CacheService = Depends(get_cache_service),
) -> OperationDayAnalysisData:
    """
    Obtiene el análisis de operación con método POST para solicitudes complejas.

    Retorna `OperationDayAnalysisData` directamente - COINCIDENCIA EXACTA con las expectativas del Frontend.
    """
    controller = OperationController(dashboard_service, cache_service)

    try:
        # Parsear fecha desde la solicitud
        fecha_analisis = datetime.strptime(request.date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usar YYYY-MM-DD.")


    return await controller.get_operation_analysis(
        fecha_analisis=fecha_analisis,
        include_hourly=request.includeHourlyBreakdown,
        include_attempts=request.includeAttemptAnalysis,
        include_queues=request.includeQueueDetails,
    )


@router.get("/kpis", response_model=List[OperationDayKPI])
async def get_daily_kpis_endpoint( # Renombrado
    fecha_analisis: Optional[date] = Query(None, description="Fecha de análisis (YYYY-MM-DD)"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
):
    """
    Obtiene solo los KPIs de operación diaria (endpoint ligero).

    Retorna solo las métricas diarias clave sin desgloses detallados.
    """
    try:
        current_date = fecha_analisis if fecha_analisis else date.today()
        dashboard_data = await dashboard_service.get_dashboard_data(
            filters={}, fecha_corte=current_date
        )

        # No se necesita cache_service para este endpoint específico del controller
        controller = OperationController(dashboard_service, None)
        kpis = controller._calculate_daily_kpis(dashboard_data)
        return kpis

    except Exception as e:
        # Usar LoggerMixin para loggear el error si OperationController estuviera instanciado aquí
        # Como es un endpoint simple, un print o logging directo podría ser una opción temporal
        # print(f"Error en get_daily_kpis_endpoint: {e}") # Reemplazar con logging adecuado
        raise HTTPException(status_code=500, detail=f"Fallo al obtener KPIs diarios: {str(e)}")


@router.get("/channels", response_model=List[ChannelMetric])
async def get_channel_performance_endpoint( # Renombrado
    fecha_analisis: Optional[date] = Query(None, description="Fecha de análisis (YYYY-MM-DD)"),
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
):
    """
    Obtiene la comparación de rendimiento de canal (Bot vs Call Center).

    Retorna métricas de rendimiento para cada tipo de canal.
    """
    try:
        current_date = fecha_analisis if fecha_analisis else date.today()
        dashboard_data = await dashboard_service.get_dashboard_data(
            filters={}, fecha_corte=current_date
        )
        
        # No se necesita cache_service para este método específico del controller
        controller = OperationController(dashboard_service, None)
        channels = controller._generate_channel_performance(dashboard_data)
        return channels

    except Exception as e:
        # print(f"Error en get_channel_performance_endpoint: {e}") # Reemplazar con logging adecuado
        raise HTTPException(
            status_code=500, detail=f"Fallo al obtener rendimiento de canal: {str(e)}"
        )


@router.get("/health")
async def operation_health_check_endpoint( # Renombrado
    dashboard_service: DashboardServiceV2 = Depends(get_dashboard_service),
):
    """Verificación de salud para los endpoints de operación."""
    try:
        health_status = await dashboard_service.health_check()
        return success_response(
            data={
                "status": "healthy",
                "service": "operation",
                "timestamp": datetime.now().isoformat(),
                "dashboard_service": health_status,
            }
        )

    except Exception as e:
        # print(f"Error en operation_health_check_endpoint: {e}") # Reemplazar con logging adecuado
        return JSONResponse(
            status_code=503, # Service Unavailable
            content=error_response(
                message="Servicio de operación no saludable",
                details={
                    "service": "operation",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                },
            ),
        )
