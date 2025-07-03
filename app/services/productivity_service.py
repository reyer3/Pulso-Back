# app/services/productivity_service.py
"""
🏆 ProductivityService - Servicio especializado para la página de Productividad
Coordina múltiples consultas BigQuery para análisis de agentes
"""

import asyncio
from datetime import datetime, date
from typing import Dict, List, Optional, Any

from app.repositories.bigquery_repo import BigQueryRepository
from app.repositories.cache_repo import CacheRepository
from app.models.productivity import (
    ProductivityData,
    ProductivityRequest,
    ProductivityResponse,
    AgentRankingRow,
    AgentHeatmapRow,
    ProductivityTrendPoint,
    AgentDailyPerformance
)
from shared.core.config import settings


class ProductivityService:
    """
    Servicio especializado para la página de Productividad

    Responsabilidades:
    - Coordinar múltiples consultas BigQuery
    - Transformar datos a modelos Pydantic
    - Manejar cache de resultados
    - Proporcionar datos para diferentes visualizaciones

    Consultas que coordina:
    1. Ranking de agentes con métricas y cuartiles
    2. Tendencias diarias de productividad
    3. Tendencias por horas del día
    4. Heatmap de performance por agente/día
    5. Lista de agentes disponibles
    """

    def __init__(self, bq_repo: BigQueryRepository, cache_repo: CacheRepository):
        self.bq_repo = bq_repo
        self.cache_repo = cache_repo
        self.cache_ttl = settings.CACHE_TTL_ASSIGNMENT  # 2 horas por defecto

    async def get_productivity_analysis(
            self,
            request: ProductivityRequest
    ) -> ProductivityResponse:
        """
        Análisis completo de productividad con múltiples consultas optimizadas

        Args:
            request: Parámetros de solicitud con filtros y fechas

        Returns:
            ProductivityResponse con todos los datos para el frontend
        """
        # Generate cache key based on request parameters
        cache_key = self._generate_cache_key("productivity_analysis", request)

        # Try cache first
        cached_data = await self.cache_repo.get_from_cache(cache_key)
        if cached_data:
            self.logger.info("📋 Cache hit for productivity analysis")
            return ProductivityResponse(**cached_data)

        self.logger.info("🔍 Executing productivity analysis with multiple queries")

        # Execute multiple queries in parallel for better performance
        start_time = datetime.now()

        try:
            # Launch all queries concurrently
            tasks = [
                self._get_agent_ranking(request),
                self._get_daily_trends(request),
                self._get_hourly_trends(request),
                self._get_agent_heatmap(request)
            ]

            agent_ranking, daily_trend, hourly_trend, agent_heatmap = await asyncio.gather(*tasks)

            # Build response with metadata
            execution_time = (datetime.now() - start_time).total_seconds()

            response = ProductivityResponse(
                agentRanking=agent_ranking,
                dailyTrend=daily_trend,
                hourlyTrend=hourly_trend,
                agentHeatmap=agent_heatmap,
                metadata={
                    "period": {
                        "start": request.fecha_inicio.isoformat() if request.fecha_inicio else None,
                        "end": request.fecha_fin.isoformat() if request.fecha_fin else None
                    },
                    "filters": request.filtros or {},
                    "generated_at": datetime.now().isoformat(),
                    "execution_time_seconds": execution_time,
                    "total_agents": len(agent_ranking),
                    "query_count": 4,
                    "cache_status": "miss"
                }
            )

            # Cache the successful result
            await self.cache_repo.set_to_cache(
                cache_key,
                response.dict(),
                self.cache_ttl
            )

            self.logger.info(
                f"✅ Productivity analysis completed in {execution_time:.2f}s",
                extra={
                    "agents_count": len(agent_ranking),
                    "daily_points": len(daily_trend),
                    "hourly_points": len(hourly_trend),
                    "heatmap_agents": len(agent_heatmap)
                }
            )

            return response

        except Exception as e:
            self.logger.error(f"❌ Productivity analysis failed: {e}", exc_info=True)
            raise

    async def get_available_agents(
            self,
            request: ProductivityRequest
    ) -> List[Dict[str, Any]]:
        """
        Obtener lista de agentes disponibles para el selector del frontend

        Args:
            request: Parámetros con filtros de fecha

        Returns:
            Lista de agentes en formato compatible con UserSelector
        """
        cache_key = self._generate_cache_key("available_agents", request)

        # Try cache first
        cached_agents = await self.cache_repo.get_from_cache(cache_key)
        if cached_agents:
            return cached_agents

        # TODO: Implementar query específica para obtener agentes disponibles
        # Esta query debe ser optimizada y rápida
        query = """
        -- Query para obtener agentes disponibles en el período
        -- TODO: Definir query específica basada en las tablas reales
        SELECT 
            'placeholder' as agent_name,
            'placeholder' as dni
        """

        params = self._build_query_params(request)
        results = await self.bq_repo.execute_query(query, params, use_cache=True)

        # Transform to frontend format
        agents = self._transform_to_user_selector_format(results)

        # Cache for shorter time (agents list changes less frequently)
        await self.cache_repo.set_to_cache(cache_key, agents, 3600)  # 1 hour

        return agents

    async def _get_agent_ranking(self, request: ProductivityRequest) -> List[AgentRankingRow]:
        """
        Query 1: Ranking de agentes con métricas principales y cuartiles

        Esta query debe obtener:
        - Ranking ordenado por performance
        - Métricas de llamadas, contactos, compromisos
        - Montos recuperados
        - Tasas de conversión
        - Cuartiles de performance
        """
        self.logger.debug("🏆 Executing agent ranking query")

        # TODO: Implementar query específica para ranking de agentes
        # Debe incluir todas las métricas necesarias para AgentRankingRow
        query = """
        -- Query para ranking de agentes con métricas completas
        -- TODO: Definir query específica basada en las tablas reales
        SELECT 
            'placeholder' as agent_name,
            1 as rank,
            100 as calls,
            50 as direct_contacts,
            25 as commitments,
            5000.0 as amount_recovered,
            50.0 as closing_rate,
            75.0 as commitment_conversion,
            1 as quartile
        """

        params = self._build_query_params(request)
        results = await self.bq_repo.execute_query(query, params, use_cache=True)

        # Transform to Pydantic models
        ranking = []
        for i, result in enumerate(results):
            agent = AgentRankingRow(
                id=f"agent-{i + 1}",
                rank=result.get('rank', i + 1),
                agentName=result.get('agent_name', f'Agent {i + 1}'),
                calls=result.get('calls', 0),
                directContacts=result.get('direct_contacts', 0),
                commitments=result.get('commitments', 0),
                amountRecovered=result.get('amount_recovered', 0.0),
                closingRate=result.get('closing_rate', 0.0),
                commitmentConversion=result.get('commitment_conversion', 0.0),
                quartile=result.get('quartile', 4)
            )
            ranking.append(agent)

        # Apply agent filter if specified
        if request.agente:
            ranking = [a for a in ranking if a.agentName == request.agente]

        return ranking

    async def _get_daily_trends(self, request: ProductivityRequest) -> List[ProductivityTrendPoint]:
        """
        Query 2: Tendencias diarias de productividad

        Esta query debe obtener por día:
        - Total de llamadas
        - Total de compromisos
        - Monto recuperado estimado
        """
        self.logger.debug("📅 Executing daily trends query")

        # TODO: Implementar query específica para tendencias diarias
        query = """
        -- Query para tendencias de productividad por día
        -- TODO: Definir query específica basada en las tablas reales
        SELECT 
            1 as day,
            100 as llamadas,
            25 as compromisos,
            5000.0 as recupero
        """

        params = self._build_query_params(request)
        results = await self.bq_repo.execute_query(query, params, use_cache=True)

        # Transform to Pydantic models
        daily_trends = []
        for result in results:
            trend = ProductivityTrendPoint(
                day=result.get('day'),
                llamadas=result.get('llamadas', 0),
                compromisos=result.get('compromisos', 0),
                recupero=result.get('recupero')
            )
            daily_trends.append(trend)

        return daily_trends

    async def _get_hourly_trends(self, request: ProductivityRequest) -> List[ProductivityTrendPoint]:
        """
        Query 3: Tendencias por horas del día

        Esta query debe obtener por hora (7:00-20:00):
        - Total de llamadas
        - Total de compromisos
        """
        self.logger.debug("⏰ Executing hourly trends query")

        # TODO: Implementar query específica para tendencias por hora
        query = """
        -- Query para tendencias de productividad por hora
        -- TODO: Definir query específica basada en las tablas reales
        SELECT 
            '09:00' as hour,
            50 as llamadas,
            12 as compromisos
        """

        params = self._build_query_params(request)
        results = await self.bq_repo.execute_query(query, params, use_cache=True)

        # Transform to Pydantic models
        hourly_trends = []
        for result in results:
            trend = ProductivityTrendPoint(
                hour=result.get('hour'),
                llamadas=result.get('llamadas', 0),
                compromisos=result.get('compromisos', 0)
            )
            hourly_trends.append(trend)

        return hourly_trends

    async def _get_agent_heatmap(self, request: ProductivityRequest) -> List[AgentHeatmapRow]:
        """
        Query 4: Heatmap de productividad por agente y día

        Esta query debe obtener para cada agente y día:
        - Gestiones por hora trabajada
        - Contactos efectivos por hora trabajada
        - Compromisos por hora trabajada
        """
        self.logger.debug("🔥 Executing agent heatmap query")

        # TODO: Implementar query específica para heatmap de agentes
        query = """
        -- Query para heatmap de productividad por agente y día
        -- TODO: Definir query específica basada en las tablas reales
        SELECT 
            'Agent 1' as agent_name,
            'DNI123' as dni,
            1 as day,
            10.5 as gestiones,
            8.2 as contactos_efectivos,
            5.1 as compromisos
        """

        params = self._build_query_params(request)
        results = await self.bq_repo.execute_query(query, params, use_cache=True)

        # Group by agent and build daily performance
        agent_heatmap_dict = {}
        for result in results:
            agent_key = result['agent_name']
            if agent_key not in agent_heatmap_dict:
                agent_heatmap_dict[agent_key] = {
                    "agent_name": result['agent_name'],
                    "dni": result.get('dni', 'SIN DNI'),
                    "daily_performance": {}
                }

            day = result['day']
            daily_perf = AgentDailyPerformance(
                gestiones=result.get('gestiones'),
                contactosEfectivos=result.get('contactos_efectivos'),
                compromisos=result.get('compromisos')
            )

            agent_heatmap_dict[agent_key]["daily_performance"][day] = daily_perf

        # Convert to list of AgentHeatmapRow objects
        heatmap_list = []
        for i, (agent_name, data) in enumerate(agent_heatmap_dict.items()):
            heatmap_row = AgentHeatmapRow(
                id=f"agent-{i + 1}",
                dni=data["dni"],
                agentName=data["agent_name"],
                dailyPerformance=data["daily_performance"]
            )
            heatmap_list.append(heatmap_row)

        # Apply agent filter if specified
        if request.agente:
            heatmap_list = [h for h in heatmap_list if h.agentName == request.agente]

        return heatmap_list

    def _build_query_params(self, request: ProductivityRequest) -> Dict[str, Any]:
        """
        Construir parámetros para las queries basado en el request

        Args:
            request: Parámetros de solicitud

        Returns:
            Diccionario con parámetros para BigQuery
        """
        params = {}

        # Fechas
        if request.fecha_inicio:
            params['fecha_inicio'] = request.fecha_inicio
        if request.fecha_fin:
            params['fecha_fin'] = request.fecha_fin

        # Filtros adicionales si están definidos
        if request.filtros:
            # TODO: Mapear filtros específicos según las necesidades
            for key, value in request.filtros.items():
                if value and value != ['TODAS']:
                    params[f'filter_{key}'] = value

        return params

    def _generate_cache_key(self, operation: str, request: ProductivityRequest) -> str:
        """
        Generar clave de cache única basada en la operación y parámetros

        Args:
            operation: Tipo de operación (ej: 'productivity_analysis')
            request: Parámetros de solicitud

        Returns:
            Clave de cache única
        """
        import hashlib

        # Create deterministic string from request
        key_data = {
            "operation": operation,
            "fecha_inicio": request.fecha_inicio.isoformat() if request.fecha_inicio else None,
            "fecha_fin": request.fecha_fin.isoformat() if request.fecha_fin else None,
            "agente": request.agente,
            "filtros": request.filtros or {},
            "metric_type": request.metric_type
        }

        key_string = str(sorted(key_data.items()))
        return f"productivity:{hashlib.md5(key_string.encode()).hexdigest()}"

    def _transform_to_user_selector_format(self, results: List[Dict]) -> List[Dict[str, Any]]:
        """
        Transformar resultados de query a formato UserSelector del frontend

        Args:
            results: Resultados crudos de BigQuery

        Returns:
            Lista en formato compatible con UserSelector
        """
        return [
            {
                "id": agent.get('agent_name', f'agent_{i}'),
                "nombre": agent.get('agent_name', 'Sin Nombre'),
                "dni": agent.get('dni', 'SIN DNI'),
                "label": f"{agent.get('agent_name', 'Sin Nombre')} ({agent.get('dni', 'Sin DNI')})",
                "fechaInicio": agent.get('fecha_inicio'),
                "fechaFin": agent.get('fecha_fin')
            }
            for i, agent in enumerate(results)
        ]

    @property
    def logger(self):
        """Logger instance for this service"""
        import logging
        return logging.getLogger(f"{__name__}.{self.__class__.__name__}")


# Factory function for dependency injection
async def get_productivity_service(
        bq_repo: BigQueryRepository,
        cache_repo: CacheRepository
) -> ProductivityService:
    """
    Factory function para obtener instancia del ProductivityService

    Args:
        bq_repo: Repositorio BigQuery inicializado
        cache_repo: Repositorio Cache inicializado

    Returns:
        Instancia configurada del ProductivityService
    """
    return ProductivityService(bq_repo, cache_repo)