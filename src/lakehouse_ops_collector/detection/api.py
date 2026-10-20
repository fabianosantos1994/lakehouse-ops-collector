"""API HTTP da detecção."""

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import AwareDatetime, ValidationError

from ..incident import IncidentKey
from .models import DetectedIncident, Health, IncidentList, SourceHealth
from .ports import SourceUnavailable, TaskInstanceSource
from .service import find_incidents, get_incident


def create_app(source: TaskInstanceSource) -> FastAPI:
    app = FastAPI(
        title="lakehouse-ops-collector: detecção de incidentes",
        description="Encontra no Airflow as tasks com falha. Somente leitura.",
        version="0.1.0",
    )

    @app.get("/health", response_model=Health)
    def health() -> Health:
        try:
            return Health(airflow=SourceHealth(status="ok", detail=source.health()))
        except SourceUnavailable as error:
            return Health(airflow=SourceHealth(status="error", detail=str(error)))

    @app.get("/incidents", response_model=IncidentList)
    def list_incidents(
        since: Annotated[AwareDatetime | None, Query(description="só falhas terminadas a partir deste horário (ISO 8601 com fuso)")] = None,
        dag_id: Annotated[str | None, Query(description="só desta DAG")] = None,
        include_retrying: Annotated[bool, Query(description="inclui tasks que ainda vão tentar de novo")] = False,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> IncidentList:
        try:
            return find_incidents(source, since=since, dag_id=dag_id, include_retrying=include_retrying, limit=limit)
        except SourceUnavailable as error:
            raise HTTPException(status_code=502, detail=str(error)) from error

    @app.get("/incidents/{dag_id}/{run_id}/{task_id}", response_model=DetectedIncident)
    def read_incident(
        dag_id: Annotated[str, Path()],
        run_id: Annotated[str, Path()],
        task_id: Annotated[str, Path()],
        map_index: Annotated[int, Query(ge=-1)] = -1,
    ) -> DetectedIncident:
        try:
            key = IncidentKey(dag_id=dag_id, run_id=run_id, task_id=task_id, map_index=map_index)
        except ValidationError as error:
            raise HTTPException(status_code=422, detail=error.errors(include_url=False)) from error
        try:
            incident = get_incident(source, key)
        except SourceUnavailable as error:
            raise HTTPException(status_code=502, detail=str(error)) from error
        if incident is None:
            raise HTTPException(status_code=404, detail="task not found or not failed")
        return incident

    return app
