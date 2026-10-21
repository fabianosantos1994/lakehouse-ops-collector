"""Modelos da detecção: o que a fonte devolve e o que a API responde."""

from __future__ import annotations

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict

from ..incident import IncidentKey

# Estados do Airflow que abrem incidente. `upstream_failed` não abre: a task
# não rodou, e o incidente é o da task que falhou antes dela.
FAILED = "failed"
UP_FOR_RETRY = "up_for_retry"

# `failed` e `retrying` vêm do estado atual da task. `running` e `recovered`
# só aparecem na consulta por chave, depois de uma falha: a task voltou a
# rodar (ou espera uma nova tentativa) ou terminou com sucesso.
IncidentStatus = Literal["failed", "retrying", "running", "recovered"]


class TaskInstanceRecord(BaseModel):
    """Tentativa de uma task como a fonte a devolve, só com o que a detecção usa."""

    model_config = ConfigDict(frozen=True)

    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    state: str | None
    try_number: int
    max_tries: int
    operator: str | None = None
    start_date: AwareDatetime | None = None
    end_date: AwareDatetime | None = None


class Attempt(BaseModel):
    """Uma tentativa da task, numerada a partir de 1.

    `cycle` começa em 1 e sobe a cada `clear`: o clear reinicia a cota de
    repetições, e as tentativas seguintes formam um ciclo novo.
    """

    number: int
    cycle: int
    state: str | None
    operator: str | None
    start_date: AwareDatetime | None
    end_date: AwareDatetime | None


class DetectedIncident(BaseModel):
    """Um incidente detectado. `key` é exatamente o que o coletor recebe."""

    key: IncidentKey
    status: IncidentStatus
    attempts: list[Attempt]


class IncidentList(BaseModel):
    count: int
    incidents: list[DetectedIncident]


class SourceHealth(BaseModel):
    status: Literal["ok", "error"]
    detail: dict | str | None = None


class Health(BaseModel):
    status: Literal["ok"] = "ok"
    airflow: SourceHealth
