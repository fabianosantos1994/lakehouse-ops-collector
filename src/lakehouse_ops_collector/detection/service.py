"""Casos de uso da detecção."""

from __future__ import annotations

from datetime import datetime

from ..incident import IncidentKey
from .models import (
    FAILED,
    UP_FOR_RETRY,
    Attempt,
    DetectedIncident,
    IncidentList,
    TaskInstanceRecord,
)
from .ports import TaskInstanceSource

_STATUS_BY_STATE = {FAILED: "failed", UP_FOR_RETRY: "retrying"}


def _attempts(tries: list[TaskInstanceRecord]) -> list[Attempt]:
    """As tentativas em ordem, com o ciclo de cada uma.

    Cada tentativa guarda o max_tries do seu momento. Numa repetição normal ele
    não muda; o `clear` o recalcula para tentativa atual + retries, então um
    aumento entre duas tentativas marca o início de um ciclo novo.
    """
    attempts: list[Attempt] = []
    cycle = 1
    previous: TaskInstanceRecord | None = None
    for record in sorted(tries, key=lambda t: t.try_number):
        if previous is not None and record.max_tries > previous.max_tries:
            cycle += 1
        attempts.append(
            Attempt(
                number=record.try_number,
                cycle=cycle,
                state=record.state,
                operator=record.operator,
                start_date=record.start_date,
                end_date=record.end_date,
            )
        )
        previous = record
    return attempts


def _to_incident(source: TaskInstanceSource, record: TaskInstanceRecord) -> DetectedIncident:
    tries = source.list_tries(record.dag_id, record.run_id, record.task_id, record.map_index)
    # A tentativa atual vem da consulta principal, mais atual que o histórico.
    by_number = {t.try_number: t for t in tries} | {record.try_number: record}
    return DetectedIncident(
        key=IncidentKey(
            dag_id=record.dag_id,
            run_id=record.run_id,
            task_id=record.task_id,
            map_index=record.map_index,
        ),
        status=_STATUS_BY_STATE[record.state],
        attempts=_attempts(list(by_number.values())),
    )


def find_incidents(
    source: TaskInstanceSource,
    since: datetime | None = None,
    dag_id: str | None = None,
    include_retrying: bool = False,
    limit: int = 100,
) -> IncidentList:
    """Tasks com falha definitiva e, se pedido, as que ainda vão tentar de novo."""
    states = [FAILED, UP_FOR_RETRY] if include_retrying else [FAILED]
    records = source.list_task_instances(states, since=since, dag_id=dag_id, limit=limit)
    incidents = [_to_incident(source, record) for record in records]
    return IncidentList(count=len(incidents), incidents=incidents)


def get_incident(source: TaskInstanceSource, key: IncidentKey) -> DetectedIncident | None:
    """O incidente da chave, ou None se a task não existe ou não está com falha."""
    record = source.get_task_instance(key.dag_id, key.run_id, key.task_id, key.map_index)
    if record is None or record.state not in _STATUS_BY_STATE:
        return None
    return _to_incident(source, record)
