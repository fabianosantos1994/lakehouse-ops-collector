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

# Estado atual de uma task que já falhou. `None` é a task logo depois do
# `clear`, antes de o scheduler agendá-la. Os demais estados (`skipped`,
# `upstream_failed`, `removed`...) não dizem nada sobre esta falha e não mudam
# o incidente.
_STATUS_AFTER_FAILURE = {
    "success": "recovered",
    "running": "running",
    "queued": "running",
    "scheduled": "running",
    "deferred": "running",
    "up_for_reschedule": "running",
    "restarting": "running",
    None: "running",
}


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


def _merge_tries(record: TaskInstanceRecord, tries: list[TaskInstanceRecord]) -> list[TaskInstanceRecord]:
    """O histórico mais a tentativa atual, uma entrada por número de tentativa.

    Logo depois do `clear`, a task fica sem estado e com o número da tentativa
    anterior até o scheduler agendá-la; nesse intervalo, o histórico daquela
    tentativa vale mais que a task limpa. Fora isso, a tentativa atual vem da
    consulta principal, mais atual que o histórico.
    """
    by_number: dict[int, TaskInstanceRecord] = {}
    for item in [*tries, record]:
        previous = by_number.get(item.try_number)
        if previous is None or item.state is not None or previous.state is None:
            by_number[item.try_number] = item
    return list(by_number.values())


def _to_incident(
    source: TaskInstanceSource, record: TaskInstanceRecord, status: str | None = None
) -> DetectedIncident | None:
    tries = _merge_tries(record, source.list_tries(record.dag_id, record.run_id, record.task_id, record.map_index))
    if status is None:
        status = _STATUS_BY_STATE.get(record.state)
    if status is None:
        # Fora de falha agora: só é incidente se alguma tentativa já falhou.
        if not any(t.state == FAILED for t in tries):
            return None
        status = _STATUS_AFTER_FAILURE.get(record.state)
        if status is None:
            return None
    return DetectedIncident(
        key=IncidentKey(
            dag_id=record.dag_id,
            run_id=record.run_id,
            task_id=record.task_id,
            map_index=record.map_index,
        ),
        status=status,
        attempts=_attempts(tries),
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
    incidents = [_to_incident(source, record, _STATUS_BY_STATE[record.state]) for record in records]
    return IncidentList(count=len(incidents), incidents=incidents)


def get_incident(source: TaskInstanceSource, key: IncidentKey) -> DetectedIncident | None:
    """O incidente da chave, também depois da falha.

    Devolve `failed` ou `retrying` com a task em falha; `running` ou
    `recovered` se ela já falhou e depois voltou a rodar ou passou. None se a
    task não existe, nunca falhou ou está num estado que não diz nada sobre a
    falha (`skipped`, `upstream_failed`...).
    """
    record = source.get_task_instance(key.dag_id, key.run_id, key.task_id, key.map_index)
    if record is None:
        return None
    return _to_incident(source, record)
