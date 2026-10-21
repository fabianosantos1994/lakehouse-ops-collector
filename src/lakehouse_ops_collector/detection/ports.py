"""Interfaces da fonte de tentativas e do registro; os adapters as implementam."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import NamedTuple, Protocol

from ..incident import IncidentKey
from .models import DetectedIncident, TaskInstanceRecord


class SourceUnavailable(Exception):
    """A fonte não respondeu ou recusou a consulta."""


class RegistryUnavailable(Exception):
    """O banco do registro não respondeu ou recusou a operação."""


class TaskInstanceSource(Protocol):
    def list_task_instances(
        self,
        states: Sequence[str],
        since: datetime | None = None,
        dag_id: str | None = None,
        limit: int = 100,
    ) -> list[TaskInstanceRecord]:
        """Tentativas atuais nos estados pedidos, das mais recentes para as mais antigas."""
        ...

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> TaskInstanceRecord | None:
        """A tentativa atual de uma task, ou None se ela não existe."""
        ...

    def list_tries(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> list[TaskInstanceRecord]:
        """Todas as tentativas de uma task, inclusive a atual."""
        ...

    def health(self) -> dict:
        """Estado da fonte; levanta SourceUnavailable se ela não responde."""
        ...


class SaveCounts(NamedTuple):
    """Resultado de uma gravação no registro: incidentes novos e alterados."""

    new: int
    updated: int


class IncidentRegistry(Protocol):
    def save_all(self, incidents: Sequence[DetectedIncident], source: str) -> SaveCounts:
        """Grava os incidentes pela `key`, numa transação só, sem duplicar.

        `source` é o modo que grava (`created_at_ds` e `updated_at_ds`). Um
        incidente que já existe só é alterado se o status ou as tentativas
        mudaram; igual, ele não conta como novo nem como atualizado.
        """
        ...

    def latest_end_date(self) -> datetime | None:
        """O maior `last_end_date` do registro, ou None se ele está vazio."""
        ...

    def open_count(self) -> int:
        """Quantos incidentes não estão `recovered`."""
        ...

    def list_open(self) -> list[IncidentKey]:
        """As chaves dos incidentes que não estão `recovered`."""
        ...
