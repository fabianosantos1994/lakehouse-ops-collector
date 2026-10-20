"""Interfaces da fonte de tentativas e do registro; os adapters as implementam."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from .models import TaskInstanceRecord


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
