"""Chave do incidente: o que a detecção devolve e o coletor recebe."""

from __future__ import annotations

from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


def _path_safe(value: str) -> str:
    # O coletor usa os identificadores como partes do caminho do pacote em disco.
    if value.strip() != value:
        raise ValueError("must not start or end with whitespace")
    if "/" in value or "\\" in value or value in {".", ".."}:
        raise ValueError("must not contain path separators or be '.' or '..'")
    return value


Identifier = Annotated[str, Field(min_length=1), AfterValidator(_path_safe)]


class IncidentKey(BaseModel):
    """Identifica um incidente: uma task com falha em uma DAG run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    dag_id: Identifier
    run_id: Identifier
    task_id: Identifier
    map_index: int = Field(default=-1, ge=-1)
