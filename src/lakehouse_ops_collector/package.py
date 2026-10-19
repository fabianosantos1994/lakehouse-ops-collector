"""Formato publicado do pacote do incidente.

É o contrato com o `lakehouse-ai-ops`: mudar ou remover um campo exige subir
`PACKAGE_VERSION`; acrescentar campo não exige.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field

PACKAGE_VERSION = 1


def _path_safe(value: str) -> str:
    # Os identificadores viram partes do caminho do pacote em disco.
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


class IncidentPackage(BaseModel):
    """Pacote de um incidente, lido pelo diagnóstico no `lakehouse-ai-ops`."""

    model_config = ConfigDict(extra="forbid")

    package_version: Literal[1] = PACKAGE_VERSION
    collected_at: AwareDatetime
    incident: IncidentKey


def utc_now() -> datetime:
    return datetime.now(UTC)
