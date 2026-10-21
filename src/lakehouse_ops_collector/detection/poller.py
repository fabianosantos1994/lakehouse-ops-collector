"""Poller da detecção: lê os incidentes do Airflow e grava no registro.

`python -m lakehouse_ops_collector.detection.poller --once` faz uma rodada e
encerra. O laço contínuo de 30 s é da etapa 4 da fase 2.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from .adapters.airflow_api import AirflowApiSource
from .adapters.postgres import PostgresRegistry
from .ports import IncidentRegistry, RegistryUnavailable, SourceUnavailable, TaskInstanceSource
from .service import find_incidents
from .settings import DetectionSettings, RegistrySettings, load_settings

# Cobre duas tasks que terminam quase juntas, quando uma só aparece como
# `failed` no Airflow depois da leitura. Reler um incidente não duplica nada.
SINCE_MARGIN = timedelta(minutes=5)

# Teto de incidentes lidos por rodada; o adapter pagina até ele. Com o registro
# vazio, a primeira rodada lê todo o histórico, do mais recente para o mais
# antigo: acima do teto, os mais antigos ficam de fora.
MAX_INCIDENTS_PER_ROUND = 10_000

# Modo gravado em `created_at_ds` e `updated_at_ds` do registro.
SOURCE = "poller"


class RoundResult(NamedTuple):
    since: datetime | None
    read: int
    new: int
    updated: int
    open: int


def since_for(registry: IncidentRegistry) -> datetime | None:
    """O maior `last_end_date` do registro menos a margem; None com o registro vazio."""
    latest = registry.latest_end_date()
    return latest - SINCE_MARGIN if latest is not None else None


def run_once(source: TaskInstanceSource, registry: IncidentRegistry) -> RoundResult:
    """Uma rodada: busca os incidentes `failed` desde o `since` e grava os novos e os alterados."""
    since = since_for(registry)
    found = find_incidents(source, since=since, limit=MAX_INCIDENTS_PER_ROUND)
    counts = registry.save_all(found.incidents, source=SOURCE)
    return RoundResult(
        since=since, read=found.count, new=counts.new, updated=counts.updated, open=registry.open_count()
    )


def _iso(value: datetime | None) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z") if value else "-"


def format_round(result: RoundResult, at: datetime) -> str:
    line = (
        f"{_iso(at)}  since={_iso(result.since)}  lidos={result.read}  "
        f"novos={result.new}  atualizados={result.updated}  abertos={result.open}"
    )
    if result.read >= MAX_INCIDENTS_PER_ROUND:
        line += f"  (aviso: atingiu o teto de {MAX_INCIDENTS_PER_ROUND}; incidentes mais antigos ficaram de fora)"
    return line


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m lakehouse_ops_collector.detection.poller")
    parser.add_argument("--once", action="store_true", help="faz uma rodada e encerra")
    args = parser.parse_args(argv)
    if not args.once:
        raise SystemExit("o modo contínuo ainda não está implementado (etapa 4); use --once")

    source = AirflowApiSource.from_settings(load_settings(DetectionSettings))
    registry = PostgresRegistry(load_settings(RegistrySettings))
    try:
        result = run_once(source, registry)
    except (SourceUnavailable, RegistryUnavailable) as error:
        raise SystemExit(f"rodada falhou: {error}") from None
    print(format_round(result, datetime.now(UTC)))


if __name__ == "__main__":
    main()
