"""Poller da detecção: lê os incidentes do Airflow e grava no registro.

`python -m lakehouse_ops_collector.detection.poller` roda uma rodada a cada
30 s até o `Ctrl+C`; com `--once`, faz uma rodada e encerra.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from .adapters.airflow_api import AirflowApiSource
from .adapters.postgres import PostgresRegistry
from .ports import IncidentRegistry, RegistryUnavailable, SourceUnavailable, TaskInstanceSource
from .models import DetectedIncident
from .service import find_incidents, get_incident
from .settings import DetectionSettings, PollerSettings, RegistrySettings, load_settings

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


def _for_registry(incident: DetectedIncident) -> DetectedIncident:
    # No registro não existe `retrying`: depois de uma falha, a task que espera
    # uma nova tentativa ainda está em andamento.
    if incident.status == "retrying":
        return incident.model_copy(update={"status": "running"})
    return incident


def track_open(source: TaskInstanceSource, registry: IncidentRegistry) -> list[DetectedIncident]:
    """O estado atual, no Airflow, de cada incidente aberto do registro.

    Um incidente que a consulta por chave não reconhece (a run foi apagada ou a
    task ficou `skipped`, por exemplo) fica como está.
    """
    current = (get_incident(source, key) for key in registry.list_open())
    return [_for_registry(incident) for incident in current if incident is not None]


def run_once(source: TaskInstanceSource, registry: IncidentRegistry) -> RoundResult:
    """Uma rodada: busca as falhas novas desde o `since`, acompanha os abertos e grava o que mudou."""
    since = since_for(registry)
    found = find_incidents(source, since=since, limit=MAX_INCIDENTS_PER_ROUND)
    # Uma entrada por chave; o acompanhamento é lido depois, então vale mais.
    by_key = {i.key: i for i in found.incidents} | {i.key: i for i in track_open(source, registry)}
    counts = registry.save_all(list(by_key.values()), source=SOURCE)
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


def _one_line(error: Exception) -> str:
    # O psycopg quebra a mensagem em linhas; o laço mostra uma linha por rodada.
    return " ".join(str(error).split())


def _now() -> datetime:
    return datetime.now(UTC)


def run_loop(
    source: TaskInstanceSource,
    registry: IncidentRegistry,
    interval_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = _now,
    rounds: int | None = None,
) -> None:
    """Uma rodada a cada intervalo, até o Ctrl+C (`rounds` limita o laço nos testes).

    Uma rodada que falha não grava nada e não encerra o laço: a seguinte tenta
    de novo, a partir do mesmo `since`.
    """
    done = 0
    while rounds is None or done < rounds:
        try:
            line = format_round(run_once(source, registry), now())
        except (SourceUnavailable, RegistryUnavailable) as error:
            line = f"{_iso(now())}  rodada falhou: {_one_line(error)}"
        print(line, flush=True)
        done += 1
        if rounds is None or done < rounds:
            sleep(interval_seconds)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m lakehouse_ops_collector.detection.poller")
    parser.add_argument("--once", action="store_true", help="faz uma rodada e encerra")
    args = parser.parse_args(argv)

    source = AirflowApiSource.from_settings(load_settings(DetectionSettings))
    registry = PostgresRegistry(load_settings(RegistrySettings))
    if args.once:
        try:
            result = run_once(source, registry)
        except (SourceUnavailable, RegistryUnavailable) as error:
            raise SystemExit(f"rodada falhou: {_one_line(error)}") from None
        print(format_round(result, _now()))
        return

    interval = load_settings(PollerSettings).poller_interval_seconds
    print(f"poller iniciado: uma rodada a cada {interval:g} s; Ctrl+C encerra", flush=True)
    try:
        run_loop(source, registry, interval)
    except KeyboardInterrupt:
        print("poller encerrado")


if __name__ == "__main__":
    main()
