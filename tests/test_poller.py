from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from lakehouse_ops_collector.detection import poller
from lakehouse_ops_collector.detection.models import DetectedIncident, TaskInstanceRecord
from lakehouse_ops_collector.detection.poller import (
    MAX_INCIDENTS_PER_ROUND,
    RoundResult,
    format_round,
    run_loop,
    run_once,
)
from lakehouse_ops_collector.detection.ports import SaveCounts, SourceUnavailable

DAG = "commercial_daily_sales_by_region"
T0 = datetime(2026, 10, 18, 20, 53, 54, tzinfo=UTC)


def record(run_id, task_id, try_number=2, end=T0 + timedelta(minutes=5), state="failed"):
    return TaskInstanceRecord(
        dag_id=DAG,
        run_id=run_id,
        task_id=task_id,
        map_index=-1,
        state=state,
        try_number=try_number,
        max_tries=1,
        operator="LakehouseDataQualityOperator",
        start_date=end - timedelta(seconds=9),
        end_date=end,
    )


class FakeSource:
    """Airflow simulado: tentativas atuais e histórico por task."""

    def __init__(self, records: list[TaskInstanceRecord]):
        self.records = records
        self.calls: list[dict] = []
        # Histórico montado à mão por (run_id, task_id), para simular o clear.
        self.history: dict[tuple, list[TaskInstanceRecord]] = {}

    def list_task_instances(self, states, since=None, dag_id=None, limit=100):
        self.calls.append({"states": list(states), "since": since, "limit": limit})
        return [
            r for r in self.records
            if r.state in states and (since is None or (r.end_date and r.end_date >= since))
        ]

    def get_task_instance(self, dag_id, run_id, task_id, map_index=-1):
        return next((r for r in self.records if (r.run_id, r.task_id) == (run_id, task_id)), None)

    def list_tries(self, dag_id, run_id, task_id, map_index=-1):
        """Tentativas anteriores com falha, 5 min uma da outra, e a atual por último."""
        if (run_id, task_id) in self.history:
            return self.history[(run_id, task_id)]
        current = self.get_task_instance(dag_id, run_id, task_id, map_index)
        return [
            current.model_copy(update={
                "try_number": n, "state": "failed",
                "end_date": current.end_date - timedelta(minutes=5 * (current.try_number - n)),
            })
            for n in range(1, current.try_number)
        ] + [current]

    def health(self):
        return {}


class FakeRegistry:
    """Registro em memória com a mesma regra do adapter: só conta o que mudou."""

    def __init__(self):
        self.rows: dict[tuple, DetectedIncident] = {}
        self.sources: list[str] = []

    def save_all(self, incidents: Sequence[DetectedIncident], source: str) -> SaveCounts:
        self.sources.append(source)
        new = updated = 0
        for incident in incidents:
            key = tuple(incident.key.model_dump().values())
            previous = self.rows.get(key)
            if previous is None:
                new += 1
            elif (previous.status, previous.attempts) != (incident.status, incident.attempts):
                updated += 1
            else:
                continue
            self.rows[key] = incident
        return SaveCounts(new=new, updated=updated)

    def latest_end_date(self):
        # Como o max() do PostgreSQL, ignora a tentativa que ainda não terminou.
        ends = [i.attempts[-1].end_date for i in self.rows.values() if i.attempts and i.attempts[-1].end_date]
        return max(ends, default=None)

    def open_count(self):
        return len(self.list_open())

    def list_open(self):
        return [i.key for i in self.rows.values() if i.status != "recovered"]


def four_incidents():
    return FakeSource(
        [
            record("backfill__2026-10-15T00:00:00+00:00", "validate_daily_sales_by_region"),
            record("backfill__2026-10-16T00:00:00+00:00", "validate_daily_sales_by_region", end=T0 + timedelta(hours=1)),
            record("backfill__2026-10-17T00:00:00+00:00", "build_daily_sales_by_region", end=T0 + timedelta(hours=2)),
            record("scheduled__2026-10-18T00:00:00+00:00", "validate_daily_sales_by_region", end=T0 + timedelta(hours=3)),
        ]
    )


def test_first_round_reads_the_whole_history_and_registers_every_failure():
    source, registry = four_incidents(), FakeRegistry()

    result = run_once(source, registry)

    assert result == RoundResult(since=None, read=4, new=4, updated=0, open=4)
    assert source.calls == [{"states": ["failed"], "since": None, "limit": MAX_INCIDENTS_PER_ROUND}]
    assert {i.status for i in registry.rows.values()} == {"failed"}
    assert registry.sources == ["poller"]


def test_second_round_starts_from_the_latest_end_date_minus_the_margin_and_adds_nothing():
    source, registry = four_incidents(), FakeRegistry()
    run_once(source, registry)

    result = run_once(source, registry)

    latest = T0 + timedelta(hours=3)
    assert result == RoundResult(since=latest - timedelta(minutes=5), read=1, new=0, updated=0, open=4)
    assert source.calls[-1]["since"] == latest - timedelta(minutes=5)


def test_a_new_attempt_updates_the_incident_instead_of_duplicating_it():
    source, registry = four_incidents(), FakeRegistry()
    run_once(source, registry)
    latest = source.records[-1]
    source.records[-1] = latest.model_copy(
        update={"try_number": 4, "max_tries": 3, "end_date": latest.end_date + timedelta(minutes=10)}
    )

    result = run_once(source, registry)

    assert (result.new, result.updated, result.open) == (0, 1, 4)
    attempts = registry.rows[(DAG, latest.run_id, latest.task_id, -1)].attempts
    assert [a.number for a in attempts] == [1, 2, 3, 4]


def test_retrying_tasks_are_not_registered():
    source = FakeSource([record("scheduled__2026-10-18T00:00:00+00:00", "validate_daily_sales_by_region", state="up_for_retry")])
    registry = FakeRegistry()

    assert run_once(source, registry).new == 0
    assert registry.rows == {}


RUN_18 = "scheduled__2026-10-18T00:00:00+00:00"
TASK = "validate_daily_sales_by_region"


def clear_run_18(source, state, end=None, try_3_state=None):
    """Simula o clear da task da run do dia 18 (retries: 1).

    O clear recalcula max_tries = 2 + 1 = 3; a tentativa 3 abre o ciclo 2.
    `state=None` é a task limpa antes de ser agendada: ainda com o número 2.
    """
    index = next(i for i, r in enumerate(source.records) if r.run_id == RUN_18)
    failed = [t for t in source.list_tries(DAG, RUN_18, TASK) if t.try_number <= 2]
    if state is None:
        current = failed[-1].model_copy(update={"state": None, "max_tries": 3, "start_date": None, "end_date": None})
        source.history[(RUN_18, TASK)] = failed
    else:
        current = failed[-1].model_copy(update={
            "try_number": 3, "max_tries": 3, "state": state,
            "start_date": failed[-1].end_date + timedelta(hours=60), "end_date": end,
        })
        source.history[(RUN_18, TASK)] = failed + [current]
    source.records[index] = current


def test_a_cleared_task_goes_from_failed_to_running_to_recovered(capsys):
    source, registry = four_incidents(), FakeRegistry()
    run_once(source, registry)

    clear_run_18(source, state=None)
    waiting = run_once(source, registry)
    assert (waiting.new, waiting.updated, waiting.open) == (0, 1, 4)
    incident = registry.rows[(DAG, RUN_18, TASK, -1)]
    assert incident.status == "running"
    # Antes de agendada, a task limpa não substitui a tentativa 2, que falhou.
    assert [(a.number, a.state) for a in incident.attempts] == [(1, "failed"), (2, "failed")]

    clear_run_18(source, state="running")
    running = run_once(source, registry)
    assert (running.new, running.updated, running.open) == (0, 1, 4)
    attempts = registry.rows[(DAG, RUN_18, TASK, -1)].attempts
    assert [(a.number, a.cycle, a.state) for a in attempts] == [(1, 1, "failed"), (2, 1, "failed"), (3, 2, "running")]

    end = T0 + timedelta(hours=63)
    clear_run_18(source, state="success", end=end)
    recovered = run_once(source, registry)
    assert (recovered.new, recovered.updated, recovered.open) == (0, 1, 3)
    incident = registry.rows[(DAG, RUN_18, TASK, -1)]
    assert incident.status == "recovered"
    assert [(a.number, a.cycle, a.state) for a in incident.attempts][-1] == (3, 2, "success")

    # Recuperado, sai do acompanhamento: a rodada seguinte não muda nada.
    after = run_once(source, registry)
    assert (after.new, after.updated, after.open) == (0, 0, 3)
    assert after.since == end - timedelta(minutes=5)


def test_a_cleared_task_that_fails_again_returns_to_failed_with_cycle_2():
    source, registry = four_incidents(), FakeRegistry()
    run_once(source, registry)

    clear_run_18(source, state="up_for_retry", end=T0 + timedelta(hours=63))
    run_once(source, registry)
    assert registry.rows[(DAG, RUN_18, TASK, -1)].status == "running"

    index = next(i for i, r in enumerate(source.records) if r.run_id == RUN_18)
    third = source.records[index]
    fourth = third.model_copy(update={"try_number": 4, "state": "failed", "end_date": third.end_date + timedelta(minutes=5)})
    source.history[(RUN_18, TASK)] = source.history[(RUN_18, TASK)][:-1] + [third.model_copy(update={"state": "failed"}), fourth]
    source.records[index] = fourth

    result = run_once(source, registry)

    incident = registry.rows[(DAG, RUN_18, TASK, -1)]
    assert (result.updated, result.open) == (1, 4)
    assert incident.status == "failed"
    assert [(a.number, a.cycle) for a in incident.attempts] == [(1, 1), (2, 1), (3, 2), (4, 2)]


def test_an_open_incident_that_the_key_lookup_does_not_recognize_stays_unchanged():
    source, registry = four_incidents(), FakeRegistry()
    run_once(source, registry)
    index = next(i for i, r in enumerate(source.records) if r.run_id == RUN_18)
    source.records[index] = source.records[index].model_copy(update={"state": "upstream_failed"})

    result = run_once(source, registry)

    assert (result.updated, result.open) == (0, 4)
    assert registry.rows[(DAG, RUN_18, TASK, -1)].status == "failed"


def test_round_line_shows_since_and_counts():
    result = RoundResult(since=datetime(2026, 10, 18, 23, 48, tzinfo=UTC), read=1, new=0, updated=0, open=4)

    line = format_round(result, datetime(2026, 10, 20, 12, 0, tzinfo=UTC))

    assert line == "2026-10-20T12:00:00Z  since=2026-10-18T23:48:00Z  lidos=1  novos=0  atualizados=0  abertos=4"


def test_round_line_warns_when_the_cap_is_reached():
    result = RoundResult(since=None, read=MAX_INCIDENTS_PER_ROUND, new=0, updated=0, open=0)

    assert "incidentes mais antigos ficaram de fora" in format_round(result, T0)


class Clock:
    """Relógio e pausa simulados: cada pausa avança o relógio."""

    def __init__(self):
        self.at = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
        self.sleeps: list[float] = []

    def now(self):
        return self.at

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.at += timedelta(seconds=seconds)


def test_loop_runs_one_round_per_interval_and_prints_a_line_per_round(capsys):
    source, registry, clock = four_incidents(), FakeRegistry(), Clock()

    run_loop(source, registry, 30, sleep=clock.sleep, now=clock.now, rounds=3)

    lines = capsys.readouterr().out.splitlines()
    assert clock.sleeps == [30, 30]
    assert [line.split("  ")[0] for line in lines] == [
        "2026-10-21T12:00:00Z", "2026-10-21T12:00:30Z", "2026-10-21T12:01:00Z",
    ]
    assert "novos=4" in lines[0]
    assert all("novos=0  atualizados=0  abertos=4" in line for line in lines[1:])
    assert len(registry.rows) == 4


def test_a_failed_round_does_not_stop_the_loop(capsys):
    class FlakySource(FakeSource):
        def list_task_instances(self, *args, **kwargs):
            if not self.calls:
                self.calls.append({})
                raise SourceUnavailable("airflow unavailable: connection refused\n\tIs the server running?")
            return super().list_task_instances(*args, **kwargs)

    source = FlakySource(four_incidents().records)
    registry, clock = FakeRegistry(), Clock()

    run_loop(source, registry, 30, sleep=clock.sleep, now=clock.now, rounds=2)

    first, second = capsys.readouterr().out.splitlines()
    assert first == "2026-10-21T12:00:00Z  rodada falhou: airflow unavailable: connection refused Is the server running?"
    assert "since=-  lidos=4  novos=4" in second
    assert len(registry.rows) == 4


def test_restarting_continues_from_the_registry_without_duplicating(capsys):
    registry, clock = FakeRegistry(), Clock()
    run_loop(four_incidents(), registry, 30, sleep=clock.sleep, now=clock.now, rounds=1)

    # Novo processo: outra fonte, o mesmo registro.
    run_loop(four_incidents(), registry, 30, sleep=clock.sleep, now=clock.now, rounds=1)

    second = capsys.readouterr().out.splitlines()[1]
    assert "since=2026-10-18T23:48:54Z  lidos=1  novos=0  atualizados=0  abertos=4" in second
    assert len(registry.rows) == 4


def env_for_main(tmp_path, monkeypatch, source, registry, interval="0.01"):
    monkeypatch.chdir(tmp_path)
    for name, value in {
        "AIRFLOW_USERNAME": "u", "AIRFLOW_PASSWORD": "p", "OPS_DB_PASSWORD": "s",
        "POLLER_INTERVAL_SECONDS": interval,
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(poller.AirflowApiSource, "from_settings", classmethod(lambda cls, s: source))
    monkeypatch.setattr(poller, "PostgresRegistry", lambda settings: registry)


def test_poller_without_once_loops_until_ctrl_c(tmp_path, monkeypatch, capsys):
    class InterruptedSource(FakeSource):
        def list_task_instances(self, *args, **kwargs):
            if self.calls:
                raise KeyboardInterrupt
            return super().list_task_instances(*args, **kwargs)

    env_for_main(tmp_path, monkeypatch, InterruptedSource(four_incidents().records), FakeRegistry())

    poller.main([])

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "poller iniciado: uma rodada a cada 0.01 s; Ctrl+C encerra"
    assert "novos=4" in lines[1]
    assert lines[2] == "poller encerrado"


def test_poller_rejects_a_non_positive_interval(tmp_path, monkeypatch):
    env_for_main(tmp_path, monkeypatch, four_incidents(), FakeRegistry(), interval="0")

    with pytest.raises(SystemExit) as error:
        poller.main([])

    assert "POLLER_INTERVAL_SECONDS" in str(error.value)


def test_poller_exits_with_the_error_when_airflow_is_down(tmp_path, monkeypatch):
    class DownSource(FakeSource):
        def list_task_instances(self, *args, **kwargs):
            raise SourceUnavailable("airflow unavailable: connection refused")

    monkeypatch.chdir(tmp_path)
    for name, value in {"AIRFLOW_USERNAME": "u", "AIRFLOW_PASSWORD": "p", "OPS_DB_PASSWORD": "s"}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(poller.AirflowApiSource, "from_settings", classmethod(lambda cls, s: DownSource([])))
    monkeypatch.setattr(poller, "PostgresRegistry", lambda settings: FakeRegistry())

    with pytest.raises(SystemExit) as error:
        poller.main(["--once"])

    assert str(error.value) == "rodada falhou: airflow unavailable: connection refused"
