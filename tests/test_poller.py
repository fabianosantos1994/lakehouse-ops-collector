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

    def list_task_instances(self, states, since=None, dag_id=None, limit=100):
        self.calls.append({"states": list(states), "since": since, "limit": limit})
        return [
            r for r in self.records
            if r.state in states and (since is None or (r.end_date and r.end_date >= since))
        ]

    def get_task_instance(self, dag_id, run_id, task_id, map_index=-1):
        raise AssertionError("o poller da etapa 3 não consulta por chave")

    def list_tries(self, dag_id, run_id, task_id, map_index=-1):
        current = next(r for r in self.records if (r.run_id, r.task_id) == (run_id, task_id))
        return [
            current.model_copy(update={"try_number": n, "end_date": current.end_date - timedelta(minutes=5 * (current.try_number - n))})
            for n in range(1, current.try_number + 1)
        ]

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
        ends = [i.attempts[-1].end_date for i in self.rows.values() if i.attempts]
        return max(ends, default=None)

    def open_count(self):
        return len(self.rows)


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


def test_round_line_shows_since_and_counts():
    result = RoundResult(since=datetime(2026, 10, 18, 23, 48, tzinfo=UTC), read=1, new=0, updated=0, open=4)

    line = format_round(result, datetime(2026, 10, 20, 12, 0, tzinfo=UTC))

    assert line == "2026-10-20T12:00:00Z  since=2026-10-18T23:48:00Z  lidos=1  novos=0  atualizados=0  abertos=4"


def test_round_line_warns_when_the_cap_is_reached():
    result = RoundResult(since=None, read=MAX_INCIDENTS_PER_ROUND, new=0, updated=0, open=0)

    assert "incidentes mais antigos ficaram de fora" in format_round(result, T0)


def test_poller_without_once_explains_that_the_loop_is_step_4():
    with pytest.raises(SystemExit) as error:
        poller.main([])

    assert "etapa 4" in str(error.value)


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
