from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from lakehouse_ops_collector.detection.adapters.airflow_api import AirflowApiSource
from lakehouse_ops_collector.detection.api import create_app
from lakehouse_ops_collector.detection.models import TaskInstanceRecord
from lakehouse_ops_collector.detection.ports import SourceUnavailable
from lakehouse_ops_collector.detection.service import find_incidents, get_incident
from lakehouse_ops_collector.incident import IncidentKey

DAG = "commercial_daily_sales_by_region"


def airflow_item(run_id, task_id, state="failed", try_number=2, end="2026-10-18T20:59:07Z"):
    return {
        "dag_id": DAG,
        "dag_run_id": run_id,
        "task_id": task_id,
        "map_index": -1,
        "state": state,
        "try_number": try_number,
        "max_tries": 1,
        "operator": "LakehouseDataQualityOperator",
        "start_date": "2026-10-18T20:58:58Z",
        "end_date": end,
        # Campos que a detecção não usa e não devem vazar para a resposta.
        "hostname": "airflow-scheduler",
        "rendered_fields": {"config": "x"},
    }


RI_001 = airflow_item("backfill__2026-10-15T00:00:00+00:00", "validate_daily_sales_by_region")
RI_003 = airflow_item("backfill__2026-10-17T00:00:00+00:00", "build_daily_sales_by_region", end="2026-10-18T22:26:30Z")


class FakeAirflow:
    """Simula a API do Airflow e guarda as requisições recebidas."""

    def __init__(self, task_instances, token_status=201):
        self.task_instances = task_instances
        self.token_status = token_status
        self.requests: list[httpx.Request] = []
        self.tokens_issued = 0
        self.expire_next = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/auth/token":
            self.tokens_issued += 1
            return httpx.Response(self.token_status, json={"access_token": f"t{self.tokens_issued}"})
        if path == "/api/v2/monitor/health":
            return httpx.Response(200, json={"scheduler": {"status": "healthy"}})
        if self.expire_next:
            self.expire_next = False
            return httpx.Response(401)
        if path.endswith("/dagRuns/~/taskInstances"):
            states = request.url.params.get_list("state")
            offset = int(request.url.params["offset"])
            limit = int(request.url.params["limit"])
            matching = [item for item in self.task_instances if item["state"] in states]
            return httpx.Response(200, json={"task_instances": matching[offset:offset + limit],
                                             "total_entries": len(matching)})
        for item in self.task_instances:
            base = f"/api/v2/dags/{DAG}/dagRuns/{item['dag_run_id']}/taskInstances/{item['task_id']}"
            if path == base:
                return httpx.Response(200, json=item)
            if path == base + "/tries":
                return httpx.Response(200, json={"task_instances": tries_of(item)})
        return httpx.Response(404, json={"detail": "not found"})


def tries_of(item):
    """Histórico como o Airflow devolve: as tentativas anteriores falharam, a última é a atual."""
    earlier = [
        {**item, "try_number": n, "state": "failed",
         "start_date": f"2026-10-18T20:5{n}:00Z", "end_date": f"2026-10-18T20:5{n}:04Z"}
        for n in range(1, item["try_number"])
    ]
    return earlier + [item]


def source_for(fake):
    client = httpx.Client(base_url="http://airflow", transport=httpx.MockTransport(fake))
    return AirflowApiSource(client, "admin", "secret")


def test_lists_failed_task_instances_across_all_dags():
    fake = FakeAirflow([RI_001, RI_003])

    records = source_for(fake).list_task_instances(["failed"])

    assert [r.task_id for r in records] == ["validate_daily_sales_by_region", "build_daily_sales_by_region"]
    request = fake.requests[-1]
    assert request.url.path == "/api/v2/dags/~/dagRuns/~/taskInstances"
    assert request.headers["Authorization"] == "Bearer t1"
    assert request.url.params["order_by"] == "-end_date"


def test_sends_since_and_dag_filters():
    fake = FakeAirflow([RI_001])
    since = datetime(2026, 10, 18, 21, 0, tzinfo=UTC)

    source_for(fake).list_task_instances(["failed"], since=since, dag_id=DAG)

    request = fake.requests[-1]
    assert request.url.path == f"/api/v2/dags/{DAG}/dagRuns/~/taskInstances"
    assert request.url.params["end_date_gte"] == "2026-10-18T21:00:00+00:00"


def test_paginates_until_the_limit():
    items = [airflow_item(f"run_{i}", "t") for i in range(250)]
    fake = FakeAirflow(items)

    records = source_for(fake).list_task_instances(["failed"], limit=230)

    assert len(records) == 230
    assert [r.url.params["offset"] for r in fake.requests if r.url.path.endswith("taskInstances")] == ["0", "100", "200"]


def test_renews_an_expired_token_once():
    fake = FakeAirflow([RI_001])
    source = source_for(fake)
    source.list_task_instances(["failed"])
    fake.expire_next = True

    source.list_task_instances(["failed"])

    assert fake.tokens_issued == 2


def test_failed_authentication_is_reported_as_unavailable():
    with pytest.raises(SourceUnavailable, match="authentication failed"):
        source_for(FakeAirflow([], token_status=401)).list_task_instances(["failed"])


def test_unreachable_airflow_is_reported_as_unavailable():
    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    client = httpx.Client(base_url="http://airflow", transport=httpx.MockTransport(refuse))

    with pytest.raises(SourceUnavailable, match="airflow unavailable"):
        AirflowApiSource(client, "admin", "secret").list_task_instances(["failed"])


def test_retrying_tasks_are_only_included_when_asked():
    retrying = airflow_item("scheduled__2026-10-19T00:00:00+00:00", "t", state="up_for_retry", try_number=1)
    source = source_for(FakeAirflow([RI_001, retrying]))

    only_failed = find_incidents(source)
    with_retrying = find_incidents(source, include_retrying=True)

    assert [i.status for i in only_failed.incidents] == ["failed"]
    assert sorted(i.status for i in with_retrying.incidents) == ["failed", "retrying"]


def client_for(fake):
    return TestClient(create_app(source_for(fake)))


def test_api_lists_incidents_with_the_collector_key():
    response = client_for(FakeAirflow([RI_001, RI_003])).get("/incidents")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["incidents"][0]["key"] == {
        "dag_id": DAG,
        "run_id": "backfill__2026-10-15T00:00:00+00:00",
        "task_id": "validate_daily_sales_by_region",
        "map_index": -1,
    }
    assert body["incidents"][0]["status"] == "failed"
    assert "hostname" not in body["incidents"][0]


def test_lists_every_attempt_in_order():
    retrying = airflow_item("scheduled__2026-10-19T00:00:00+00:00", "t", state="up_for_retry", try_number=1)
    source = source_for(FakeAirflow([RI_001, retrying]))

    incidents = {i.status: i for i in find_incidents(source, include_retrying=True).incidents}

    failed = incidents["failed"]
    assert [(a.number, a.cycle, a.state) for a in failed.attempts] == [(1, 1, "failed"), (2, 1, "failed")]
    assert {a.operator for a in failed.attempts} == {"LakehouseDataQualityOperator"}
    assert failed.attempts[0].end_date < failed.attempts[1].start_date
    retrying_incident = incidents["retrying"]
    assert [(a.number, a.state) for a in retrying_incident.attempts] == [(1, "up_for_retry")]


class HistorySource:
    """Fonte com um histórico de tentativas montado à mão."""

    def __init__(self, tries):
        self.tries = tries

    def get_task_instance(self, dag_id, run_id, task_id, map_index=-1):
        return self.tries[-1]

    def list_tries(self, dag_id, run_id, task_id, map_index=-1):
        return self.tries


def attempt_record(number, max_tries, state="failed", operator="LakehouseSparkSubmitOperator"):
    return TaskInstanceRecord(
        dag_id=DAG, run_id="r", task_id="t", map_index=-1, state=state,
        try_number=number, max_tries=max_tries, operator=operator,
    )


def test_a_clear_starts_a_new_cycle():
    # retries: 1. Duas tentativas falham (max_tries 1); o clear recalcula
    # max_tries = 2 + 1 = 3, e as tentativas 3 e 4 formam o ciclo 2.
    source = HistorySource([
        attempt_record(1, 1), attempt_record(2, 1),
        attempt_record(3, 3), attempt_record(4, 3),
    ])

    incident = get_incident(source, IncidentKey(dag_id=DAG, run_id="r", task_id="t"))

    assert [(a.number, a.cycle) for a in incident.attempts] == [(1, 1), (2, 1), (3, 2), (4, 2)]


def test_each_attempt_keeps_its_own_operator():
    source = HistorySource([
        attempt_record(1, 1, operator="LakehouseSparkSubmitOperator"),
        attempt_record(2, 2, operator="LakehouseTrinoOperator"),
    ])

    incident = get_incident(source, IncidentKey(dag_id=DAG, run_id="r", task_id="t"))

    assert [a.operator for a in incident.attempts] == ["LakehouseSparkSubmitOperator", "LakehouseTrinoOperator"]


def test_api_shows_attempts_without_the_airflow_field_names():
    body = client_for(FakeAirflow([RI_001])).get("/incidents").json()

    incident = body["incidents"][0]
    assert [(a["number"], a["cycle"]) for a in incident["attempts"]] == [(1, 1), (2, 1)]
    assert set(incident) == {"key", "status", "attempts"}
    assert set(incident["attempts"][0]) == {"number", "cycle", "state", "operator", "start_date", "end_date"}


def test_api_reads_one_incident_with_an_encoded_run_id():
    client = client_for(FakeAirflow([RI_001]))

    response = client.get(f"/incidents/{DAG}/backfill__2026-10-15T00%3A00%3A00%2B00%3A00/validate_daily_sales_by_region")

    assert response.status_code == 200
    assert response.json()["key"]["run_id"] == "backfill__2026-10-15T00:00:00+00:00"


def test_api_returns_404_for_a_task_that_did_not_fail():
    # Passou na primeira tentativa: nunca falhou, então não é incidente.
    succeeded = airflow_item("backfill__2026-10-15T00:00:00+00:00", "build_daily_sales_by_region", state="success", try_number=1)
    client = client_for(FakeAirflow([succeeded]))

    response = client.get(f"/incidents/{DAG}/backfill__2026-10-15T00%3A00%3A00%2B00%3A00/build_daily_sales_by_region")

    assert response.status_code == 404


def test_api_reads_an_incident_that_recovered_after_a_failure():
    # Falhou na tentativa 1 e passou na 2: antes da fase 2 era 404.
    recovered = airflow_item("backfill__2026-10-15T00:00:00+00:00", "validate_daily_sales_by_region", state="success")
    client = client_for(FakeAirflow([recovered]))

    body = client.get(f"/incidents/{DAG}/backfill__2026-10-15T00%3A00%3A00%2B00%3A00/validate_daily_sales_by_region").json()

    assert body["status"] == "recovered"
    assert [(a["number"], a["state"]) for a in body["attempts"]] == [(1, "failed"), (2, "success")]


def test_a_cleared_task_waiting_to_be_scheduled_is_running_and_keeps_the_failed_try():
    source = HistorySource([
        attempt_record(1, 1), attempt_record(2, 1),
        attempt_record(2, 3, state=None),
    ])

    incident = get_incident(source, IncidentKey(dag_id=DAG, run_id="r", task_id="t"))

    assert incident.status == "running"
    assert [(a.number, a.cycle, a.state) for a in incident.attempts] == [(1, 1, "failed"), (2, 1, "failed")]


def test_api_rejects_a_naive_since():
    response = client_for(FakeAirflow([])).get("/incidents", params={"since": "2026-10-18T21:00:00"})

    assert response.status_code == 422


def test_api_returns_502_when_airflow_is_down_and_health_reports_it():
    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    client = httpx.Client(base_url="http://airflow", transport=httpx.MockTransport(refuse))
    api = TestClient(create_app(AirflowApiSource(client, "admin", "secret")))

    assert api.get("/incidents").status_code == 502
    health = api.get("/health").json()
    assert health["status"] == "ok"
    assert health["airflow"]["status"] == "error"


def test_record_model_keeps_only_what_detection_uses():
    assert set(TaskInstanceRecord.model_fields) == {
        "dag_id", "run_id", "task_id", "map_index", "state", "try_number", "max_tries",
        "operator", "start_date", "end_date",
    }


def test_missing_credentials_explain_how_to_create_the_env(tmp_path, monkeypatch):
    from lakehouse_ops_collector.detection.settings import DetectionSettings, load_settings

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AIRFLOW_USERNAME", raising=False)
    monkeypatch.delenv("AIRFLOW_PASSWORD", raising=False)

    with pytest.raises(SystemExit) as error:
        load_settings(DetectionSettings)

    message = str(error.value)
    assert "AIRFLOW_PASSWORD, AIRFLOW_USERNAME" in message
    assert "cp .env.example .env" in message
