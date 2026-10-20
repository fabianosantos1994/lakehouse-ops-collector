"""Fonte de tentativas pela API REST v2 do Airflow, somente leitura."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from urllib.parse import quote

import httpx

from ..models import TaskInstanceRecord
from ..ports import SourceUnavailable

_PAGE_SIZE = 100


class AirflowApiSource:
    def __init__(self, client: httpx.Client, username: str, password: str) -> None:
        self._client = client
        self._username = username
        self._password = password
        self._token: str | None = None

    def _authenticate(self) -> str:
        response = self._client.post(
            "/auth/token", json={"username": self._username, "password": self._password}
        )
        if response.status_code not in (200, 201):
            raise SourceUnavailable(f"airflow authentication failed: HTTP {response.status_code}")
        self._token = response.json()["access_token"]
        return self._token

    def _get(self, path: str, params: list[tuple[str, str]] | None = None) -> httpx.Response:
        try:
            token = self._token or self._authenticate()
            response = self._client.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
            if response.status_code == 401:
                # Token expirado: renova uma vez.
                token = self._authenticate()
                response = self._client.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
        except httpx.HTTPError as error:
            raise SourceUnavailable(f"airflow unavailable: {error}") from error
        return response

    def list_task_instances(
        self,
        states: Sequence[str],
        since: datetime | None = None,
        dag_id: str | None = None,
        limit: int = 100,
    ) -> list[TaskInstanceRecord]:
        dag = quote(dag_id, safe="") if dag_id else "~"
        path = f"/api/v2/dags/{dag}/dagRuns/~/taskInstances"
        records: list[TaskInstanceRecord] = []
        offset = 0
        while len(records) < limit:
            page_size = min(_PAGE_SIZE, limit - len(records))
            params = [("state", state) for state in states]
            params += [("order_by", "-end_date"), ("limit", str(page_size)), ("offset", str(offset))]
            if since is not None:
                params.append(("end_date_gte", since.isoformat()))
            response = self._get(path, params)
            if response.status_code == 404 and dag_id:
                return []
            if response.status_code != 200:
                raise SourceUnavailable(f"airflow returned HTTP {response.status_code} for {path}")
            page = response.json()["task_instances"]
            records += [_record(item) for item in page]
            if len(page) < page_size:
                break
            offset += len(page)
        return records

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> TaskInstanceRecord | None:
        path = _task_instance_path(dag_id, run_id, task_id, map_index)
        response = self._get(path)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise SourceUnavailable(f"airflow returned HTTP {response.status_code} for {path}")
        return _record(response.json())

    def list_tries(
        self, dag_id: str, run_id: str, task_id: str, map_index: int = -1
    ) -> list[TaskInstanceRecord]:
        path = _task_instance_path(dag_id, run_id, task_id, map_index) + "/tries"
        response = self._get(path)
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            raise SourceUnavailable(f"airflow returned HTTP {response.status_code} for {path}")
        return [_record(item) for item in response.json()["task_instances"]]

    def health(self) -> dict:
        try:
            response = self._client.get("/api/v2/monitor/health")
        except httpx.HTTPError as error:
            raise SourceUnavailable(f"airflow unavailable: {error}") from error
        if response.status_code != 200:
            raise SourceUnavailable(f"airflow health returned HTTP {response.status_code}")
        return response.json()


def _task_instance_path(dag_id: str, run_id: str, task_id: str, map_index: int) -> str:
    path = "/api/v2/dags/{}/dagRuns/{}/taskInstances/{}".format(
        quote(dag_id, safe=""), quote(run_id, safe=""), quote(task_id, safe="")
    )
    if map_index != -1:
        path += f"/{map_index}"
    return path


def _record(item: dict) -> TaskInstanceRecord:
    return TaskInstanceRecord(
        dag_id=item["dag_id"],
        run_id=item["dag_run_id"],
        task_id=item["task_id"],
        map_index=item["map_index"],
        state=item["state"],
        try_number=item["try_number"],
        max_tries=item["max_tries"],
        operator=item.get("operator"),
        start_date=item.get("start_date"),
        end_date=item.get("end_date"),
    )
