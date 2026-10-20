from __future__ import annotations

import re

import psycopg
import pytest

from lakehouse_ops_collector.detection import registry
from lakehouse_ops_collector.detection.adapters import postgres
from lakehouse_ops_collector.detection.adapters.postgres import PostgresRegistry, schema_sql
from lakehouse_ops_collector.detection.ports import RegistryUnavailable
from lakehouse_ops_collector.detection.settings import RegistrySettings, load_settings


class FakeConnection:
    """Simula a conexão do psycopg: guarda os comandos e responde ao to_regclass."""

    def __init__(self, table_exists=False, fail_on_schema=False):
        self.table_exists = table_exists
        self.fail_on_schema = fail_on_schema
        self.statements: list[str] = []
        self.connect_kwargs: dict = {}
        self.committed = False

    def __call__(self, **kwargs):
        self.connect_kwargs = kwargs
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        self.committed = exc_type is None
        return False

    def execute(self, query, params=None):
        self.statements.append(query)
        if query == schema_sql() and self.fail_on_schema:
            raise psycopg.errors.InsufficientPrivilege("permission denied for database ops")
        return self

    def fetchone(self):
        return (self.table_exists,)


def settings(**overrides) -> RegistrySettings:
    return RegistrySettings(ops_db_password="secret", **overrides)


def test_schema_only_creates_what_is_missing():
    ddl = schema_sql()
    creates = re.findall(r"CREATE \w+", ddl)

    assert creates == ["CREATE SCHEMA", "CREATE TABLE"]
    assert ddl.count("IF NOT EXISTS") == len(creates)
    for statement in ("DROP", "ALTER", "DELETE", "TRUNCATE", "INSERT", "UPDATE"):
        assert statement not in ddl


def test_schema_uses_the_incident_key_as_primary_key():
    ddl = schema_sql()

    assert "CREATE TABLE IF NOT EXISTS detection.incident" in ddl
    assert "PRIMARY KEY (dag_id, run_id, task_id, map_index)" in ddl
    assert "CHECK (status IN ('failed', 'running', 'recovered'))" in ddl
    assert "attempts      jsonb" in ddl


def test_init_creates_the_table_on_an_empty_database():
    connection = FakeConnection(table_exists=False)

    created = PostgresRegistry(settings(), connect=connection).init_schema()

    assert created is True
    assert connection.statements[-1] == schema_sql()
    assert connection.committed


def test_init_again_reports_that_nothing_changed():
    connection = FakeConnection(table_exists=True)

    assert PostgresRegistry(settings(), connect=connection).init_schema() is False


def test_connection_uses_the_registry_settings():
    connection = FakeConnection()

    PostgresRegistry(settings(ops_db_port=6000), connect=connection).init_schema()

    assert connection.connect_kwargs == {
        "host": "127.0.0.1",
        "port": 6000,
        "dbname": "ops",
        "user": "ops",
        "password": "secret",
        "connect_timeout": 5,
    }


def test_unreachable_database_raises_registry_unavailable_without_the_password():
    def refuse(**_):
        raise psycopg.OperationalError("connection refused")

    with pytest.raises(RegistryUnavailable) as error:
        PostgresRegistry(settings(), connect=refuse).init_schema()

    assert "127.0.0.1:5435/ops" in str(error.value)
    assert "secret" not in str(error.value)


def test_failed_schema_is_not_committed():
    connection = FakeConnection(fail_on_schema=True)

    with pytest.raises(RegistryUnavailable):
        PostgresRegistry(settings(), connect=connection).init_schema()

    assert not connection.committed


def test_registry_password_has_no_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OPS_DB_PASSWORD", raising=False)

    with pytest.raises(SystemExit) as error:
        load_settings(RegistrySettings)

    assert "OPS_DB_PASSWORD" in str(error.value)


def test_registry_settings_do_not_require_airflow_credentials(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AIRFLOW_USERNAME", raising=False)
    monkeypatch.delenv("AIRFLOW_PASSWORD", raising=False)
    monkeypatch.setenv("OPS_DB_PASSWORD", "secret")

    assert load_settings(RegistrySettings).ops_db_port == 5435


@pytest.mark.parametrize(
    ("table_exists", "expected"),
    [
        (False, "detection.incident criada em 127.0.0.1:5435/ops\n"),
        (True, "detection.incident já existia em 127.0.0.1:5435/ops; nada alterado\n"),
    ],
)
def test_init_command_reports_the_result(tmp_path, monkeypatch, capsys, table_exists, expected):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPS_DB_PASSWORD", "secret")
    monkeypatch.setattr(postgres.psycopg, "connect", FakeConnection(table_exists=table_exists))

    registry.main(["init"])

    assert capsys.readouterr().out == expected


def test_init_command_exits_with_the_error_when_the_database_is_down(tmp_path, monkeypatch):
    def refuse(**_):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPS_DB_PASSWORD", "secret")
    monkeypatch.setattr(postgres.psycopg, "connect", refuse)

    with pytest.raises(SystemExit) as error:
        registry.main(["init"])

    assert "registro indisponível em 127.0.0.1:5435/ops" in str(error.value)
