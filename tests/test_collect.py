from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse_ops_collector.collection.__main__ import main
from lakehouse_ops_collector.collection.adapters.json_store import JsonPackageStore
from lakehouse_ops_collector.collection.collect import collect_incident
from lakehouse_ops_collector.package import IncidentKey, IncidentPackage

RI_003 = IncidentKey(
    dag_id="commercial_daily_sales_by_region",
    run_id="backfill__2026-10-17T00:00:00+00:00",
    task_id="build_daily_sales_by_region",
)
FIXED_NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


class MemoryStore:
    def __init__(self) -> None:
        self.saved: list[IncidentPackage] = []

    def save(self, package: IncidentPackage) -> Path:
        self.saved.append(package)
        return Path("memory")


def test_collect_incident_builds_and_saves_the_package():
    store = MemoryStore()

    package, path = collect_incident(RI_003, store, now=lambda: FIXED_NOW)

    assert store.saved == [package]
    assert package.incident == RI_003
    assert package.collected_at == FIXED_NOW
    assert path == Path("memory")


def test_json_store_writes_to_the_incident_path(tmp_path):
    store = JsonPackageStore(tmp_path)

    _, path = collect_incident(RI_003, store, now=lambda: FIXED_NOW)

    assert path == (
        tmp_path
        / "commercial_daily_sales_by_region"
        / "backfill__2026-10-17T00:00:00+00:00"
        / "build_daily_sales_by_region"
        / "package.json"
    )
    content = json.loads(path.read_text(encoding="utf-8"))
    assert content["package_version"] == 1
    assert content["incident"]["task_id"] == "build_daily_sales_by_region"
    assert not path.with_suffix(".json.tmp").exists()


def test_json_store_separates_mapped_task_instances(tmp_path):
    key = RI_003.model_copy(update={"map_index": 2})

    path = JsonPackageStore(tmp_path).path_for(key)

    assert path.parent.name == "build_daily_sales_by_region__2"


def test_cli_writes_the_package_and_prints_its_path(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("INCIDENTS_DIR", str(tmp_path))

    code = main([
        "collect",
        "--dag-id", RI_003.dag_id,
        "--run-id", RI_003.run_id,
        "--task-id", RI_003.task_id,
    ])

    printed = Path(capsys.readouterr().out.strip())
    assert code == 0
    assert printed.is_file()
    assert IncidentPackage.model_validate_json(printed.read_text(encoding="utf-8")).incident == RI_003


def test_cli_rejects_an_empty_run_id_without_writing(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("INCIDENTS_DIR", str(tmp_path))

    code = main(["collect", "--dag-id", RI_003.dag_id, "--run-id", "", "--task-id", RI_003.task_id])

    assert code == 2
    assert "--run-id" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("missing", ["--dag-id", "--run-id", "--task-id"])
def test_cli_requires_every_key_argument(missing):
    args = {"--dag-id": "d", "--run-id": "r", "--task-id": "t"}
    argv = ["collect"]
    for name, value in args.items():
        if name != missing:
            argv += [name, value]

    with pytest.raises(SystemExit) as error:
        main(argv)

    assert error.value.code == 2
