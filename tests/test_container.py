from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_image_pins_the_python_base_and_runs_as_a_non_root_user():
    dockerfile = (ROOT / "Dockerfile").read_text()

    # A mesma base do executor do Spark no lakehouse-infra.
    assert "FROM python:3.12.11-slim-bullseye\n" in dockerfile
    assert "USER ops\n" in dockerfile
    assert 'CMD ["python", "-m", "lakehouse_ops_collector.detection.poller"]' in dockerfile


def test_credentials_and_local_files_stay_out_of_the_image():
    ignored = (ROOT / ".dockerignore").read_text().splitlines()

    for entry in (".env", ".venv/", "incidents/", "tests/", ".git/"):
        assert entry in ignored
