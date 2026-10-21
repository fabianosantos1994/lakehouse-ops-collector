"""Sobe a API de detecção: `python -m lakehouse_ops_collector.detection`."""

from __future__ import annotations

import uvicorn

from .adapters.airflow_api import AirflowApiSource
from .api import create_app
from .settings import DetectionSettings, load_settings


def main() -> None:
    settings = load_settings(DetectionSettings)
    source = AirflowApiSource.from_settings(settings)
    uvicorn.run(create_app(source), host=settings.detection_api_host, port=settings.detection_api_port)


if __name__ == "__main__":
    main()
