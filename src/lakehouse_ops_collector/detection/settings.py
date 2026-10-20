"""Configuração da detecção, lida de variáveis de ambiente ou do `.env`."""

from __future__ import annotations

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DetectionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    airflow_api_url: str = "http://127.0.0.1:8080"
    airflow_username: str
    airflow_password: SecretStr
    airflow_timeout_seconds: float = 10.0

    # Só loopback, como os serviços do lakehouse-infra (ADR 0011 do lakehouse-infra).
    detection_api_host: str = "127.0.0.1"
    detection_api_port: int = 8090
