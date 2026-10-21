"""Configuração da detecção, lida de variáveis de ambiente ou do `.env`."""

from __future__ import annotations

from typing import TypeVar

from pydantic import Field, SecretStr, ValidationError
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


class RegistrySettings(BaseSettings):
    """Banco do registro de incidentes: `ops` do `postgres-ops` (ADR 0001)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ops_db_host: str = "127.0.0.1"
    ops_db_port: int = 5435
    ops_db_name: str = "ops"
    ops_db_user: str = "ops"
    ops_db_password: SecretStr
    ops_db_connect_timeout_seconds: int = 5


class PollerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Pausa entre o fim de uma rodada e o início da seguinte.
    poller_interval_seconds: float = Field(default=30.0, gt=0)


SettingsT = TypeVar("SettingsT", bound=BaseSettings)


def load_settings(settings_class: type[SettingsT]) -> SettingsT:
    """Lê a configuração ou encerra com a lista das variáveis que faltam."""
    try:
        return settings_class()
    except ValidationError as error:
        names = sorted({str(item["loc"][0]).upper() for item in error.errors()})
        raise SystemExit(
            "configuração incompleta: " + ", ".join(names) + ".\n"
            "Crie o .env na pasta onde o comando é iniciado (cp .env.example .env) "
            "e preencha os valores, ou defina essas variáveis de ambiente."
        ) from None
