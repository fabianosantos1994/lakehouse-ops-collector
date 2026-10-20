"""Registro de incidentes no PostgreSQL do `postgres-ops` (psycopg 3).

É o único lugar do repositório que grava, e só no banco do registro (ADR 0001).
"""

from __future__ import annotations

from collections.abc import Callable
from importlib.resources import files
from typing import Any

import psycopg

from ..ports import RegistryUnavailable
from ..settings import RegistrySettings

TABLE = "detection.incident"


def schema_sql() -> str:
    """DDL do registro, empacotado com o código (`detection/schema.sql`)."""
    return files("lakehouse_ops_collector.detection").joinpath("schema.sql").read_text(encoding="utf-8")


class PostgresRegistry:
    def __init__(self, settings: RegistrySettings, connect: Callable[..., Any] | None = None):
        self._settings = settings
        # Os testes trocam a conexão por uma simulada; o padrão é o psycopg.
        self._connect = connect or psycopg.connect

    @property
    def location(self) -> str:
        """Onde está o banco, sem a senha, para mensagens no terminal."""
        s = self._settings
        return f"{s.ops_db_host}:{s.ops_db_port}/{s.ops_db_name}"

    def _connection(self):
        s = self._settings
        try:
            return self._connect(
                host=s.ops_db_host,
                port=s.ops_db_port,
                dbname=s.ops_db_name,
                user=s.ops_db_user,
                password=s.ops_db_password.get_secret_value(),
                connect_timeout=s.ops_db_connect_timeout_seconds,
            )
        except psycopg.Error as error:
            raise RegistryUnavailable(f"registro indisponível em {self.location}: {error}") from error

    def init_schema(self) -> bool:
        """Cria o schema e a tabela que faltarem; devolve True se a tabela foi criada agora."""
        try:
            # A conexão confirma a transação ao sair do bloco sem erro.
            with self._connection() as connection:
                existed = connection.execute("SELECT to_regclass(%s) IS NOT NULL", (TABLE,)).fetchone()[0]
                connection.execute(schema_sql())
        except psycopg.Error as error:
            raise RegistryUnavailable(f"falha ao criar o schema em {self.location}: {error}") from error
        return not existed
