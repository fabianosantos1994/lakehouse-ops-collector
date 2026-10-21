"""Registro de incidentes no PostgreSQL do `postgres-ops` (psycopg 3).

É o único lugar do repositório que grava, e só no banco do registro (ADR 0001).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from importlib.resources import files
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from ...incident import IncidentKey
from ..models import DetectedIncident
from ..ports import RegistryUnavailable, SaveCounts
from ..settings import RegistrySettings

TABLE = "detection.incident"

# Insere a chave nova ou altera a existente só se o status ou as tentativas
# mudaram. `created_at_*` só é gravado na inserção. `xmax = 0` identifica a
# linha inserida; a alteração ignorada pelo WHERE não devolve linha.
_UPSERT = """
INSERT INTO detection.incident AS i
    (dag_id, run_id, task_id, map_index, status, attempts, last_end_date, created_at_ds, updated_at_ds)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dag_id, run_id, task_id, map_index) DO UPDATE
SET status = EXCLUDED.status,
    attempts = EXCLUDED.attempts,
    last_end_date = EXCLUDED.last_end_date,
    updated_at_dt = now(),
    updated_at_ds = EXCLUDED.updated_at_ds
WHERE (i.status, i.attempts) IS DISTINCT FROM (EXCLUDED.status, EXCLUDED.attempts)
RETURNING (xmax = 0) AS inserted
"""


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

    @contextmanager
    def _transaction(self) -> Iterator[Any]:
        """Uma conexão por operação; confirma ao sair do bloco sem erro."""
        s = self._settings
        try:
            connection = self._connect(
                host=s.ops_db_host,
                port=s.ops_db_port,
                dbname=s.ops_db_name,
                user=s.ops_db_user,
                password=s.ops_db_password.get_secret_value(),
                connect_timeout=s.ops_db_connect_timeout_seconds,
            )
        except psycopg.Error as error:
            raise RegistryUnavailable(f"registro indisponível em {self.location}: {error}") from error
        try:
            with connection:
                yield connection
        except psycopg.errors.UndefinedTable as error:
            raise RegistryUnavailable(
                f"{TABLE} não existe em {self.location}; rode antes "
                "`python -m lakehouse_ops_collector.detection.registry init`"
            ) from error
        except psycopg.Error as error:
            raise RegistryUnavailable(f"falha no registro em {self.location}: {error}") from error

    def init_schema(self) -> bool:
        """Cria o schema e a tabela que faltarem; devolve True se a tabela foi criada agora."""
        with self._transaction() as connection:
            existed = connection.execute("SELECT to_regclass(%s) IS NOT NULL", (TABLE,)).fetchone()[0]
            connection.execute(schema_sql())
        return not existed

    def save_all(self, incidents: Sequence[DetectedIncident], source: str) -> SaveCounts:
        new = updated = 0
        with self._transaction() as connection:
            for incident in incidents:
                key = incident.key
                attempts = [attempt.model_dump(mode="json") for attempt in incident.attempts]
                last_end_date = incident.attempts[-1].end_date if incident.attempts else None
                row = connection.execute(
                    _UPSERT,
                    (
                        key.dag_id,
                        key.run_id,
                        key.task_id,
                        key.map_index,
                        incident.status,
                        Jsonb(attempts),
                        last_end_date,
                        source,
                        source,
                    ),
                ).fetchone()
                if row is None:
                    continue
                if row[0]:
                    new += 1
                else:
                    updated += 1
        return SaveCounts(new=new, updated=updated)

    def latest_end_date(self) -> datetime | None:
        with self._transaction() as connection:
            return connection.execute(f"SELECT max(last_end_date) FROM {TABLE}").fetchone()[0]

    def open_count(self) -> int:
        with self._transaction() as connection:
            return connection.execute(
                f"SELECT count(*) FROM {TABLE} WHERE status <> 'recovered'"
            ).fetchone()[0]

    def list_open(self) -> list[IncidentKey]:
        with self._transaction() as connection:
            rows = connection.execute(
                f"SELECT dag_id, run_id, task_id, map_index FROM {TABLE} "
                "WHERE status <> 'recovered' ORDER BY last_end_date DESC NULLS FIRST"
            ).fetchall()
        return [IncidentKey(dag_id=d, run_id=r, task_id=t, map_index=m) for d, r, t, m in rows]
