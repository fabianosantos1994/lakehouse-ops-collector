"""Comandos do registro de incidentes.

`python -m lakehouse_ops_collector.detection.registry init` cria o schema
`detection` e a tabela `detection.incident` no banco do `postgres-ops`.
"""

from __future__ import annotations

import argparse

from .adapters.postgres import TABLE, PostgresRegistry
from .ports import RegistryUnavailable
from .settings import RegistrySettings, load_settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m lakehouse_ops_collector.detection.registry")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="cria o schema do registro; rodar de novo não altera nada")
    parser.parse_args(argv)

    registry = PostgresRegistry(load_settings(RegistrySettings))
    try:
        created = registry.init_schema()
    except RegistryUnavailable as error:
        raise SystemExit(str(error)) from None

    if created:
        print(f"{TABLE} criada em {registry.location}")
    else:
        print(f"{TABLE} já existia em {registry.location}; nada alterado")


if __name__ == "__main__":
    main()
