"""CLI do coletor: `python -m lakehouse_ops_collector.collection collect ...`."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pydantic import ValidationError

from ..package import IncidentKey
from .adapters.json_store import JsonPackageStore
from .collect import collect_incident

DEFAULT_INCIDENTS_DIR = "incidents"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m lakehouse_ops_collector.collection")
    commands = parser.add_subparsers(dest="command", required=True)

    collect = commands.add_parser("collect", help="monta e grava o pacote de um incidente")
    collect.add_argument("--dag-id", required=True)
    collect.add_argument("--run-id", required=True)
    collect.add_argument("--task-id", required=True)
    collect.add_argument("--map-index", type=int, default=-1)
    return parser


def _format_errors(error: ValidationError) -> str:
    lines = []
    for item in error.errors():
        field = ".".join(str(part) for part in item["loc"]) or "chave"
        lines.append(f"  --{field.replace('_', '-')}: {item['msg']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        key = IncidentKey(
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_id=args.task_id,
            map_index=args.map_index,
        )
    except ValidationError as error:
        print(f"chave do incidente inválida:\n{_format_errors(error)}", file=sys.stderr)
        return 2

    store = JsonPackageStore(Path(os.environ.get("INCIDENTS_DIR", DEFAULT_INCIDENTS_DIR)))
    _, path = collect_incident(key, store)
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
