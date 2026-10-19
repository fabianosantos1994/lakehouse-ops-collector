"""Grava o pacote em `<raiz>/<dag_id>/<run_id>/<task_id>/package.json`."""

from __future__ import annotations

import os
from pathlib import Path

from ...package import IncidentKey, IncidentPackage


class JsonPackageStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def path_for(self, key: IncidentKey) -> Path:
        task_dir = key.task_id if key.map_index == -1 else f"{key.task_id}__{key.map_index}"
        return self.root / key.dag_id / key.run_id / task_dir / "package.json"

    def save(self, package: IncidentPackage) -> Path:
        path = self.path_for(package.incident)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Grava em arquivo temporário e troca, para nunca deixar um pacote pela metade.
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(package.model_dump_json(indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)
        return path
