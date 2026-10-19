"""Interfaces que o caso de uso de coleta usa; os adapters as implementam."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from ..package import IncidentPackage


class PackageStore(Protocol):
    def save(self, package: IncidentPackage) -> Path:
        """Grava o pacote e devolve onde ele ficou."""
        ...
