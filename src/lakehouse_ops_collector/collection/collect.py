"""Caso de uso: montar o pacote de um incidente."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from ..package import IncidentKey, IncidentPackage, utc_now
from .ports import PackageStore


def collect_incident(
    key: IncidentKey,
    store: PackageStore,
    now: Callable[[], datetime] = utc_now,
) -> tuple[IncidentPackage, Path]:
    package = IncidentPackage(collected_at=now(), incident=key)
    return package, store.save(package)
