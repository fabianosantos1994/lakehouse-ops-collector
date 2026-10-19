from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from lakehouse_ops_collector.package import IncidentKey, IncidentPackage

RI_001 = {
    "dag_id": "commercial_daily_sales_by_region",
    "run_id": "backfill__2026-10-15T00:00:00+00:00",
    "task_id": "validate_daily_sales_by_region",
}


def test_key_accepts_airflow_run_ids():
    key = IncidentKey(**RI_001)

    assert key.run_id == "backfill__2026-10-15T00:00:00+00:00"
    assert key.map_index == -1


@pytest.mark.parametrize("field", ["dag_id", "run_id", "task_id"])
@pytest.mark.parametrize("value", ["", " ", " padded", "a/b", "a\\b", ".", ".."])
def test_key_rejects_empty_or_path_unsafe_identifiers(field, value):
    with pytest.raises(ValidationError):
        IncidentKey(**{**RI_001, field: value})


def test_key_rejects_map_index_below_minus_one():
    with pytest.raises(ValidationError):
        IncidentKey(**RI_001, map_index=-2)


def test_key_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        IncidentKey(**RI_001, try_number=1)


def test_package_requires_timezone_aware_collected_at():
    with pytest.raises(ValidationError):
        IncidentPackage(collected_at=datetime(2026, 10, 19), incident=IncidentKey(**RI_001))


def test_package_round_trips_through_json():
    package = IncidentPackage(
        collected_at=datetime(2026, 10, 19, 12, 0, tzinfo=UTC),
        incident=IncidentKey(**RI_001),
    )

    restored = IncidentPackage.model_validate_json(package.model_dump_json())

    assert restored == package
    assert restored.package_version == 1
