"""Unit tests for application settings and DQ configuration contracts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from config.settings import (
    AmountBounds,
    BatchRules,
    DateBounds,
    DQConfig,
    QuarantineRules,
    ReferentialRules,
    SchemaRules,
    Settings,
    load_dq_config,
)


@pytest.mark.unit
def test_amount_bounds_validation() -> None:
    valid = AmountBounds(min=0.01, max=100.0)
    assert valid.min == 0.01
    assert valid.max == 100.0

    with pytest.raises(ValueError, match="amount min must be less than or equal to max"):
        AmountBounds(min=500.0, max=100.0)

    with pytest.raises(ValidationError):
        AmountBounds(min=-1.0, max=100.0)


@pytest.mark.unit
def test_date_bounds_validation() -> None:
    valid = DateBounds(min=date(2020, 1, 1), max=date(2025, 12, 31))
    assert valid.min < valid.max

    with pytest.raises(ValueError, match="date min must be less than or equal to max"):
        DateBounds(min=date(2025, 1, 1), max=date(2020, 1, 1))


@pytest.mark.unit
def test_batch_rules_validation() -> None:
    valid = BatchRules(min_rows=10, max_rows=1000)
    assert valid.min_rows == 10
    assert valid.max_rows == 1000
    assert valid.duplicate_policy == "keep_first"

    with pytest.raises(ValueError, match="min_rows must be less than or equal to max_rows"):
        BatchRules(min_rows=500, max_rows=100)

    with pytest.raises(ValidationError):
        BatchRules(max_rejection_rate=1.5)


@pytest.mark.unit
def test_schema_and_quarantine_rules() -> None:
    schema = SchemaRules(required_columns=["id", "val"], allowed_values={"val": ["A", "B"]})
    assert len(schema.required_columns) == 2

    with pytest.raises(ValidationError):
        SchemaRules(required_columns=[])

    quarantine = QuarantineRules(max_reason_length=200, table="quality.errors")
    assert quarantine.max_reason_length == 200

    ref = ReferentialRules(reference_table="reference.accounts", foreign_key="acc_id")
    assert ref.foreign_key == "acc_id"


@pytest.mark.unit
def test_settings_properties_and_runtime_dirs(tmp_path: Path) -> None:
    staging = tmp_path / "staging_test"
    settings = Settings(
        postgres_host="pg-host",
        postgres_port=5433,
        postgres_db="testdb",
        postgres_user="testuser",
        postgres_password="secretpassword",
        staging_dir=staging,
    )

    assert "host=pg-host" in settings.database_dsn
    assert "port=5433" in settings.database_dsn
    assert "dbname=testdb" in settings.database_dsn
    assert "secretpassword" in settings.database_dsn
    assert (
        settings.database_url == "postgresql+psycopg2://testuser:secretpassword@pg-host:5433/testdb"
    )

    assert not staging.exists()
    settings.ensure_runtime_directories()
    assert staging.exists()


@pytest.mark.unit
def test_load_dq_config_success(dq_config: DQConfig) -> None:
    assert dq_config.version >= 1
    assert dq_config.batch.min_rows >= 0
    assert len(dq_config.schema_rules.required_columns) > 0
    assert dq_config.fields.amount.min >= 0


@pytest.mark.unit
def test_load_dq_config_errors(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError, match="DQ rules file not found"):
        load_dq_config(missing)

    invalid_yaml = tmp_path / "not_dict.yaml"
    invalid_yaml.write_text("- item1\n- item2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must contain a YAML mapping"):
        load_dq_config(invalid_yaml)
