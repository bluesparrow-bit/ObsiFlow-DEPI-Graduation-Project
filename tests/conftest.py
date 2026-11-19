"""Shared pytest fixtures for unit, DagBag, and PostgreSQL integration tests."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TEMP_ROOT = Path(tempfile.gettempdir()) / "obsiflow-tests"
_TEMP_ROOT.mkdir(parents=True, exist_ok=True)

# Match local `docker compose` behaviour: values from `.env` are available to
# tests, while explicitly exported shell variables still take precedence.
load_dotenv(_PROJECT_ROOT / ".env", override=False)

# Environment must be configured before importing project settings/Airflow.
os.environ.setdefault("OBSIFLOW_ENVIRONMENT", "test")
os.environ.setdefault("OBSIFLOW_POSTGRES_HOST", "localhost")
os.environ.setdefault("OBSIFLOW_POSTGRES_PORT", "5432")
os.environ.setdefault("OBSIFLOW_POSTGRES_DB", "obsiflow")
os.environ.setdefault("OBSIFLOW_POSTGRES_USER", "obsiflow")
os.environ.setdefault("OBSIFLOW_POSTGRES_PASSWORD", "obsiflow")
os.environ.setdefault("OBSIFLOW_STAGING_DIR", str(_TEMP_ROOT / "staging"))
os.environ.setdefault("AIRFLOW_HOME", str(_TEMP_ROOT / "airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    f"sqlite:///{_TEMP_ROOT / 'airflow-metadata.db'}",
)

from config.settings import DQConfig, Settings, get_settings, load_dq_config  # noqa: E402
from etl.extract import extract_batch  # noqa: E402


@pytest.fixture(scope="session")
def project_root() -> Path:
    return _PROJECT_ROOT


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session")
def dq_config() -> DQConfig:
    return load_dq_config(_PROJECT_ROOT / "config" / "dq_rules_config.yaml")


@pytest.fixture()
def sample_valid_frame() -> pd.DataFrame:
    result = extract_batch(_PROJECT_ROOT / "data" / "sample_valid.csv")
    assert not result.fatal
    return result.frame


@pytest.fixture()
def sample_corrupted_frame() -> pd.DataFrame:
    result = extract_batch(_PROJECT_ROOT / "data" / "sample_corrupted.csv")
    assert not result.fatal
    return result.frame


@pytest.fixture()
def reference_ids() -> set[str]:
    return {f"ACC{number:06d}" for number in range(1, 31)}


@pytest.fixture(scope="session")
def db_connection(settings: Settings) -> Iterator[object]:
    """Open PostgreSQL and apply the idempotent schema.

    Local developers without Docker get a skip; CI sets ``OBSIFLOW_REQUIRE_DB=1``
    so an unavailable database is a hard failure instead of a false green build.
    """

    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - dependency is in requirements
        pytest.skip(f"psycopg2 is not installed: {exc}")

    try:
        connection = psycopg2.connect(settings.database_dsn)
    except psycopg2.OperationalError as exc:
        if os.getenv("OBSIFLOW_REQUIRE_DB") == "1":
            raise
        pytest.skip(f"PostgreSQL is not available: {exc}")

    ddl = (_PROJECT_ROOT / "db" / "init.sql").read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(ddl)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture()
def clean_db(db_connection: object) -> object:
    """Reset mutable tables while preserving reference data between tests."""

    with db_connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            "TRUNCATE quality.error_records, observability.pipeline_runs RESTART IDENTITY"
        )
        cursor.execute("DELETE FROM target.transactions")
    db_connection.commit()  # type: ignore[attr-defined]
    return db_connection


@pytest.fixture()
def db_reference_ids(clean_db: object) -> set[str]:
    with clean_db.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute("SELECT account_id FROM reference.accounts")
        return {str(row[0]).upper() for row in cursor.fetchall()}
