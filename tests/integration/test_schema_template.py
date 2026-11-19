"""Reference integration test template.

This test does not depend on role stubs; it proves that CI bootstraps the
PostgreSQL contract from ``db/init.sql``.  Add new integration tests beside it.
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    "reference.accounts",
    "target.transactions",
    "quality.error_records",
    "observability.pipeline_runs",
}


def test_schema_contract_is_bootstrapped(db_connection: Any) -> None:
    """The four operational tables must exist before pipeline tests run."""

    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('reference', 'target', 'quality', 'observability')
            """
        )
        actual = {f"{schema}.{table}" for schema, table in cursor.fetchall()}

    assert actual >= EXPECTED_TABLES
