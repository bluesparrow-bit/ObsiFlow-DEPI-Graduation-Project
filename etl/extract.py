"""Source extraction interface.

[DE] Implement CSV extraction with graceful handling of missing, empty, and
corrupt files.  A fatal result must be reported as data (``fatal=True``) so the
orchestrator can persist a clean ``FAILED`` run before applying its
permanent-failure policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ExtractResult:
    """Normalized extraction outcome.

    A fatal result is reported as data, not raised, so the orchestrator can log
    a clean ``FAILED`` run before applying its permanent-failure policy.
    """

    source_file: str
    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    is_empty: bool = True
    error: str | None = None
    error_kind: str | None = None
    fatal: bool = False


def extract_batch(source_path: str | Path) -> ExtractResult:
    """Read a CSV batch as raw strings.

    Args:
        source_path: CSV file to ingest.

    Returns:
        An :class:`ExtractResult`.  Missing files, unreadable bytes, and parser
        failures return ``fatal=True`` with a diagnostic; zero-row and header-only
        files return ``is_empty=True`` with no exception.
    """

    raise NotImplementedError("TODO [DE]: Implement this function")
