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
    path = Path(source_path)
    source_str = str(path)

    if not path.exists() or not path.is_file():
        return ExtractResult(
            source_file=source_str,
            error=f"File not found: {source_str}",
            error_kind="missing_file",
            fatal=True,
        )

    if path.stat().st_size == 0:
        return ExtractResult(
            source_file=source_str,
            is_empty=True,
            fatal=False,
        )

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return ExtractResult(
            source_file=source_str,
            is_empty=True,
            fatal=False,
        )
    except (UnicodeDecodeError, pd.errors.ParserError, OSError) as exc:
        return ExtractResult(
            source_file=source_str,
            error=str(exc),
            error_kind=type(exc).__name__,
            fatal=True,
        )

    columns = [str(col) for col in df.columns]
    row_count = int(len(df))

    if row_count == 0:
        return ExtractResult(
            source_file=source_str,
            frame=df,
            row_count=0,
            columns=columns,
            is_empty=True,
            fatal=False,
        )

    return ExtractResult(
        source_file=source_str,
        frame=df,
        row_count=row_count,
        columns=columns,
        is_empty=False,
        fatal=False,
    )
