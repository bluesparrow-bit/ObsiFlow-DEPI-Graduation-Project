"""Source extraction with graceful handling of missing, empty, and corrupt files."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


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


def _empty_result(
    source: Path, message: str | None = None, kind: str | None = None
) -> ExtractResult:
    return ExtractResult(
        source_file=str(source),
        frame=pd.DataFrame(),
        row_count=0,
        columns=[],
        is_empty=True,
        error=message,
        error_kind=kind,
        fatal=False,
    )


def extract_batch(source_path: str | Path) -> ExtractResult:
    """Read a CSV batch as raw strings.

    Args:
        source_path: CSV file to ingest.

    Returns:
        An :class:`ExtractResult`.  Missing files, unreadable bytes, and parser
        failures return ``fatal=True`` with a diagnostic; zero-row and header-only
        files return ``is_empty=True`` with no exception.
    """

    source = Path(source_path)
    if not source.exists():
        message = f"Source file does not exist: {source}"
        logger.error(message)
        return ExtractResult(
            source_file=str(source),
            error=message,
            error_kind="missing_file",
            fatal=True,
        )
    if source.is_dir():
        message = f"Source path is a directory, not a CSV file: {source}"
        logger.error(message)
        return ExtractResult(
            source_file=str(source),
            error=message,
            error_kind="not_a_file",
            fatal=True,
        )

    try:
        if source.stat().st_size == 0:
            logger.warning("Source file is empty (zero bytes): %s", source)
            return _empty_result(source, message="Source file is zero bytes", kind="empty_file")

        frame = pd.read_csv(
            source,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig",
            on_bad_lines="error",
        )
    except pd.errors.EmptyDataError:
        logger.warning("Source file has no header or data: %s", source)
        return _empty_result(source, message="Source file has no header or data", kind="empty_file")
    except UnicodeDecodeError as exc:
        message = f"Source file is not valid UTF-8: {source}"
        logger.exception(message)
        return ExtractResult(
            source_file=str(source),
            error=f"{message}: {exc}",
            error_kind="decode_error",
            fatal=True,
        )
    except pd.errors.ParserError as exc:
        message = f"Source CSV could not be parsed: {source}"
        logger.exception(message)
        return ExtractResult(
            source_file=str(source),
            error=f"{message}: {exc}",
            error_kind="parse_error",
            fatal=True,
        )
    except OSError as exc:
        message = f"Source file could not be read: {source}"
        logger.exception(message)
        return ExtractResult(
            source_file=str(source),
            error=f"{message}: {exc}",
            error_kind="io_error",
            fatal=True,
        )

    frame = frame.reset_index(drop=True)
    columns = [str(column) for column in frame.columns]
    row_count = len(frame)
    if row_count == 0:
        logger.warning("Source file contains a header but zero data rows: %s", source)
        return ExtractResult(
            source_file=str(source),
            frame=frame,
            row_count=0,
            columns=columns,
            is_empty=True,
            error="Source file contains zero data rows",
            error_kind="zero_rows",
            fatal=False,
        )

    logger.info("Extracted %d rows and %d columns from %s", row_count, len(columns), source)
    return ExtractResult(
        source_file=str(source),
        frame=frame,
        row_count=row_count,
        columns=columns,
        is_empty=False,
    )
