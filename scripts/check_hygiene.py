#!/usr/bin/env python3
"""Single source of truth for ObsiFlow repository hygiene and secret audits.

Enforces:
1. No sensitive secrets (.env, tokens, credentials) are committed.
2. No temporary binary database dumps, logs, or SQLite databases are committed.
3. No generated artifacts (raw_counters_report.json, generator outputs, batch CSVs) are committed.
4. All tracked files use POSIX path separators (no backslashes).
5. No leftover git merge conflict markers exist in source/config files.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_EXACT_NAMES: frozenset[str] = frozenset(
    {
        ".env",
        "raw_counters_report.json",
        "standalone_admin_password.txt",
    }
)

FORBIDDEN_SUFFIXES: tuple[str, ...] = (
    ".log",
    ".sqlite",
    ".sqlite3",
    ".dump",
    ".sql.gz",
)

FORBIDDEN_DIR_PREFIXES: tuple[str, ...] = (
    "data/staging/",
    "data/batches/",
    "data/genration_script/output/",
)


def get_tracked_files(root: Path = PROJECT_ROOT) -> list[str]:
    """Retrieve list of files currently tracked by git."""
    proc = subprocess.run(
        ["git", "ls-files"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def find_forbidden_files(tracked_files: list[str]) -> list[str]:
    """Identify any tracked files that violate repository hygiene rules."""
    violations: list[str] = []
    for f in tracked_files:
        path = Path(f)
        name = path.name

        # Exact forbidden names
        if name in FORBIDDEN_EXACT_NAMES:
            violations.append(f)
            continue

        # Forbidden extensions
        if name.endswith(FORBIDDEN_SUFFIXES):
            violations.append(f)
            continue

        # Environment files (except .env.example)
        if name.startswith(".env.") and name != ".env.example":
            violations.append(f)
            continue

        # Forbidden directories / generated batches
        posix_str = path.as_posix()
        if any(posix_str.startswith(prefix) for prefix in FORBIDDEN_DIR_PREFIXES):
            violations.append(f)
            continue

    return sorted(violations)


def check_posix_paths(tracked_files: list[str]) -> list[str]:
    """Flag non-POSIX paths containing backslashes."""
    return sorted([f for f in tracked_files if "\\" in f])


def find_conflict_markers(root: Path = PROJECT_ROOT) -> list[str]:
    """Search for leftover git merge conflict markers in tracked text files."""
    conflict_start = re.compile(r"^<{7}\s+[^\r\n]+")
    conflict_end = re.compile(r"^>{7}\s+[^\r\n]+")

    tracked = get_tracked_files(root)
    violations: list[str] = []

    for rel_path in tracked:
        file_path = root / rel_path
        if file_path.suffix in {".pdf", ".png", ".jpg", ".docx", ".gz"}:
            continue
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        lines = content.splitlines()
        has_start = any(conflict_start.search(line) for line in lines)
        has_end = any(conflict_end.search(line) for line in lines)

        if has_start and has_end:
            violations.append(rel_path)

    return sorted(violations)


def run_all_hygiene_checks(root: Path = PROJECT_ROOT) -> int:
    """Run full hygiene verification and print human-readable diagnostics."""
    print("Auditing Git repository hygiene and sensitive files...")
    tracked = get_tracked_files(root)

    forbidden = find_forbidden_files(tracked)
    if forbidden:
        print("ERROR: Forbidden or sensitive files tracked in git:", file=sys.stderr)
        for f in forbidden:
            print(f"  - {f}", file=sys.stderr)
        return 1

    non_posix = check_posix_paths(tracked)
    if non_posix:
        print("ERROR: Non-POSIX path with backslash found in git tree:", file=sys.stderr)
        for f in non_posix:
            print(f"  - {f}", file=sys.stderr)
        return 1

    conflicts = find_conflict_markers(root)
    if conflicts:
        print("ERROR: Unresolved git merge conflict markers detected:", file=sys.stderr)
        for f in conflicts:
            print(f"  - {f}", file=sys.stderr)
        return 1

    print(f"Repository hygiene verified: {len(tracked)} tracked files checked, 0 violations.")
    return 0


if __name__ == "__main__":
    sys.exit(run_all_hygiene_checks())
