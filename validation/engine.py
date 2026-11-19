"""Orchestration interface for the six data-quality rule families.

[DQ] Implement ``validate_batch`` as a pure function: it takes a dataframe, a
validated rule configuration, and a collection of reference keys, then returns a
deterministic valid/invalid split.  Database access belongs in callers so every
rule stays unit-testable.
"""

from __future__ import annotations

from collections.abc import Collection

import pandas as pd

from config.settings import DQConfig
from etl.contracts import BatchValidationResult


def validate_batch(
    frame: pd.DataFrame,
    config: DQConfig,
    reference_ids: Collection[str],
    source_file: str = "<memory>",
) -> BatchValidationResult:
    """Evaluate the six rule families and split valid from invalid rows.

    Policy summary:
    * Missing required columns and out-of-bounds row counts are blocking.
    * A row with any violation is quarantined; all other rows remain loadable.
    * The rejection-rate threshold is alert-only, not a load gate.
    """

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
