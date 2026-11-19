"""Application settings and data-quality configuration contracts.

All runtime configuration is externalised through environment variables with the
``OBSIFLOW_`` prefix.  The YAML rule catalogue is validated with the same Pydantic
models that the validation engine consumes, so a malformed rule file fails fast
at startup instead of silently disabling a data-quality gate.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES_PATH = PROJECT_ROOT / "config" / "dq_rules_config.yaml"


class AmountBounds(BaseModel):
    """Inclusive numeric bounds for the transaction amount column."""

    model_config = ConfigDict(extra="forbid")

    min: float = Field(ge=0)
    max: float = Field(gt=0)

    @model_validator(mode="after")
    def _check_order(self) -> AmountBounds:
        if self.min > self.max:
            raise ValueError("amount min must be less than or equal to max")
        return self


class DateBounds(BaseModel):
    """Inclusive calendar bounds for the transaction date column."""

    model_config = ConfigDict(extra="forbid")

    min: date
    max: date

    @model_validator(mode="after")
    def _check_order(self) -> DateBounds:
        if self.min > self.max:
            raise ValueError("date min must be less than or equal to max")
        return self


class BatchRules(BaseModel):
    """Batch-level row-count and rejection thresholds."""

    model_config = ConfigDict(extra="forbid")

    min_rows: int = Field(default=1, ge=0)
    max_rows: int = Field(default=100_000, gt=0)
    fail_on_row_count_violation: bool = True
    max_rejection_rate: float = Field(default=0.10, ge=0, le=1)
    duplicate_key: str = "transaction_id"
    duplicate_policy: Literal["keep_first", "keep_last"] = "keep_first"

    @model_validator(mode="after")
    def _check_row_bounds(self) -> BatchRules:
        if self.min_rows > self.max_rows:
            raise ValueError("min_rows must be less than or equal to max_rows")
        return self


class SchemaRules(BaseModel):
    """Required columns plus enum-style allowed values."""

    model_config = ConfigDict(extra="forbid")

    required_columns: list[str] = Field(min_length=1)
    allowed_values: dict[str, list[str]] = Field(default_factory=dict)


class FieldRules(BaseModel):
    """Field-level null, amount, and date validation rules."""

    model_config = ConfigDict(extra="forbid")

    mandatory: list[str] = Field(min_length=1)
    amount: AmountBounds
    date: DateBounds


class ReferentialRules(BaseModel):
    """Foreign-key lookup configuration for the reference warehouse table."""

    model_config = ConfigDict(extra="forbid")

    reference_table: str = "reference.accounts"
    foreign_key: str = "account_id"
    reference_key: str = "account_id"
    cache_ttl_seconds: int = Field(default=300, ge=0)


class QuarantineRules(BaseModel):
    """Quarantine persistence limits."""

    model_config = ConfigDict(extra="forbid")

    max_reason_length: int = Field(default=500, ge=64)
    table: str = "quality.error_records"


class DQConfig(BaseModel):
    """Validated representation of ``config/dq_rules_config.yaml``."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    version: int = Field(default=1, ge=1)
    batch: BatchRules
    schema_rules: SchemaRules = Field(alias="schema")
    fields: FieldRules
    referential: ReferentialRules = Field(default_factory=ReferentialRules)
    quarantine: QuarantineRules = Field(default_factory=QuarantineRules)


class Settings(BaseSettings):
    """Environment-driven application settings."""

    model_config = SettingsConfigDict(
        env_prefix="OBSIFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "obsiflow"
    environment: Literal["local", "development", "test", "production"] = "local"
    log_level: str = "INFO"

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5432, ge=1, le=65_535)
    postgres_db: str = "obsiflow"
    postgres_user: str = "obsiflow"
    postgres_password: SecretStr = SecretStr("obsiflow")
    postgres_connect_timeout_seconds: int = Field(default=5, ge=1)

    dq_rules_path: Path = DEFAULT_RULES_PATH
    data_dir: Path = PROJECT_ROOT / "data"
    staging_dir: Path = PROJECT_ROOT / "data" / "staging"
    sample_valid_file: Path = PROJECT_ROOT / "data" / "sample_valid.csv"
    sample_corrupted_file: Path = PROJECT_ROOT / "data" / "sample_corrupted.csv"

    pipeline_dag_id: str = "dq_etl_pipeline"
    pipeline_schedule: str = "0 2 * * *"
    pipeline_max_retries: int = Field(default=3, ge=0)
    pipeline_retry_delay_seconds: int = Field(default=120, ge=1)

    metrics_port: int = Field(default=9101, ge=1, le=65_535)
    metrics_refresh_interval_seconds: int = Field(default=15, ge=1)
    metrics_namespace: str = "pipeline"

    @property
    def database_dsn(self) -> str:
        """Return a libpq-compatible connection string."""

        password = self.postgres_password.get_secret_value()
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={password} connect_timeout={self.postgres_connect_timeout_seconds}"
        )

    @property
    def database_url(self) -> str:
        """Return a SQLAlchemy-style URL for tooling and documentation."""

        password = self.postgres_password.get_secret_value()
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    def ensure_runtime_directories(self) -> None:
        """Create writable runtime directories if they are missing."""

        self.staging_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""

    return Settings()


def load_dq_config(path: Path | str | None = None) -> DQConfig:
    """Load and validate the YAML rule catalogue.

    Args:
        path: Optional explicit rule path.  Defaults to ``OBSIFLOW_DQ_RULES_PATH``.

    Raises:
        FileNotFoundError: If the rule file does not exist.
        ValueError: If the YAML document is empty or invalid.
    """

    rules_path = Path(path) if path is not None else get_settings().dq_rules_path
    if not rules_path.exists():
        raise FileNotFoundError(f"DQ rules file not found: {rules_path}")

    raw = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"DQ rules file must contain a YAML mapping: {rules_path}")

    return DQConfig.model_validate(raw)
