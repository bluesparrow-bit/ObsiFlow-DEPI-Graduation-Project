"""Configuration for the banking test-data generator.

Everything the validator and the QA tests must agree on (rule bounds,
defect mix, file names) lives here, and is echoed into manifest.json.
"""
from datetime import date

DEFAULT_SEED = 42
DEFAULT_ROWS = 1000
DEFAULT_ACCOUNTS = 500

# ---- Schema ---------------------------------------------------------------
REFERENCE_COLUMNS = ["account_id", "customer_name", "account_type", "branch", "open_date", "status"]
BATCH_COLUMNS = ["transaction_id", "account_id", "txn_type", "amount", "currency",
                "txn_date", "channel", "status"]
MANDATORY_FIELDS = ["account_id", "txn_type", "amount", "txn_date"]

TXN_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT", "FEE", "INTEREST"]
CURRENCIES = ["USD", "EUR", "GBP", "EGP"]
CHANNELS = ["ATM", "ONLINE", "BRANCH", "MOBILE", "POS"]
TXN_STATUSES = ["POSTED", "PENDING", "SETTLED"]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "BUSINESS", "CREDIT"]
BRANCHES = ["CAIRO-01", "ALEX-02", "GIZA-03", "TANTA-04", "ASWAN-05", "LONDON-06"]

# ---- Rule bounds (the validator's range rule must use these) ---------------
AMOUNT_MIN = 0.01
AMOUNT_MAX = 1_000_000.00
DATE_MIN = date(2020, 1, 1)
DATE_MAX = date(2026, 11, 5)          # fixed (not "today") so output is deterministic
VALID_DATE_START = date(2023, 1, 1)   # range used for *valid* generated rows
VALID_DATE_END = date(2026, 10, 31)
VALID_AMOUNT_CAP = 250_000.00

# ---- Defect mix for the main demo batch (1000 rows -> 50 invalid = 5%) ----
# One defect per bad row, so expected counts are exact.
BASE_ROWS = 1000
CLEAN_MIX = {"null": 12, "duplicate": 10, "schema_type": 10, "range": 10, "referential": 8}   # 50
HIGH_REJECTION_MIX = {k: v * 3 for k, v in CLEAN_MIX.items()}                                 # 150 = 15%

REJECTION_ALERT_THRESHOLD = 0.10

# ---- Output file names -----------------------------------------------------
FILES = {
    "reference": "reference/accounts.csv",
    "clean": "batches/transactions_clean_1000.csv",
    "high_rejection": "batches/transactions_high_rejection.csv",
    "schema_corrupt": "batches/transactions_schema_corrupt.csv",
    "schema_corrupt_fixed": "batches/transactions_schema_corrupt_fixed.csv",
    "empty": "batches/transactions_empty.csv",
    "unreadable": "batches/transactions_unreadable.csv",
    "manifest": "expected/manifest.json",
}
