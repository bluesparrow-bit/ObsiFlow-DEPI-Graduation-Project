-- ObsiFlow PostgreSQL bootstrap schema.
-- Idempotent: safe to run against a fresh or existing database.
-- The four operational objects are reference.accounts, target.transactions,
-- quality.error_records, and observability.pipeline_runs.

CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS target;
CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS observability;

-- ---------------------------------------------------------------------------
-- Reference data (foreign-key authority)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.accounts (
    account_id     VARCHAR(32) PRIMARY KEY,
    customer_name  TEXT        NOT NULL,
    account_type   TEXT        NOT NULL,
    branch         TEXT        NOT NULL,
    open_date      DATE        NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'ACTIVE'
);

COMMENT ON TABLE reference.accounts IS 'Authoritative account master for referential checks.';

INSERT INTO reference.accounts
    (account_id, customer_name, account_type, branch, open_date, status)
VALUES
    ('ACC000001', 'Mona Hassan',      'BUSINESS', 'ALEX-02',  '2017-07-03', 'ACTIVE'),
    ('ACC000002', 'Mona Samir',       'CHECKING', 'ASWAN-05', '2019-09-25', 'ACTIVE'),
    ('ACC000003', 'Omar Farouk',      'SAVINGS',  'ASWAN-05', '2021-10-01', 'ACTIVE'),
    ('ACC000004', 'Karim Samir',      'CREDIT',   'ALEX-02',  '2020-01-14', 'ACTIVE'),
    ('ACC000005', 'Sara Ibrahim',     'CHECKING', 'CAIRO-01', '2018-03-11', 'ACTIVE'),
    ('ACC000006', 'Nour Adel',        'SAVINGS',  'GIZA-03',  '2022-01-20', 'ACTIVE'),
    ('ACC000007', 'Ali Nasser',       'BUSINESS', 'TANTA-04', '2016-06-30', 'ACTIVE'),
    ('ACC000008', 'Laila Fathy',      'CREDIT',   'CAIRO-01', '2021-05-17', 'ACTIVE'),
    ('ACC000009', 'Youssef Gamal',    'CHECKING', 'LONDON-06','2019-12-02', 'ACTIVE'),
    ('ACC000010', 'Dina Mahmoud',     'SAVINGS',  'ALEX-02',  '2020-08-08', 'ACTIVE'),
    ('ACC000011', 'Hassan Saleh',     'BUSINESS', 'GIZA-03',  '2015-11-23', 'ACTIVE'),
    ('ACC000012', 'Rana Mostafa',     'CHECKING', 'CAIRO-01', '2022-04-14', 'ACTIVE'),
    ('ACC000013', 'Tarek Farouk',     'CREDIT',   'TANTA-04', '2017-02-19', 'ACTIVE'),
    ('ACC000014', 'Heba Samir',       'SAVINGS',  'ASWAN-05', '2021-09-05', 'ACTIVE'),
    ('ACC000015', 'Mostafa Adel',     'CHECKING', 'ALEX-02',  '2018-07-27', 'ACTIVE'),
    ('ACC000016', 'Salma Nasser',     'BUSINESS', 'LONDON-06','2019-10-09', 'ACTIVE'),
    ('ACC000017', 'Omar Ibrahim',     'SAVINGS',  'GIZA-03',  '2020-12-31', 'ACTIVE'),
    ('ACC000018', 'Mona Farouk',      'CREDIT',   'CAIRO-01', '2016-03-18', 'ACTIVE'),
    ('ACC000019', 'Karim Hassan',     'CHECKING', 'TANTA-04', '2022-06-06', 'ACTIVE'),
    ('ACC000020', 'Nour Samir',       'SAVINGS',  'ASWAN-05', '2021-01-12', 'ACTIVE'),
    ('ACC000021', 'Ali Mahmoud',      'BUSINESS', 'ALEX-02',  '2015-08-21', 'ACTIVE'),
    ('ACC000022', 'Laila Saleh',      'CHECKING', 'GIZA-03',  '2019-04-03', 'ACTIVE'),
    ('ACC000023', 'Youssef Fathy',    'CREDIT',   'LONDON-06','2018-09-15', 'ACTIVE'),
    ('ACC000024', 'Dina Gamal',       'SAVINGS',  'CAIRO-01', '2020-02-28', 'ACTIVE'),
    ('ACC000025', 'Hassan Nasser',    'BUSINESS', 'TANTA-04', '2016-12-07', 'ACTIVE'),
    ('ACC000026', 'Rana Ibrahim',     'CHECKING', 'ASWAN-05', '2021-11-19', 'ACTIVE'),
    ('ACC000027', 'Tarek Adel',       'CREDIT',   'ALEX-02',  '2017-05-24', 'ACTIVE'),
    ('ACC000028', 'Heba Mostafa',     'SAVINGS',  'GIZA-03',  '2019-07-01', 'ACTIVE'),
    ('ACC000029', 'Mostafa Hassan',   'CHECKING', 'CAIRO-01', '2022-08-10', 'ACTIVE'),
    ('ACC000030', 'Salma Farouk',     'BUSINESS', 'LONDON-06','2018-01-26', 'ACTIVE')
ON CONFLICT (account_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Target warehouse
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS target.transactions (
    transaction_id  VARCHAR(64) PRIMARY KEY,
    account_id      VARCHAR(32) NOT NULL REFERENCES reference.accounts (account_id)
                        ON UPDATE CASCADE,
    txn_type        VARCHAR(32) NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    currency        CHAR(3)     NOT NULL,
    txn_date        DATE        NOT NULL,
    channel         VARCHAR(32) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    source_file     TEXT        NOT NULL,
    load_run_id     TEXT        NOT NULL,
    first_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_target_transactions_account
    ON target.transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_target_transactions_load_run
    ON target.transactions (load_run_id);
CREATE INDEX IF NOT EXISTS idx_target_transactions_txn_date
    ON target.transactions (txn_date);

COMMENT ON TABLE target.transactions IS 'Curated transactions; idempotent ON CONFLICT upsert target.';

CREATE OR REPLACE FUNCTION target.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_target_transactions_updated_at ON target.transactions;
CREATE TRIGGER trg_target_transactions_updated_at
    BEFORE UPDATE ON target.transactions
    FOR EACH ROW
    EXECUTE FUNCTION target.touch_updated_at();

-- ---------------------------------------------------------------------------
-- Quarantine / dead-letter store
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality.error_records (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT        NOT NULL,
    source_file     TEXT        NOT NULL,
    row_number      INTEGER     NOT NULL CHECK (row_number > 0),
    transaction_id  TEXT,
    rule_families   TEXT[]      NOT NULL,
    failure_reasons JSONB       NOT NULL,
    raw_record      JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_error_records_run_row UNIQUE (run_id, row_number),
    CONSTRAINT ck_error_records_families CHECK (
        rule_families <@ ARRAY[
            'null', 'duplicate', 'schema', 'row_count', 'range', 'referential'
        ]::TEXT[]
    )
);

CREATE INDEX IF NOT EXISTS idx_error_records_run
    ON quality.error_records (run_id);
CREATE INDEX IF NOT EXISTS idx_error_records_created
    ON quality.error_records (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_error_records_families
    ON quality.error_records USING GIN (rule_families);

COMMENT ON TABLE quality.error_records IS 'Dead-letter store: one row per rejected source row.';

-- ---------------------------------------------------------------------------
-- Run observability / audit log
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id           TEXT PRIMARY KEY,
    dag_id           TEXT        NOT NULL,
    batch_id         TEXT,
    source_file      TEXT        NOT NULL,
    status           VARCHAR(16) NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    records_extracted INTEGER NOT NULL DEFAULT 0 CHECK (records_extracted >= 0),
    records_valid     INTEGER NOT NULL DEFAULT 0 CHECK (records_valid >= 0),
    records_loaded    INTEGER NOT NULL DEFAULT 0 CHECK (records_loaded >= 0),
    records_rejected  INTEGER NOT NULL DEFAULT 0 CHECK (records_rejected >= 0),
    rejection_rate    NUMERIC(7, 6) NOT NULL DEFAULT 0
        CHECK (rejection_rate >= 0 AND rejection_rate <= 1),
    failures_by_rule  JSONB NOT NULL DEFAULT '{}'::JSONB,
    error_summary     TEXT,
    CONSTRAINT ck_pipeline_runs_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_started
    ON observability.pipeline_runs (dag_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON observability.pipeline_runs (status);

COMMENT ON TABLE observability.pipeline_runs IS 'Audit log and single source of truth for telemetry.';

CREATE OR REPLACE VIEW observability.v_latest_runs AS
SELECT DISTINCT ON (dag_id)
    dag_id,
    run_id,
    status,
    source_file,
    started_at,
    finished_at,
    duration_seconds,
    records_extracted,
    records_loaded,
    records_rejected,
    rejection_rate,
    failures_by_rule,
    error_summary
FROM observability.pipeline_runs
ORDER BY dag_id, started_at DESC;

COMMENT ON VIEW observability.v_latest_runs IS 'Most recent run per DAG for dashboards and runbooks.';
