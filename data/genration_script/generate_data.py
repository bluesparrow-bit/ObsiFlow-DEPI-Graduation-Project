#!/usr/bin/env python3
"""Generate banking test data for the Data Quality & Pipeline Observability platform.

Usage:
    python generate_data.py --out ./output --rows 1000 --seed 42

Standard library only. Same seed => byte-identical output.
"""
import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

import config as C

FAMILY_LABEL = {
    "null": "null_checks",
    "duplicate": "duplicate_checks",
    "schema_type": "schema_type_checks",
    "range": "row_count_range",
    "referential": "referential_integrity",
}


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------
def acct_id(n):
    return f"ACC{n:06d}"


def rand_date(rng, start, end):
    return start + timedelta(days=rng.randint(0, (end - start).days))


def make_accounts(rng, n):
    first = ["Ahmed", "Sara", "Omar", "Mona", "Youssef", "Laila", "Karim", "Nour", "Hassan", "Dina",
            "Ali", "Salma", "Tarek", "Heba", "Mostafa", "Rana"]
    last = ["Hassan", "Ibrahim", "Mahmoud", "Farouk", "Saleh", "Nasser", "Adel", "Gamal", "Samir", "Fathy"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "account_id": acct_id(i),
            "customer_name": f"{rng.choice(first)} {rng.choice(last)}",
            "account_type": rng.choice(C.ACCOUNT_TYPES),
            "branch": rng.choice(C.BRANCHES),
            "open_date": rand_date(rng, date(2015, 1, 1), date(2022, 12, 31)).isoformat(),
            "status": "ACTIVE" if rng.random() < 0.95 else "CLOSED",
        })
    return rows


# --------------------------------------------------------------------------
# Row builders
# --------------------------------------------------------------------------
def valid_row(rng, accounts):
    amount = min(C.VALID_AMOUNT_CAP, max(1.00, round(rng.lognormvariate(5.5, 1.2), 2)))
    return {
        "transaction_id": "",  # assigned after shuffle
        "account_id": rng.choice(accounts)["account_id"],
        "txn_type": rng.choice(C.TXN_TYPES),
        "amount": f"{amount:.2f}",
        "currency": rng.choice(C.CURRENCIES),
        "txn_date": rand_date(rng, C.VALID_DATE_START, C.VALID_DATE_END).isoformat(),
        "channel": rng.choice(C.CHANNELS),
        "status": rng.choice(C.TXN_STATUSES),
    }


def inject(rng, row, family):
    """Mutate a valid row with exactly one defect. Returns the defect description."""
    if family == "null":
        field = rng.choice(C.MANDATORY_FIELDS)
        row[field] = ""
        return f"null {field}"
    if family == "schema_type":
        if rng.random() < 0.5:
            row["amount"] = rng.choice(["abc", "12,5x", "N/A", "ten dollars"])
            return "non-numeric amount"
        row["txn_date"] = rng.choice(["31/13/2025", "not-a-date", "2025-02-30", "2025/07/04"])
        return "malformed txn_date"
    if family == "range":
        kind = rng.choice(["neg_amount", "zero_amount", "huge_amount", "old_date", "future_date"])
        if kind == "neg_amount":
            row["amount"] = f"-{rng.uniform(1, 5000):.2f}"
        elif kind == "zero_amount":
            row["amount"] = "0.00"
        elif kind == "huge_amount":
            row["amount"] = f"{rng.uniform(5_000_000, 99_000_000):.2f}"
        elif kind == "old_date":
            row["txn_date"] = rand_date(rng, date(1990, 1, 1), date(1999, 12, 31)).isoformat()
        else:
            row["txn_date"] = rand_date(rng, date(2027, 1, 1), date(2035, 12, 31)).isoformat()
        return kind
    if family == "referential":
        row["account_id"] = acct_id(rng.randint(900_001, 999_999))
        return "orphan account_id"
    raise ValueError(family)


def scaled_mix(mix, rows):
    """Scale the 1000-row defect mix to `rows` (exact for rows == 1000)."""
    if rows == C.BASE_ROWS:
        return dict(mix)
    return {k: round(v * rows / C.BASE_ROWS) for k, v in mix.items()}


def build_batch(rng, accounts, rows, mix, id_start):
    """Return (list of row dicts in file order, list of rejected-row records)."""
    mix = scaled_mix(mix, rows)
    n_dup = mix["duplicate"]
    n_other = sum(v for k, v in mix.items() if k != "duplicate")
    n_valid = rows - n_other - n_dup
    if n_valid < n_dup:
        raise SystemExit("Too few rows for the requested defect mix.")

    items = [{"row": valid_row(rng, accounts), "tag": None} for _ in range(n_valid)]
    for fam, cnt in mix.items():
        if fam == "duplicate":
            continue
        for _ in range(cnt):
            r = valid_row(rng, accounts)
            items.append({"row": r, "tag": (fam, inject(rng, r, fam))})
    rng.shuffle(items)

    for i, it in enumerate(items):
        it["row"]["transaction_id"] = f"TXN{id_start + i:08d}"

    # Duplicates: a copy of a valid row's transaction_id placed AFTER the original,
    # so a "keep first, reject later" validator flags exactly the copy.
    sources = rng.sample([it for it in items if it["tag"] is None], n_dup)
    for src in sources:
        copy = valid_row(rng, accounts)
        copy["transaction_id"] = src["row"]["transaction_id"]
        pos = next(i for i, it in enumerate(items) if it is src)
        insert_at = rng.randint(pos + 1, len(items))
        items.insert(insert_at, {"row": copy, "tag": ("duplicate", "repeated transaction_id")})

    rejected = []
    for n, it in enumerate(items, start=1):
        if it["tag"]:
            fam, defect = it["tag"]
            rejected.append({"row_number": n, "transaction_id": it["row"]["transaction_id"],
                            "rule_family": FAMILY_LABEL[fam], "defect": defect})
    return [it["row"] for it in items], rejected


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------
def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def summarize(rejected):
    by = {v: 0 for v in FAMILY_LABEL.values()}
    for r in rejected:
        by[r["rule_family"]] += 1
    return by


def batch_entry(file, total, rejected, outcome, alert=None, note=None):
    inv = len(rejected)
    e = {
        "file": file,
        "total_rows": total,
        "expected_valid": total - inv,
        "expected_invalid": inv,
        "rejection_rate_pct": round(100 * inv / total, 2) if total else 0,
        "by_rule_family": summarize(rejected),
        "expected_outcome": outcome,
        "expected_alerts": alert or [],
        "rejected_rows": rejected,
    }
    if note:
        e["note"] = note
    return e


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="./data")
    ap.add_argument("--rows", type=int, default=C.DEFAULT_ROWS)
    ap.add_argument("--seed", type=int, default=C.DEFAULT_SEED)
    ap.add_argument("--accounts", type=int, default=C.DEFAULT_ACCOUNTS)
    a = ap.parse_args()

    out = Path(a.out)
    rng = random.Random(a.seed)
    F = C.FILES

    accounts = make_accounts(rng, a.accounts)
    write_csv(out / F["reference"], C.REFERENCE_COLUMNS, accounts)

    manifest = {
        "generator": {"seed": a.seed, "rows": a.rows, "accounts": a.accounts},
        "rules": {
            "mandatory_fields": C.MANDATORY_FIELDS,
            "expected_columns": C.BATCH_COLUMNS,
            "amount_range": [C.AMOUNT_MIN, C.AMOUNT_MAX],
            "date_range": [C.DATE_MIN.isoformat(), C.DATE_MAX.isoformat()],
            "duplicate_policy": "keep first occurrence of transaction_id, reject later ones",
            "reference_file": F["reference"],
            "rejection_alert_threshold_pct": C.REJECTION_ALERT_THRESHOLD * 100,
        },
        "batches": {},
    }

    # 1. Main demo batch: 950 valid / 50 invalid
    rows, rej = build_batch(rng, accounts, a.rows, C.CLEAN_MIX, 1)
    write_csv(out / F["clean"], C.BATCH_COLUMNS, rows)
    manifest["batches"]["clean"] = batch_entry(F["clean"], len(rows), rej, "SUCCESS",
        note="Rerun twice (KPI-05): target table must hold exactly expected_valid rows, zero duplicate PKs.")

    # 2. High rejection batch: ~15% invalid -> alert #4
    rows, rej = build_batch(rng, accounts, a.rows, C.HIGH_REJECTION_MIX, 100_000)
    write_csv(out / F["high_rejection"], C.BATCH_COLUMNS, rows)
    manifest["batches"]["high_rejection"] = batch_entry(F["high_rejection"], len(rows), rej, "SUCCESS",
        alert=["HighDataRejectionRate"])

    # 3/4. Catastrophic schema corruption + repaired twin (Stage 6)
    rows, rej = build_batch(rng, accounts, a.rows, C.CLEAN_MIX, 200_000)
    corrupt_cols = ["transaction_id", "acct_no", "txn_date", "txn_type", "currency", "channel", "status"]
    corrupt_rows = [{"transaction_id": r["transaction_id"], "acct_no": r["account_id"],
                    "txn_date": r["txn_date"], "txn_type": r["txn_type"], "currency": r["currency"],
                    "channel": r["channel"], "status": r["status"]} for r in rows]
    write_csv(out / F["schema_corrupt"], corrupt_cols, corrupt_rows)
    write_csv(out / F["schema_corrupt_fixed"], C.BATCH_COLUMNS, rows)
    manifest["batches"]["schema_corrupt"] = {
        "file": F["schema_corrupt"], "total_rows": len(rows),
        "defect": "'amount' column missing, 'account_id' renamed to 'acct_no', columns reordered",
        "expected_outcome": "FAILED", "expected_alerts": ["FailedPipelineRun"],
        "note": "Whole batch halted by schema check; nothing loaded.",
    }
    manifest["batches"]["schema_corrupt_fixed"] = batch_entry(F["schema_corrupt_fixed"], len(rows), rej, "SUCCESS",
        note="Same transaction_ids as the corrupt file; rerun after the fix flips status to SUCCESS.")

    # 5. Empty batch (header only)
    write_csv(out / F["empty"], C.BATCH_COLUMNS, [])
    manifest["batches"]["empty"] = {
        "file": F["empty"], "total_rows": 0,
        "expected_outcome": "HANDLED_GRACEFULLY",
        "note": "Zero-row batch: no crash, nothing loaded, run logged.",
    }

    # 6. Unreadable batch (random bytes)
    p = out / F["unreadable"]
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(bytes(rng.getrandbits(8) for _ in range(2048)))
    manifest["batches"]["unreadable"] = {
        "file": F["unreadable"],
        "expected_outcome": "HANDLED_GRACEFULLY",
        "note": "Binary garbage: extraction must fail cleanly and be logged, not raise an unhandled exception.",
    }

    mp = out / F["manifest"]
    mp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Generated data in {out.resolve()}")
    for name, b in manifest["batches"].items():
        extra = f" valid={b['expected_valid']} invalid={b['expected_invalid']}" if "expected_valid" in b else ""
        print(f"  {name:22s} -> {b['expected_outcome']}{extra}")


if __name__ == "__main__":
    main()
