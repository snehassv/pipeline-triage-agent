"""
Scenario: upstream schema drift.

Adds a column to the orders extract that the target table doesn't have. The
load fails because it wasn't configured to tolerate field addition — the
single most common shape of pipeline breakage in practice.

The column is whichever candidate stg_orders doesn't already have. Once a
migration adding one is merged — the agent proposed exactly that in #13 — the
scenario moves to the next name instead of silently passing.

Run:
    python -m scripts.scenarios.add_column_to_orders_extract
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2

from scripts.db import CONN

random.seed(42)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
EXTRACT = DATA_DIR / "extracts" / "orders_20260921.csv"

# Tried in order; the first one stg_orders doesn't have is the one that drifts.
CANDIDATE_COLUMNS = ["effective_ts", "source_system_ts", "ingested_at", "batch_ts"]
BASE_DATE = datetime(2026, 9, 21)


def pick_column() -> str:
    """A column name the target table doesn't have, so the load really fails."""
    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'stg_orders'")
            existing = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()
    for candidate in CANDIDATE_COLUMNS:
        if candidate not in existing:
            return candidate
    raise SystemExit(
        "stg_orders already has every candidate column: "
        f"{', '.join(CANDIDATE_COLUMNS)}. Add another to CANDIDATE_COLUMNS."
    )


def add_column(path: Path, new_column: str) -> int:
    if not path.exists():
        raise SystemExit(
            f"{path} not found — run the seed script first:\n"
            f"    python scripts/seed_warehouse.py"
        )

    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)

    if new_column in header:
        print(f"{new_column} already present — extract is already drifted.")
        return 0

    header.append(new_column)
    for row in rows:
        ts = BASE_DATE - timedelta(minutes=random.randint(0, 60 * 24 * 7))
        row.append(ts.isoformat(sep=" ", timespec="seconds"))

    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)

    return len(rows)


def main():
    new_column = pick_column()
    n = add_column(EXTRACT, new_column)
    if n:
        print(f"Added '{new_column}' to {EXTRACT.name} ({n} rows rewritten).")
    print("Trigger `orders_refresh` in Airflow — it should fail on a column mismatch.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()