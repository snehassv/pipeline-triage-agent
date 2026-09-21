"""
Scenario: upstream schema drift.

Adds a column to the orders extract that the target table doesn't have. The
load fails because it wasn't configured to tolerate field addition — the
single most common shape of pipeline breakage in practice.

Run:
    python -m scripts.scenarios.add_column_to_orders_extract
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
EXTRACT = DATA_DIR / "extracts" / "orders_20260921.csv"

NEW_COLUMN = "effective_ts"
BASE_DATE = datetime(2026, 9, 21)


def add_column(path: Path) -> int:
    if not path.exists():
        raise SystemExit(
            f"{path} not found — run the seed script first:\n"
            f"    python scripts/seed_warehouse.py"
        )

    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)

    if NEW_COLUMN in header:
        print(f"{NEW_COLUMN} already present — extract is already drifted.")
        return 0

    header.append(NEW_COLUMN)
    for row in rows:
        ts = BASE_DATE - timedelta(minutes=random.randint(0, 60 * 24 * 7))
        row.append(ts.isoformat(sep=" ", timespec="seconds"))

    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)

    return len(rows)


def main():
    n = add_column(EXTRACT)
    if n:
        print(f"Added '{NEW_COLUMN}' to {EXTRACT.name} ({n} rows rewritten).")
    print("Trigger `orders_refresh` in Airflow — it should fail on a column mismatch.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()