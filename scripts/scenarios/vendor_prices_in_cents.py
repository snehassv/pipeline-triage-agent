"""
Scenario: the vendor's price list arrives in cents.

Every unit_price in today's file is 100 times what it should be, and the column name
and currency are unchanged. Maybe the vendor switched units, maybe their export broke:
either way someone has to ask them before anything is rescaled. Dividing by 100 in the
loader makes the run pass and is a guess — wrong the day they fix their side.

Run:
    python -m scripts.scenarios.vendor_prices_in_cents
"""

import csv
from decimal import Decimal
from pathlib import Path

EXTRACT = (Path(__file__).resolve().parent.parent.parent
           / "data" / "extracts" / "vendor_prices_20260921.csv")


def main():
    with EXTRACT.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)
    price = header.index("unit_price")
    for row in rows:
        row[price] = str(int(Decimal(row[price]) * 100))
    with EXTRACT.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Rewrote {len(rows)} prices in {EXTRACT.name} as cents.")
    print("Trigger `vendor_price_load` in Airflow — it should fail on a check constraint.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()
