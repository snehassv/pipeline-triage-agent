"""
Scenario: orders arrive for stores the store master has never heard of.

Three new stores start trading before the store system sends them, so stg_orders has
rows whose store_id is in neither stg_stores nor dim_store. fct_order_lines' foreign
key refuses them. The code is right and so is the key: the store master is late.
The tempting patches — inner-join the orphans away, or insert placeholder "unknown"
stores — both pass, and both hide that three stores are missing from every report.

Run:
    python -m scripts.scenarios.add_orders_for_unknown_stores
"""

import random
from datetime import date

import psycopg2

from scripts.db import CONN

random.seed(7)
NEW_STORES = (51, 52, 53)
N_ORDERS = 40
ORDER_DATE = date(2026, 9, 21)


def main():
    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT coalesce(max(order_id), 0) FROM stg_orders")
            next_id = cur.fetchone()[0] + 1
            rows = []
            for i in range(N_ORDERS):
                qty = random.randint(1, 12)
                rows.append((next_id + i, random.randint(1, 2000), random.choice(NEW_STORES),
                             random.randint(1, 1000), qty,
                             round(qty * random.uniform(1.99, 24.99), 2), ORDER_DATE))
            cur.executemany("INSERT INTO stg_orders VALUES (%s,%s,%s,%s,%s,%s,%s)", rows)
        conn.commit()
    finally:
        conn.close()
    print(f"Added {N_ORDERS} orders for stores {NEW_STORES}, which dim_store doesn't have.")
    print("Trigger `sales_fact_load` in Airflow — it should fail on a foreign key.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()
