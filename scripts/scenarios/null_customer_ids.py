"""
Scenario: the orders extract loses customer_id for part of a day.

137 of the latest day's orders arrive with no customer. The null check on stg_orders
is doing its job. Filtering the rows out, or raising the threshold, gets the run
through and drops real orders from every customer report.

Run:
    python -m scripts.scenarios.null_customer_ids
"""

import psycopg2

from scripts.db import CONN

N_ORDERS = 137


def main():
    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE stg_orders SET customer_id = NULL
                 WHERE order_id IN (SELECT order_id FROM stg_orders
                                     WHERE order_date = (SELECT max(order_date) FROM stg_orders)
                                     ORDER BY order_id LIMIT %s)
            """, (N_ORDERS,))
            n = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    print(f"Blanked customer_id on {n} orders.")
    print("Trigger `order_null_check` in Airflow — it should fail its DQ check.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()
