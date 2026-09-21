"""
Scenario: bulk product deactivation.

Marks enough products INACTIVE today to cross the threshold configured on the
`product_deactivation_check` DAG, which halts rather than let a large
destructive change flow downstream.

Run:
    python -m scripts.scenarios.bulk_deactivate_products
"""

import psycopg2

from scripts.db import CONN

# product_deactivation_check has threshold: 500, so go comfortably past it.
N_TO_DEACTIVATE = 650


def bulk_deactivate(cur):
    cur.execute("""
        UPDATE dim_product
           SET status = 'INACTIVE',
               updated_at = NOW()
         WHERE product_id IN (
               SELECT product_id FROM dim_product
                ORDER BY product_id
                LIMIT %s
         )
    """, (N_TO_DEACTIVATE,))
    return cur.rowcount


def main():
    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            n = bulk_deactivate(cur)
        conn.commit()
    finally:
        conn.close()

    print(f"Marked {n} products INACTIVE with today's timestamp.")
    print("Trigger `product_deactivation_check` in Airflow — it should fail on the DQ threshold.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()