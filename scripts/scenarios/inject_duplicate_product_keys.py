import random
from datetime import datetime,timedelta
import psycopg2
from scripts.db import CONN

from faker import Faker

fake = Faker()
random.seed(42)

N_PRODUCTS = 10
EXTRACT_DATE = datetime(2026, 9, 21)
CATEGORIES = ["Cereal", "Snacks", "Baking", "Frozen", "Beverages"]


def inject_duplicate_product_keys(cur):
    rows = []
    for pid in range(1, N_PRODUCTS + 1):
        rows.append((
            pid,
            fake.catch_phrase(),
            random.choice(CATEGORIES),
            random.choices(["ACTIVE", "INACTIVE"], weights=[9, 1])[0],
            EXTRACT_DATE - timedelta(days=random.randint(0, 30)),
        ))
    cur.executemany("INSERT INTO stg_products VALUES (%s,%s,%s,%s,%s)", rows)
    print(f"Injected {len(rows)} duplicate product_ids into stg_products.")
    return rows

def main():
    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            inject_duplicate_product_keys(cur)
        print("Trigger `product_dim_upsert` in Airflow — it should fail on cardinality violation.")
        print("Run scripts/scenarios/reset.py to restore.")
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()