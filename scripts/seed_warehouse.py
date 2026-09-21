"""
Seed the local warehouse with fake data.

Idempotent — drops and recreates everything, so it's safe to re-run whenever
you want a clean slate. Deterministic: the same seed produces the same data
every time, so failures reproduce identically.

Usage:
    python scripts/seed_warehouse.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

DATA_DIR = Path(__file__).parent.parent / "data"
EXTRACT_DATE = datetime(2026, 9, 21)

CONN = dict(
    host="localhost",
    port=5433,
    user="warehouse",
    password="warehouse",
    dbname="warehouse",
)

N_PRODUCTS = 500
N_STORES = 50
N_CUSTOMERS = 2000
N_ORDERS = 20000

CATEGORIES = ["Cereal", "Snacks", "Baking", "Frozen", "Beverages"]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def create_tables(cur):
    cur.execute("DROP TABLE IF EXISTS fct_sales CASCADE")
    cur.execute("DROP TABLE IF EXISTS stg_orders CASCADE")
    cur.execute("DROP TABLE IF EXISTS stg_customers CASCADE")
    cur.execute("DROP TABLE IF EXISTS stg_products CASCADE")
    cur.execute("DROP TABLE IF EXISTS stg_stores CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_product CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_store CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_hierarchy_v1 CASCADE")

    cur.execute("""
        CREATE TABLE stg_products (
            product_id   INT,
            product_name TEXT,
            category     TEXT,
            status       TEXT,
            updated_at   TIMESTAMP
        )
    """)

    # The PRIMARY KEY here is load-bearing: it's what makes the MERGE DAG
    # fail with a real cardinality violation when the source has dupes.
    cur.execute("""
        CREATE TABLE dim_product (
            product_id   INT PRIMARY KEY,
            product_name TEXT,
            category     TEXT,
            status       TEXT,
            updated_at   TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE stg_stores (
            store_id   INT,
            store_name TEXT,
            region     TEXT,
            updated_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE dim_store (
            store_id   INT PRIMARY KEY,
            store_name TEXT,
            region     TEXT,
            updated_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE stg_customers (
            customer_id INT,
            name        TEXT,
            email       TEXT,
            region      TEXT,
            created_at  TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE stg_orders (
            order_id    INT,
            customer_id INT,
            store_id    INT,
            product_id  INT,
            qty         INT,
            amount      NUMERIC(10, 2),
            order_date  DATE
        )
    """)

    # Exists only so a scenario script can drop it and break a DAG.
    cur.execute("""
        CREATE TABLE dim_hierarchy_v1 (
            node_id     INT PRIMARY KEY,
            parent_id   INT,
            node_name   TEXT,
            level_depth INT
        )
    """)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_products(cur):
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
    cur.executemany("INSERT INTO dim_product VALUES (%s,%s,%s,%s,%s)", rows)
    print(f"  stg_products / dim_product: {len(rows)} rows")
    return rows


def seed_stores(cur):
    rows = []
    for sid in range(1, N_STORES + 1):
        rows.append((
            sid,
            f"{fake.city()} {random.choice(['Market', 'Superstore', 'Express'])}",
            random.choice(REGIONS),
            EXTRACT_DATE - timedelta(days=random.randint(0, 60)),
        ))
    cur.executemany("INSERT INTO stg_stores VALUES (%s,%s,%s,%s)", rows)
    cur.executemany("INSERT INTO dim_store VALUES (%s,%s,%s,%s)", rows)
    print(f"  stg_stores / dim_store: {len(rows)} rows")
    return rows


def seed_customers(cur):
    rows = []
    for cid in range(1, N_CUSTOMERS + 1):
        rows.append((
            cid,
            fake.name(),
            fake.email(),
            random.choice(REGIONS),
            EXTRACT_DATE - timedelta(days=random.randint(0, 365)),
        ))
    cur.executemany("INSERT INTO stg_customers VALUES (%s,%s,%s,%s,%s)", rows)
    print(f"  stg_customers: {len(rows)} rows")
    return rows


def seed_orders(cur):
    rows = []
    for oid in range(1, N_ORDERS + 1):
        qty = random.randint(1, 12)
        rows.append((
            oid,
            random.randint(1, N_CUSTOMERS),
            random.randint(1, N_STORES),
            random.randint(1, N_PRODUCTS),
            qty,
            round(qty * random.uniform(1.99, 24.99), 2),
            (EXTRACT_DATE - timedelta(days=random.randint(0, 90))).date(),
        ))
    cur.executemany(
        "INSERT INTO stg_orders VALUES (%s,%s,%s,%s,%s,%s,%s)", rows
    )
    print(f"  stg_orders: {len(rows)} rows")
    return rows


def seed_hierarchy(cur):
    rows = [(1, None, "All Products", 0)]
    for i, cat in enumerate(CATEGORIES, start=2):
        rows.append((i, 1, cat, 1))
    cur.executemany("INSERT INTO dim_hierarchy_v1 VALUES (%s,%s,%s,%s)", rows)
    print(f"  dim_hierarchy_v1: {len(rows)} rows")


def build_fct_sales(cur):
    cur.execute("""
        CREATE TABLE fct_sales AS
        SELECT
            o.order_id,
            o.order_date,
            o.qty,
            o.amount,
            p.product_id,
            p.product_name,
            p.category,
            s.store_id,
            s.store_name,
            s.region
        FROM stg_orders o
        JOIN dim_product p ON p.product_id = o.product_id
        JOIN dim_store   s ON s.store_id   = o.store_id
    """)
    cur.execute("SELECT count(*) FROM fct_sales")
    print(f"  fct_sales: {cur.fetchone()[0]} rows")


# ---------------------------------------------------------------------------
# CSV extracts — some DAGs read these as their source
# ---------------------------------------------------------------------------

def write_extract(name, header, rows):
    out = DATA_DIR / "extracts" / name
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {out.relative_to(DATA_DIR.parent)} ({len(rows)} rows)")


# ---------------------------------------------------------------------------

def main():
    stamp = EXTRACT_DATE.strftime("%Y%m%d")

    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            print("creating tables...")
            create_tables(cur)

            print("seeding...")
            seed_products(cur)
            seed_stores(cur)
            customers = seed_customers(cur)
            orders = seed_orders(cur)
            seed_hierarchy(cur)
            build_fct_sales(cur)
        conn.commit()
    finally:
        conn.close()

    print("writing extracts...")
    write_extract(
        f"orders_{stamp}.csv",
        ["order_id", "customer_id", "store_id", "product_id",
         "qty", "amount", "order_date"],
        orders,
    )
    write_extract(
        f"customers_{stamp}.csv",
        ["customer_id", "name", "email", "region", "created_at"],
        customers,
    )

    # Landing directory for the sensor DAG. Deliberately left empty —
    # vendor_feed.csv never arrives, which is the point.
    (DATA_DIR / "landing").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "exports").mkdir(parents=True, exist_ok=True)

    print("seed complete")


if __name__ == "__main__":
    main()