-- Initial warehouse schema for the local harness.
--
-- Migrations are applied in order by `python -m scripts.migrate` (the seed script
-- runs it too). Each file is applied once and recorded in schema_migrations; to
-- change the schema, add a new numbered file rather than editing this one.
-- Migrations may only contain additive DDL — see scripts/ddl_policy.py.

CREATE TABLE stg_products (
    product_id   INT,
    product_name TEXT,
    category     TEXT,
    status       TEXT,
    updated_at   TIMESTAMP
);

-- The PRIMARY KEY here is load-bearing: it's what makes the MERGE DAG
-- fail with a real cardinality violation when the source has dupes.
CREATE TABLE dim_product (
    product_id   INT PRIMARY KEY,
    product_name TEXT,
    category     TEXT,
    status       TEXT,
    updated_at   TIMESTAMP
);

CREATE TABLE stg_stores (
    store_id   INT,
    store_name TEXT,
    region     TEXT,
    updated_at TIMESTAMP
);

CREATE TABLE dim_store (
    store_id   INT PRIMARY KEY,
    store_name TEXT,
    region     TEXT,
    updated_at TIMESTAMP
);

CREATE TABLE stg_customers (
    customer_id INT,
    name        TEXT,
    email       TEXT,
    region      TEXT,
    created_at  TIMESTAMP
);

CREATE TABLE stg_orders (
    order_id    INT,
    customer_id INT,
    store_id    INT,
    product_id  INT,
    qty         INT,
    amount      NUMERIC(10, 2),
    order_date  DATE
);

-- Exists only so a scenario script can drop it and break a DAG.
CREATE TABLE dim_hierarchy_v1 (
    node_id     INT PRIMARY KEY,
    parent_id   INT,
    node_name   TEXT,
    level_depth INT
);
