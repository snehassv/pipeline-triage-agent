-- Order lines, keyed to the store dimension.

CREATE TABLE fct_order_lines (
    order_id   INT PRIMARY KEY,
    store_id   INT NOT NULL REFERENCES dim_store (store_id),
    product_id INT,
    qty        INT,
    amount     NUMERIC(10, 2),
    order_date DATE
);
