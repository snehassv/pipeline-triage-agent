-- Daily snapshots of the store master, as the store system exports them.

CREATE TABLE stg_store_snapshots (
    store_id      INT,
    store_name    TEXT,
    region        TEXT,
    updated_at    TIMESTAMP,
    snapshot_date DATE
);

COMMENT ON TABLE stg_store_snapshots IS
    'One row per store per snapshot_date. The store system re-exports every store daily '
    'and this table keeps the last 7 days, so each store_id appears up to 7 times. '
    'The newest snapshot_date is the current state of a store.';
