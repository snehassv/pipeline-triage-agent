-- Upstream orders extract now includes an effective_ts column.
-- Add it to stg_orders so the loader's dynamic column list matches the table.
ALTER TABLE stg_orders ADD COLUMN effective_ts TIMESTAMP;
COMMENT ON COLUMN stg_orders.effective_ts IS 'Effective timestamp added by upstream extract change, tracked via migration 0002.';
