-- The vendor's daily price list. Prices are US dollars per unit.

CREATE TABLE stg_vendor_prices (
    sku        TEXT,
    unit_price NUMERIC(10, 2)
        CONSTRAINT unit_price_in_dollars CHECK (unit_price BETWEEN 0.01 AND 999.99),
    currency   TEXT,
    price_date DATE
);
