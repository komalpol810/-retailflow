DROP TABLE IF EXISTS gold.fct_order_items;

CREATE TABLE gold.fct_order_items (
    -- surrogate key
    order_item_key      BIGSERIAL PRIMARY KEY,

    -- natural/degenerate keys
    order_id             TEXT NOT NULL,
    order_item_id        INTEGER NOT NULL,

    -- foreign keys to dimensions
    customer_key           INTEGER REFERENCES gold.dim_customer(customer_key),
    product_key            BIGINT REFERENCES gold.dim_product(product_key),
    seller_key             BIGINT REFERENCES gold.dim_seller(seller_key),
    order_date_key         INTEGER REFERENCES gold.dim_date(date_key),
    customer_geography_key BIGINT REFERENCES gold.dim_geography(geography_key),
    order_status_key       BIGINT REFERENCES gold.dim_order_status(order_status_key),

    -- measures
    price                    NUMERIC(12,2) NOT NULL,
    freight_value            NUMERIC(12,2) NOT NULL,
    payment_value_allocated  NUMERIC(12,2),

    -- lineage
    _loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (order_id, order_item_id)
);
