CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_unique_id  TEXT NOT NULL,
    customer_zip_code_prefix TEXT,
    customer_city       TEXT,
    customer_state      TEXT,
    valid_from           TIMESTAMP NOT NULL,
    valid_to             TIMESTAMP,
    is_current           BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash             TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_unique_id ON gold.dim_customer (customer_unique_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_is_current ON gold.dim_customer (is_current);
