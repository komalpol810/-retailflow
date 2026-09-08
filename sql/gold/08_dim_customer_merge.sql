BEGIN;

-- Step A: latest snapshot per real customer, as of their most recent order
CREATE TEMP TABLE latest_snapshot AS
SELECT DISTINCT ON (c.customer_unique_id)
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    MD5(COALESCE(c.customer_zip_code_prefix,'') || '|' ||
        COALESCE(c.customer_city,'') || '|' ||
        COALESCE(c.customer_state,'')) AS row_hash
FROM staging.customers c
JOIN staging.orders o ON o.customer_id = c.customer_id
ORDER BY c.customer_unique_id, o.order_purchase_timestamp DESC;

-- Step B: expire current dim rows where the snapshot hash has changed
UPDATE gold.dim_customer AS tgt
SET valid_to = now(), is_current = FALSE
FROM latest_snapshot AS src
WHERE tgt.customer_unique_id = src.customer_unique_id
  AND tgt.is_current = TRUE
  AND tgt.row_hash <> src.row_hash;

-- Step C: insert new current rows for (a) brand-new customers, or (b) customers just expired above
INSERT INTO gold.dim_customer
    (customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
     valid_from, valid_to, is_current, row_hash)
SELECT
    src.customer_unique_id,
    src.customer_zip_code_prefix,
    src.customer_city,
    src.customer_state,
    now(),
    NULL,
    TRUE,
    src.row_hash
FROM latest_snapshot src
LEFT JOIN gold.dim_customer tgt
    ON tgt.customer_unique_id = src.customer_unique_id
    AND tgt.is_current = TRUE
WHERE tgt.customer_key IS NULL;

COMMIT;
