WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp
    FROM staging.customers c
    JOIN staging.orders o ON o.customer_id = c.customer_id
),
hashed AS (
    SELECT
        *,
        MD5(COALESCE(customer_zip_code_prefix,'') || '|' ||
            COALESCE(customer_city,'') || '|' ||
            COALESCE(customer_state,'')) AS row_hash
    FROM customer_orders
),
change_flagged AS (
    SELECT
        *,
        LAG(row_hash) OVER (
            PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp
        ) AS prev_hash
    FROM hashed
),
versioned AS (
    SELECT
        *,
        SUM(CASE WHEN prev_hash IS NULL OR prev_hash <> row_hash THEN 1 ELSE 0 END)
            OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp) AS version_num
    FROM change_flagged
),
collapsed AS (
    SELECT
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        row_hash,
        version_num,
        MIN(order_purchase_timestamp) AS valid_from
    FROM versioned
    GROUP BY customer_unique_id, customer_zip_code_prefix, customer_city,
             customer_state, row_hash, version_num
)
INSERT INTO gold.dim_customer
    (customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
     valid_from, valid_to, is_current, row_hash)
SELECT
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY customer_unique_id ORDER BY valid_from) AS valid_to,
    (LEAD(valid_from) OVER (PARTITION BY customer_unique_id ORDER BY valid_from) IS NULL) AS is_current,
    row_hash
FROM collapsed;
