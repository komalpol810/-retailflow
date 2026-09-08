SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    dc.customer_key,
    dc.valid_from,
    dc.valid_to,
    dc.is_current
FROM staging.orders o
JOIN staging.customers c
    ON c.customer_id = o.customer_id
JOIN gold.dim_customer dc
    ON dc.customer_unique_id = c.customer_unique_id
    AND o.order_purchase_timestamp >= dc.valid_from
    AND (o.order_purchase_timestamp < dc.valid_to OR dc.valid_to IS NULL)
LIMIT 20;
