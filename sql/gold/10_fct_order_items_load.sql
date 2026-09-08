INSERT INTO gold.fct_order_items (
    order_id,
    order_item_id,
    customer_key,
    product_key,
    seller_key,
    order_date_key,
    customer_geography_key,
    order_status_key,
    price,
    freight_value,
    payment_value_allocated
)
WITH payment_alloc AS (
    SELECT
        oi.order_id,
        oi.order_item_id,
        oi.price,
        oi.freight_value,
        oi.product_id,
        oi.seller_id,
        ROUND(
            oi.price / NULLIF(SUM(oi.price) OVER (PARTITION BY oi.order_id), 0)
            * COALESCE(op.total_payment, 0),
            2
        ) AS payment_value_allocated
    FROM staging.order_items oi
    LEFT JOIN (
        SELECT order_id, SUM(payment_value) AS total_payment
        FROM staging.order_payments
        GROUP BY order_id
    ) op ON op.order_id = oi.order_id
)
SELECT
    pa.order_id,
    pa.order_item_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    dd.date_key,
    dg.geography_key,
    dos.order_status_key,
    pa.price,
    pa.freight_value,
    pa.payment_value_allocated
FROM payment_alloc pa
JOIN staging.orders o
    ON o.order_id = pa.order_id
JOIN staging.customers c
    ON c.customer_id = o.customer_id
JOIN gold.dim_customer dc
    ON dc.customer_unique_id = c.customer_unique_id
    AND o.order_purchase_timestamp >= dc.valid_from
    AND (o.order_purchase_timestamp < dc.valid_to OR dc.valid_to IS NULL)
LEFT JOIN gold.dim_product dp
    ON dp.product_id = pa.product_id
LEFT JOIN gold.dim_seller ds
    ON ds.seller_id = pa.seller_id
LEFT JOIN gold.dim_date dd
    ON dd.full_date = o.order_purchase_timestamp::date
LEFT JOIN gold.dim_geography dg
    ON dg.zip_code_prefix = c.customer_zip_code_prefix
LEFT JOIN gold.dim_order_status dos
    ON dos.order_status = o.order_status;
