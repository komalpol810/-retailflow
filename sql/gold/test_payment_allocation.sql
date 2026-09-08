SELECT
    oi.order_id,
    oi.order_item_id,
    oi.price,
    SUM(oi.price) OVER (PARTITION BY oi.order_id) AS order_total_price,
    p.total_payment,
    ROUND(
        oi.price / NULLIF(SUM(oi.price) OVER (PARTITION BY oi.order_id), 0) * p.total_payment,
        2
    ) AS payment_value_allocated
FROM staging.order_items oi
LEFT JOIN (
    SELECT order_id, SUM(payment_value) AS total_payment
    FROM staging.order_payments
    GROUP BY order_id
) p ON p.order_id = oi.order_id
LIMIT 20;
