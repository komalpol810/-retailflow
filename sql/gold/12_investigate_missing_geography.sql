SELECT DISTINCT c.customer_zip_code_prefix
FROM gold.fct_order_items f
JOIN staging.orders o ON o.order_id = f.order_id
JOIN staging.customers c ON c.customer_id = o.customer_id
WHERE f.customer_geography_key IS NULL
LIMIT 10;
