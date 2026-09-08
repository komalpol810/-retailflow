SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE product_key IS NULL) AS missing_product,
    COUNT(*) FILTER (WHERE seller_key IS NULL) AS missing_seller,
    COUNT(*) FILTER (WHERE order_date_key IS NULL) AS missing_date,
    COUNT(*) FILTER (WHERE customer_geography_key IS NULL) AS missing_geography,
    COUNT(*) FILTER (WHERE order_status_key IS NULL) AS missing_order_status,
    COUNT(*) FILTER (WHERE payment_value_allocated IS NULL) AS missing_payment,
    COUNT(DISTINCT order_id) AS distinct_orders
FROM gold.fct_order_items;
