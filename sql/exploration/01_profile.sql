-- ============ ROW COUNTS (1) ============
SELECT 'customers' AS tbl, count(*) FROM raw.olist_customers_dataset
UNION ALL SELECT 'orders', count(*) FROM raw.olist_orders_dataset
UNION ALL SELECT 'order_items', count(*) FROM raw.olist_order_items_dataset
UNION ALL SELECT 'payments', count(*) FROM raw.olist_order_payments_dataset
UNION ALL SELECT 'reviews', count(*) FROM raw.olist_order_reviews_dataset
UNION ALL SELECT 'products', count(*) FROM raw.olist_products_dataset
UNION ALL SELECT 'sellers', count(*) FROM raw.olist_sellers_dataset
UNION ALL SELECT 'geolocation', count(*) FROM raw.olist_geolocation_dataset
UNION ALL SELECT 'category_translation', count(*) FROM raw.product_category_name_translation;

-- ============ PK DUPLICATE CHECKS (2-5) ============
-- (2) orders PK
SELECT order_id, count(*) FROM raw.olist_orders_dataset GROUP BY order_id HAVING count(*) > 1;

-- (3) customers PK
SELECT customer_id, count(*) FROM raw.olist_customers_dataset GROUP BY customer_id HAVING count(*) > 1;

-- (4) products PK
SELECT product_id, count(*) FROM raw.olist_products_dataset GROUP BY product_id HAVING count(*) > 1;

-- (5) reviews PK -- known Olist quirk: some review_ids repeat across different orders
SELECT review_id, count(*) FROM raw.olist_order_reviews_dataset GROUP BY review_id HAVING count(*) > 1;

-- ============ ORPHANED FOREIGN KEYS (6-9) ============
-- (6) order_items → orders
SELECT oi.order_id FROM raw.olist_order_items_dataset oi
LEFT JOIN raw.olist_orders_dataset o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- (7) order_items → products
SELECT oi.product_id FROM raw.olist_order_items_dataset oi
LEFT JOIN raw.olist_products_dataset p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- (8) orders → customers
SELECT o.customer_id FROM raw.olist_orders_dataset o
LEFT JOIN raw.olist_customers_dataset c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- (9) orders with NO matching row in order_items (paid but nothing shipped?)
SELECT o.order_id FROM raw.olist_orders_dataset o
LEFT JOIN raw.olist_order_items_dataset oi ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL;

-- ============ NULL PROFILING (10-11) ============
-- (10) orders — the 4 lifecycle timestamps
SELECT
  count(*) FILTER (WHERE order_approved_at IS NULL) AS null_approved,
  count(*) FILTER (WHERE order_delivered_carrier_date IS NULL) AS null_carrier,
  count(*) FILTER (WHERE order_delivered_customer_date IS NULL) AS null_delivered,
  count(*) AS total
FROM raw.olist_orders_dataset;

-- (11) reviews — comment fields (expect high nulls, Brazilians often skip free text)
SELECT
  count(*) FILTER (WHERE review_comment_title IS NULL) AS null_title,
  count(*) FILTER (WHERE review_comment_message IS NULL) AS null_message,
  count(*) AS total
FROM raw.olist_order_reviews_dataset;

-- ============ DATE RANGES (12) ============
SELECT min(order_purchase_timestamp), max(order_purchase_timestamp)
FROM raw.olist_orders_dataset;

-- ============ DISTRIBUTIONS (13-14) ============
-- (13) order_status distribution
SELECT order_status, count(*) 
FROM raw.olist_orders_dataset
GROUP BY order_status ORDER BY count(*) DESC;

-- (14) payment_type distribution
SELECT payment_type, count(*)
FROM raw.olist_order_payments_dataset
GROUP BY payment_type ORDER BY count(*) DESC;

-- ============ THE GEOLOCATION TRAP (15) ============
-- zip prefixes with multiple lat/lng rows -- proves geolocation is NOT 1 row per zip
SELECT geolocation_zip_code_prefix, count(*) 
FROM raw.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
ORDER BY count(*) DESC
LIMIT 10;
