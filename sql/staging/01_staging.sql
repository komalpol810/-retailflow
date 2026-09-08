-- Staging layer: typed, cleaned versions of raw tables
CREATE SCHEMA IF NOT EXISTS staging;

-- customers
DROP TABLE IF EXISTS staging.customers;
CREATE TABLE staging.customers AS
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _ingested_at,
    _source_file
FROM raw.olist_customers_dataset;

-- geolocation, aggregated by zip prefix (raw has ~1,000 duplicate rows per zip)
DROP TABLE IF EXISTS staging.geolocation;
CREATE TABLE staging.geolocation AS
SELECT
    geolocation_zip_code_prefix AS zip_code_prefix,
    AVG(geolocation_lat::NUMERIC) AS avg_lat,
    AVG(geolocation_lng::NUMERIC) AS avg_lng,
    MODE() WITHIN GROUP (ORDER BY geolocation_city) AS city,
    MODE() WITHIN GROUP (ORDER BY geolocation_state) AS state
FROM raw.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix;

-- order_items
DROP TABLE IF EXISTS staging.order_items;
CREATE TABLE staging.order_items AS
SELECT
    order_id,
    order_item_id::INTEGER,
    product_id,
    seller_id,
    shipping_limit_date::TIMESTAMP,
    price::NUMERIC,
    freight_value::NUMERIC,
    _ingested_at,
    _source_file
FROM raw.olist_order_items_dataset;

-- order_payments
DROP TABLE IF EXISTS staging.order_payments;
CREATE TABLE staging.order_payments AS
SELECT
    order_id,
    payment_sequential::INTEGER,
    payment_type,
    payment_installments::INTEGER,
    payment_value::NUMERIC,
    _ingested_at,
    _source_file
FROM raw.olist_order_payments_dataset;

-- order_reviews
DROP TABLE IF EXISTS staging.order_reviews;
CREATE TABLE staging.order_reviews AS
SELECT
    review_id,
    order_id,
    review_score::INTEGER,
    review_comment_title,
    review_comment_message,
    review_creation_date::TIMESTAMP,
    review_answer_timestamp::TIMESTAMP,
    _ingested_at,
    _source_file
FROM raw.olist_order_reviews_dataset;

-- orders
DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::TIMESTAMP,
    order_approved_at::TIMESTAMP,
    order_delivered_carrier_date::TIMESTAMP,
    order_delivered_customer_date::TIMESTAMP,
    order_estimated_delivery_date::TIMESTAMP,
    _ingested_at,
    _source_file
FROM raw.olist_orders_dataset;

-- products
DROP TABLE IF EXISTS staging.products;
CREATE TABLE staging.products AS
SELECT
    product_id,
    product_category_name,
    product_name_lenght::INTEGER,
    product_description_lenght::INTEGER,
    product_photos_qty::INTEGER,
    product_weight_g::NUMERIC,
    product_length_cm::NUMERIC,
    product_height_cm::NUMERIC,
    product_width_cm::NUMERIC,
    _ingested_at,
    _source_file
FROM raw.olist_products_dataset;

-- sellers
DROP TABLE IF EXISTS staging.sellers;
CREATE TABLE staging.sellers AS
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _ingested_at,
    _source_file
FROM raw.olist_sellers_dataset;

-- category_translation
DROP TABLE IF EXISTS staging.category_translation;
CREATE TABLE staging.category_translation AS
SELECT
    product_category_name,
    product_category_name_english,
    _ingested_at,
    _source_file
FROM raw.product_category_name_translation;
