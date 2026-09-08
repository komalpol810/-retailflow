DROP TABLE IF EXISTS gold.dim_order_status;
CREATE TABLE gold.dim_order_status AS
SELECT
    ROW_NUMBER() OVER (ORDER BY order_status) AS order_status_key,
    order_status
FROM (SELECT DISTINCT order_status FROM staging.orders) s;
