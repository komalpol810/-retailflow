DROP TABLE IF EXISTS gold.dim_seller;
CREATE TABLE gold.dim_seller AS
SELECT
    ROW_NUMBER() OVER (ORDER BY seller_id) AS seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM staging.sellers;
