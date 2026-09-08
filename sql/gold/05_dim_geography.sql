DROP TABLE IF EXISTS gold.dim_geography;
CREATE TABLE gold.dim_geography AS
SELECT
    ROW_NUMBER() OVER (ORDER BY zip_code_prefix) AS geography_key,
    zip_code_prefix,
    city,
    state,
    avg_lat,
    avg_lng
FROM staging.geolocation;
