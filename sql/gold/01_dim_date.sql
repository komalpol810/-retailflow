CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.dim_date;
CREATE TABLE gold.dim_date AS
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d::DATE AS full_date,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
    EXTRACT(MONTH FROM d)::INTEGER AS month_num,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday
FROM generate_series('2016-01-01'::DATE, '2018-12-31'::DATE, '1 day'::INTERVAL) d;
