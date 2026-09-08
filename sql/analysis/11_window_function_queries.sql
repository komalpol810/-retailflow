-- ============================================================
-- 1. RUNNING MONTHLY REVENUE
-- Concept: window function running total. Unlike GROUP BY, a window
-- function does NOT collapse rows — each row keeps its own detail
-- AND gets an aggregate calculated across a defined "window" of rows.
-- ============================================================
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month_num,
        SUM(f.price + f.freight_value) AS revenue
    FROM gold.fct_order_items f
    JOIN gold.dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month_num
)
SELECT
    year,
    month_num,
    revenue,
    SUM(revenue) OVER (ORDER BY year, month_num) AS running_total_revenue
FROM monthly_revenue
ORDER BY year, month_num;


-- ============================================================
-- 2. TOP 3 PRODUCTS BY REVENUE, WITHIN EACH CATEGORY
-- Concept: ROW_NUMBER() resets its counter every time the PARTITION BY
-- column changes. This is the standard "top N per group" pattern.
-- ============================================================
WITH product_revenue AS (
    SELECT
        p.product_category_name,
        p.product_key,
        SUM(f.price) AS total_revenue
    FROM gold.fct_order_items f
    JOIN gold.dim_product p ON f.product_key = p.product_key
    WHERE p.product_category_name IS NOT NULL
    GROUP BY p.product_category_name, p.product_key
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_category_name
            ORDER BY total_revenue DESC
        ) AS rank_in_category
    FROM product_revenue
)
SELECT *
FROM ranked
WHERE rank_in_category <= 3
ORDER BY product_category_name, rank_in_category;


-- ============================================================
-- 3. MONTH-OVER-MONTH REVENUE CHANGE
-- Concept: LAG() looks at the PREVIOUS row within an ordered window —
-- lets you compare a row to the row before it without a self-join.
-- ============================================================
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month_num,
        SUM(f.price + f.freight_value) AS revenue
    FROM gold.fct_order_items f
    JOIN gold.dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month_num
)
SELECT
    year,
    month_num,
    revenue,
    LAG(revenue) OVER (ORDER BY year, month_num) AS prev_month_revenue,
    revenue - LAG(revenue) OVER (ORDER BY year, month_num) AS mom_change,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY year, month_num))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month_num), 0), 2
    ) AS mom_pct_change
FROM monthly_revenue
ORDER BY year, month_num;


-- ============================================================
-- 4. SELLER RANKING — RANK() vs DENSE_RANK()
-- Concept: both assign the same rank to ties, but RANK() leaves a gap
-- afterward (1,1,3) while DENSE_RANK() doesn't (1,1,2). Classic
-- interview question — know which one to use and why.
-- ============================================================
WITH seller_revenue AS (
    SELECT
        s.seller_key,
        SUM(f.price) AS total_revenue
    FROM gold.fct_order_items f
    JOIN gold.dim_seller s ON f.seller_key = s.seller_key
    GROUP BY s.seller_key
)
SELECT
    seller_key,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_dense_rank
FROM seller_revenue
ORDER BY total_revenue DESC
LIMIT 20;


-- ============================================================
-- 5. REVENUE ROLLUP: STATE -> GRAND TOTAL
-- Concept: ROLLUP adds subtotal rows automatically. Grouping columns
-- are NULL on those subtotal rows — GROUPING() tells you which level
-- a row belongs to, so you can label it cleanly.
-- ============================================================
SELECT
    COALESCE(g.state, 'ALL STATES') AS customer_state,
    SUM(f.price + f.freight_value) AS revenue,
    GROUPING(g.state) AS is_subtotal_row
FROM gold.fct_order_items f
JOIN gold.dim_geography g ON f.customer_geography_key = g.geography_key
GROUP BY ROLLUP (g.state)
ORDER BY is_subtotal_row, revenue DESC;
