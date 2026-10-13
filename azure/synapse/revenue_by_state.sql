SELECT TOP 5
    g.state,
    COUNT(*)     AS items_sold,
    SUM(f.price) AS revenue
FROM gold.fct_order_items f
JOIN gold.dim_geography g
  ON f.customer_geography_key = g.geography_key
GROUP BY g.state
ORDER BY revenue DESC;
