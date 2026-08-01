-- =====================================================
-- Customer Lifetime Value (CLV) Analysis
-- Description: Detailed CLV calculation with segmentation
-- =====================================================

WITH customer_order_history AS (
    SELECT 
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment,
        c.first_order_date,
        c.last_order_date,
        c.total_orders,
        c.lifetime_value,
        DATEDIFF('day', c.first_order_date, c.last_order_date) AS customer_tenure_days,
        c.total_orders * 30.0 / NULLIF(DATEDIFF('day', c.first_order_date, COALESCE(c.last_order_date, CURRENT_DATE)), 0) AS orders_per_month,
        f.order_id,
        f.order_purchase_date,
        f.total_value,
        f.review_score,
        f.delivery_days
    FROM olist_warehouse.dim_customers_iceberg c
    JOIN olist_warehouse.fact_orders_iceberg f ON c.customer_sk = f.customer_sk
    WHERE c.is_current = TRUE
      AND f.order_status = 'delivered'
),
clv_percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lifetime_value) AS p25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY lifetime_value) AS p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lifetime_value) AS p75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY lifetime_value) AS p90
    FROM (SELECT DISTINCT customer_unique_id, lifetime_value FROM customer_order_history)
),
customer_clv AS (
    SELECT 
        customer_unique_id,
        customer_state,
        customer_city,
        customer_segment,
        first_order_date,
        last_order_date,
        total_orders,
        lifetime_value,
        customer_tenure_days,
        orders_per_month,
        CASE 
            WHEN lifetime_value >= (SELECT p90 FROM clv_percentiles) THEN 'Top 10%'
            WHEN lifetime_value >= (SELECT p75 FROM clv_percentiles) THEN 'Top 25%'
            WHEN lifetime_value >= (SELECT p50 FROM clv_percentiles) THEN 'Top 50%'
            ELSE 'Bottom 50%'
        END AS clv_percentile,
        (lifetime_value / NULLIF(customer_tenure_days, 0)) * 365 AS annualized_clv,
        CASE 
            WHEN total_orders >= 5 AND lifetime_value >= (SELECT p75 FROM clv_percentiles) THEN 'High Value Loyal'
            WHEN total_orders >= 3 AND lifetime_value >= (SELECT p50 FROM clv_percentiles) THEN 'Medium Value Regular'
            WHEN total_orders >= 1 THEN 'Low Value'
            ELSE 'Inactive'
        END AS customer_health
    FROM (SELECT DISTINCT * FROM customer_order_history)
)
SELECT 
    customer_state,
    COUNT(DISTINCT customer_unique_id) AS customer_count,
    ROUND(SUM(lifetime_value), 2) AS total_clv,
    ROUND(AVG(lifetime_value), 2) AS avg_clv,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value), 2) AS median_clv,
    ROUND(AVG(total_orders), 1) AS avg_orders_per_customer,
    ROUND(AVG(orders_per_month), 2) AS avg_orders_per_month,
    ROUND(AVG(customer_tenure_days), 0) AS avg_tenure_days,
    ROUND(SUM(annualized_clv), 2) AS projected_annual_clv,
    SUM(CASE WHEN clv_percentile = 'Top 10%' THEN 1 ELSE 0 END) AS top_10_pct_customers,
    SUM(CASE WHEN clv_percentile = 'Top 25%' THEN 1 ELSE 0 END) AS top_25_pct_customers,
    SUM(CASE WHEN customer_health = 'High Value Loyal' THEN 1 ELSE 0 END) AS high_value_loyal,
    SUM(CASE WHEN customer_health = 'Medium Value Regular' THEN 1 ELSE 0 END) AS medium_value_regular,
    SUM(CASE WHEN customer_health = 'Low Value' THEN 1 ELSE 0 END) AS low_value
FROM customer_clv
GROUP BY customer_state
ORDER BY total_clv DESC