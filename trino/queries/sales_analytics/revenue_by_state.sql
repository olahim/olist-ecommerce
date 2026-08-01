-- =====================================================
-- Revenue by State Analysis
-- Description: Sales performance breakdown by Brazilian state
-- =====================================================

WITH daily_sales AS (
    SELECT 
        DATE_TRUNC('day', f.order_purchase_date) AS sale_date,
        c.customer_state,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.total_value) AS total_revenue,
        SUM(f.total_freight) AS total_freight,
        AVG(f.total_value) AS avg_order_value,
        AVG(f.review_score) AS avg_review_score,
        COUNT(DISTINCT c.customer_unique_id) AS unique_customers
    FROM olist_warehouse.fact_orders_iceberg f
    JOIN olist_warehouse.dim_customers_iceberg c ON f.customer_sk = c.customer_sk
    WHERE f.order_status = 'delivered'
      AND c.is_current = TRUE
    GROUP BY DATE_TRUNC('day', f.order_purchase_date), c.customer_state
),
state_summary AS (
    SELECT 
        customer_state,
        SUM(total_revenue) AS total_revenue,
        SUM(order_count) AS total_orders,
        COUNT(DISTINCT sale_date) AS active_days,
        ROUND(AVG(avg_order_value), 2) AS avg_order_value,
        ROUND(AVG(avg_review_score), 2) AS avg_review_score,
        SUM(unique_customers) AS total_customers,
        SUM(CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '30' DAY THEN total_revenue ELSE 0 END) AS revenue_last_30d,
        SUM(CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '90' DAY THEN total_revenue ELSE 0 END) AS revenue_last_90d,
        SUM(CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '30' DAY THEN order_count ELSE 0 END) AS orders_last_30d,
        SUM(CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '90' DAY THEN order_count ELSE 0 END) AS orders_last_90d
    FROM daily_sales
    GROUP BY customer_state
),
national_stats AS (
    SELECT 
        SUM(total_revenue) AS national_revenue,
        AVG(avg_order_value) AS national_avg_order_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) AS median_state_revenue
    FROM state_summary
)
SELECT 
    ss.customer_state,
    ss.total_revenue,
    ROUND(ss.total_revenue / ns.national_revenue * 100, 2) AS pct_of_national_revenue,
    ss.total_orders,
    ss.total_customers,
    ROUND(ss.total_revenue / NULLIF(ss.total_orders, 0), 2) AS revenue_per_order,
    ss.avg_order_value,
    ROUND(ss.avg_order_value - ns.national_avg_order_value, 2) AS diff_from_national_avg,
    ss.avg_review_score,
    ROUND((ss.revenue_last_30d / NULLIF(ss.revenue_last_90d, 0) * 100), 2) AS revenue_retention_rate,
    ROUND(ss.revenue_last_30d / NULLIF(ss.total_revenue, 0) * 100, 2) AS revenue_last_30d_pct,
    ROW_NUMBER() OVER (ORDER BY ss.total_revenue DESC) AS revenue_rank,
    CASE 
        WHEN ss.total_revenue >= ns.median_state_revenue THEN 'Above Median'
        ELSE 'Below Median'
    END AS performance_tier
FROM state_summary ss
CROSS JOIN national_stats ns
ORDER BY ss.total_revenue DESC