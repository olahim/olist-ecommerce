-- =====================================================
-- Order Status Tracking
-- Description: Real-time order status distribution and trends
-- =====================================================

WITH order_status_daily AS (
    SELECT 
        DATE_TRUNC('day', order_purchase_date) AS order_date,
        order_status,
        COUNT(*) AS order_count,
        SUM(total_value) AS total_value
    FROM olist_warehouse.fact_orders_iceberg
    WHERE order_purchase_date >= DATE '2024-01-01'
    GROUP BY DATE_TRUNC('day', order_purchase_date), order_status
),
status_summary AS (
    SELECT 
        order_status,
        SUM(order_count) AS total_orders,
        SUM(total_value) AS total_value,
        COUNT(DISTINCT order_date) AS active_days,
        AVG(order_count) AS avg_daily_orders,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '7' DAY THEN order_count ELSE 0 END) AS orders_last_7d,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '30' DAY THEN order_count ELSE 0 END) AS orders_last_30d
    FROM order_status_daily
    GROUP BY order_status
)
SELECT 
    order_status,
    total_orders,
    ROUND(total_orders * 100.0 / SUM(total_orders) OVER (), 2) AS pct_of_total_orders,
    total_value,
    ROUND(total_value * 100.0 / SUM(total_value) OVER (), 2) AS pct_of_total_value,
    avg_daily_orders,
    orders_last_7d,
    orders_last_30d,
    ROUND(orders_last_7d * 100.0 / NULLIF(orders_last_30d, 0), 2) AS orders_7d_vs_30d_ratio,
    CASE 
        WHEN order_status IN ('delivered', 'shipped') THEN 'Completed'
        WHEN order_status IN ('processing', 'approved') THEN 'In Progress'
        WHEN order_status IN ('canceled', 'unavailable') THEN 'Failed'
        ELSE 'Other'
    END AS status_category
FROM status_summary
ORDER BY total_orders DESC