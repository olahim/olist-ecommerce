-- =====================================================
-- Delivery Performance Analysis
-- Description: Detailed delivery metrics and SLA tracking
-- =====================================================

WITH delivery_metrics AS (
    SELECT 
        order_id,
        customer_state,
        order_purchase_date,
        order_delivered_date,
        order_estimated_date,
        delivery_days,
        is_delayed,
        review_score,
        CASE 
            WHEN delivery_days <= 3 THEN 'Very Fast (0-3 days)'
            WHEN delivery_days <= 7 THEN 'Fast (4-7 days)'
            WHEN delivery_days <= 14 THEN 'Normal (8-14 days)'
            WHEN delivery_days <= 21 THEN 'Slow (15-21 days)'
            ELSE 'Very Slow (>21 days)'
        END AS delivery_speed_bucket,
        CASE 
            WHEN is_delayed AND delivery_days > 30 THEN 'Severely Delayed (>30 days)'
            WHEN is_delayed THEN 'Delayed'
            ELSE 'On Time'
        END AS delivery_status
    FROM olist_warehouse.fact_orders_iceberg
    WHERE order_status = 'delivered'
      AND order_purchase_date >= DATE '2024-01-01'
),
daily_performance AS (
    SELECT 
        DATE_TRUNC('day', order_purchase_date) AS delivery_date,
        COUNT(*) AS total_deliveries,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_deliveries,
        AVG(delivery_days) AS avg_delivery_days,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_days) AS median_delivery_days,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delivery_days) AS p95_delivery_days,
        AVG(CASE WHEN review_score IS NOT NULL THEN review_score END) AS avg_review_score_for_deliveries
    FROM delivery_metrics
    GROUP BY DATE_TRUNC('day', order_purchase_date)
),
state_performance AS (
    SELECT 
        customer_state,
        COUNT(*) AS total_deliveries,
        ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_days), 1) AS median_delivery_days,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delivery_days), 1) AS p95_delivery_days,
        ROUND(SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delayed_percentage,
        ROUND(AVG(CASE WHEN review_score IS NOT NULL THEN review_score END), 2) AS avg_review_score,
        COUNT(DISTINCT CASE WHEN is_delayed THEN order_id END) AS delayed_orders_count
    FROM delivery_metrics
    GROUP BY customer_state
)
SELECT 
    delivery_date,
    total_deliveries,
    delayed_deliveries,
    ROUND(delayed_deliveries * 100.0 / NULLIF(total_deliveries, 0), 2) AS delayed_percentage,
    ROUND(avg_delivery_days, 1) AS avg_delivery_days,
    ROUND(median_delivery_days, 1) AS median_delivery_days,
    ROUND(p95_delivery_days, 1) AS p95_delivery_days,
    ROUND(avg_review_score_for_deliveries, 2) AS avg_review_score,
    -- 7-day moving average
    ROUND(AVG(avg_delivery_days) OVER (ORDER BY delivery_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_delivery_7d_ma,
    ROUND(AVG(delayed_deliveries * 100.0 / NULLIF(total_deliveries, 0)) OVER (ORDER BY delivery_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS delayed_pct_7d_ma
FROM daily_performance
ORDER BY delivery_date DESC
LIMIT 1000;

-- State-level performance summary
SELECT 
    customer_state,
    total_deliveries,
    avg_delivery_days,
    median_delivery_days,
    p95_delivery_days,
    delayed_percentage,
    avg_review_score,
    CASE 
        WHEN delayed_percentage <= 5 THEN 'Excellent'
        WHEN delayed_percentage <= 10 THEN 'Good'
        WHEN delayed_percentage <= 20 THEN 'Fair'
        ELSE 'Needs Improvement'
    END AS delivery_performance_rating,
    delayed_orders_count
FROM state_performance
ORDER BY delayed_percentage ASC, avg_delivery_days ASC