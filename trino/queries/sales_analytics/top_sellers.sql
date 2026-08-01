-- =====================================================
-- Top Sellers Analysis
-- Description: Best-performing sellers by revenue and customer satisfaction
-- =====================================================

WITH seller_performance AS (
    SELECT 
        s.seller_id,
        s.seller_state,
        s.seller_city,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.price) AS total_revenue,
        SUM(o.freight_value) AS total_freight_collected,
        AVG(o.price) AS avg_item_price,
        COUNT(DISTINCT o.product_id) AS unique_products_sold,
        COUNT(DISTINCT f.customer_sk) AS unique_customers,
        AVG(f.review_score) AS avg_review_score,
        AVG(f.delivery_days) AS avg_delivery_days,
        SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END) AS delayed_orders,
        SUM(CASE WHEN f.order_purchase_date >= CURRENT_DATE - INTERVAL '30' DAY THEN o.price ELSE 0 END) AS revenue_last_30d,
        COUNT(CASE WHEN f.order_purchase_date >= CURRENT_DATE - INTERVAL '30' DAY THEN 1 END) AS orders_last_30d
    FROM olist_warehouse.dim_sellers_iceberg s
    JOIN olist_warehouse.fact_order_items_iceberg o ON s.seller_id = o.seller_id
    JOIN olist_warehouse.fact_orders_iceberg f ON o.order_id = f.order_id
    WHERE f.order_status = 'delivered'
    GROUP BY s.seller_id, s.seller_state, s.seller_city
),
seller_metrics AS (
    SELECT 
        *,
        ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS revenue_per_order,
        ROUND(100.0 * delayed_orders / NULLIF(total_orders, 0), 2) AS delayed_percentage,
        ROUND(avg_review_score * 20, 1) AS satisfaction_score,
        CASE 
            WHEN total_revenue >= PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_revenue) OVER () THEN 'Platinum'
            WHEN total_revenue >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_revenue) OVER () THEN 'Gold'
            WHEN total_revenue >= PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_revenue) OVER () THEN 'Silver'
            ELSE 'Bronze'
        END AS seller_tier,
        CASE 
            WHEN revenue_last_30d / NULLIF(total_revenue, 0) > 0.3 THEN 'High Growth'
            WHEN revenue_last_30d / NULLIF(total_revenue, 0) > 0.15 THEN 'Steady Growth'
            ELSE 'Mature'
        END AS growth_status
    FROM seller_performance
),
state_stats AS (
    SELECT 
        seller_state,
        COUNT(DISTINCT seller_id) AS seller_count,
        SUM(total_revenue) AS state_revenue,
        AVG(avg_review_score) AS state_avg_review,
        AVG(avg_delivery_days) AS state_avg_delivery
    FROM seller_metrics
    GROUP BY seller_state
)
SELECT 
    sm.seller_id,
    sm.seller_state,
    sm.seller_city,
    sm.seller_tier,
    sm.total_revenue,
    ROUND(sm.total_revenue / ss.state_revenue * 100, 2) AS pct_of_state_revenue,
    sm.total_orders,
    sm.unique_products_sold,
    sm.unique_customers,
    sm.avg_item_price,
    sm.revenue_per_order,
    sm.avg_review_score,
    sm.satisfaction_score,
    sm.avg_delivery_days,
    sm.delayed_percentage,
    sm.growth_status,
    ROUND(sm.avg_review_score - ss.state_avg_review, 2) AS review_diff_from_state,
    ROUND(sm.avg_delivery_days - ss.state_avg_delivery, 1) AS delivery_diff_from_state,
    ROW_NUMBER() OVER (PARTITION BY sm.seller_state ORDER BY sm.total_revenue DESC) AS rank_in_state,
    ROW_NUMBER() OVER (ORDER BY sm.total_revenue DESC) AS national_rank
FROM seller_metrics sm
JOIN state_stats ss ON sm.seller_state = ss.seller_state
WHERE sm.total_orders >= 10
ORDER BY sm.total_revenue DESC
LIMIT 200