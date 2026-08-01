-- =====================================================
-- Top Products Analysis
-- Description: Best-selling products by revenue and quantity
-- =====================================================

WITH product_sales AS (
    SELECT 
        p.product_id,
        COALESCE(p.product_category_name_english, p.product_category_name, 'Unknown') AS product_category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.price) AS total_revenue,
        SUM(o.quantity) AS total_units_sold,
        AVG(o.price) AS avg_price,
        AVG(f.review_score) AS avg_review_score,
        COUNT(DISTINCT f.customer_sk) AS unique_customers,
        SUM(CASE WHEN f.order_purchase_date >= CURRENT_DATE - INTERVAL '30' DAY THEN o.price ELSE 0 END) AS revenue_last_30d,
        SUM(CASE WHEN f.order_purchase_date >= CURRENT_DATE - INTERVAL '30' DAY THEN o.quantity ELSE 0 END) AS units_last_30d
    FROM olist_warehouse.dim_products_iceberg p
    JOIN olist_warehouse.fact_order_items_iceberg o ON p.product_id = o.product_id
    JOIN olist_warehouse.fact_orders_iceberg f ON o.order_id = f.order_id
    WHERE f.order_status = 'delivered'
    GROUP BY p.product_id, p.product_category_name_english, p.product_category_name
),
category_totals AS (
    SELECT 
        product_category,
        SUM(total_revenue) AS category_revenue,
        SUM(total_units_sold) AS category_units,
        AVG(avg_review_score) AS category_avg_review
    FROM product_sales
    GROUP BY product_category
)
SELECT 
    ps.product_category,
    ps.product_id,
    ps.total_revenue,
    ROUND(ps.total_revenue / ct.category_revenue * 100, 2) AS pct_of_category,
    ps.total_orders,
    ps.total_units_sold,
    ps.avg_price,
    ROUND(ps.total_revenue / NULLIF(ps.total_units_sold, 0), 2) AS revenue_per_unit,
    ps.avg_review_score,
    ROUND(ps.avg_review_score - ct.category_avg_review, 2) AS review_diff_from_category,
    ps.unique_customers,
    ROUND(ps.revenue_last_30d / NULLIF(ps.total_revenue, 0) * 100, 2) AS revenue_last_30d_pct,
    ps.units_last_30d,
    ROW_NUMBER() OVER (ORDER BY ps.total_revenue DESC) AS revenue_rank,
    ROW_NUMBER() OVER (ORDER BY ps.total_units_sold DESC) AS volume_rank,
    ROW_NUMBER() OVER (ORDER BY ps.avg_review_score DESC) AS rating_rank,
    CASE 
        WHEN ps.total_revenue >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ps.total_revenue) OVER () THEN 'Star Product'
        WHEN ps.total_revenue >= PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ps.total_revenue) OVER () THEN 'Core Product'
        WHEN ps.total_revenue >= PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ps.total_revenue) OVER () THEN 'Niche Product'
        ELSE 'Tail Product'
    END AS product_tier
FROM product_sales ps
JOIN category_totals ct ON ps.product_category = ct.product_category
ORDER BY ps.total_revenue DESC
LIMIT 500