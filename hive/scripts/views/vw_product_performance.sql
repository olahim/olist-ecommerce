-- =====================================================
-- View: Product Performance
-- Description: Aggregated product metrics for analysis
-- =====================================================

USE olist_warehouse;

CREATE OR REPLACE VIEW vw_product_performance AS
SELECT 
    p.product_id,
    p.product_category_name,
    COALESCE(p.product_category_name_english, 'Unknown') AS product_category_english,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.price) AS total_revenue,
    AVG(o.price) AS avg_price,
    SUM(o.quantity) AS total_units_sold,
    AVG(f.review_score) AS avg_review_score,
    COUNT(DISTINCT f.order_id) AS orders_with_reviews,
    SUM(o.price) OVER (PARTITION BY p.product_category_name) AS category_total_revenue,
    RANK() OVER (ORDER BY SUM(o.price) DESC) AS revenue_rank,
    SUM(CASE WHEN f.review_score = 5 THEN 1 ELSE 0 END) AS five_star_reviews,
    SUM(CASE WHEN f.review_score = 4 THEN 1 ELSE 0 END) AS four_star_reviews,
    SUM(CASE WHEN f.review_score = 3 THEN 1 ELSE 0 END) AS three_star_reviews,
    SUM(CASE WHEN f.review_score = 2 THEN 1 ELSE 0 END) AS two_star_reviews,
    SUM(CASE WHEN f.review_score = 1 THEN 1 ELSE 0 END) AS one_star_reviews,
    (SUM(CASE WHEN f.review_score >= 4 THEN 1 ELSE 0 END) - 
     SUM(CASE WHEN f.review_score <= 2 THEN 1 ELSE 0 END)) * 100.0 / COUNT(f.review_score) AS nps_score,
    p.ingestion_timestamp
FROM dim_products_iceberg p
LEFT JOIN fact_order_items_iceberg o ON p.product_id = o.product_id
LEFT JOIN fact_orders_iceberg f ON o.order_id = f.order_id
WHERE p.product_id IS NOT NULL
GROUP BY p.product_id, p.product_category_name, p.product_category_name_english, p.ingestion_timestamp;

SELECT 'View vw_product_performance created successfully' AS status;
