-- =====================================================
-- View: Customer Summary
-- Description: Aggregated customer metrics
-- =====================================================

USE olist_warehouse;

CREATE OR REPLACE VIEW vw_customer_summary AS
SELECT 
    customer_sk,
    customer_city,
    customer_state,
    first_order_date,
    last_order_date,
    total_orders,
    lifetime_value,
    customer_segment,
    DATEDIFF(CURRENT_DATE, last_order_date) AS days_since_last_order,
    CASE 
        WHEN total_orders > 0 THEN lifetime_value / total_orders 
        ELSE 0 
    END AS avg_order_value,
    CASE 
        WHEN lifetime_value >= 10000 THEN 'PLATINUM'
        WHEN lifetime_value >= 5000 THEN 'GOLD'
        WHEN lifetime_value >= 1000 THEN 'SILVER'
        ELSE 'BRONZE'
    END AS lifetime_tier,
    is_current
FROM dim_customers_iceberg
WHERE is_current = TRUE;

SELECT 'View vw_customer_summary created successfully' AS status;