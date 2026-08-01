-- =====================================================
-- View: Order Summary
-- Description: Aggregated order metrics for reporting
-- =====================================================

USE olist_warehouse;

CREATE OR REPLACE VIEW vw_order_summary AS
SELECT 
    o.order_id,
    o.order_purchase_date,
    o.order_approved_date,
    o.order_delivered_date,
    o.order_estimated_date,
    o.order_status,
    o.total_value,
    o.total_freight,
    o.item_count,
    o.payment_value,
    o.payment_installments,
    o.payment_type,
    o.review_score,
    o.delivery_days,
    o.is_delayed,
    CASE 
        WHEN o.delivery_days <= 3 THEN 'Very Fast'
        WHEN o.delivery_days <= 7 THEN 'Fast'
        WHEN o.delivery_days <= 14 THEN 'Normal'
        WHEN o.delivery_days > 14 THEN 'Slow'
        ELSE 'Unknown'
    END AS delivery_category,
    CASE 
        WHEN o.payment_installments = 1 THEN 'Single Payment'
        WHEN o.payment_installments <= 3 THEN 'Short Term'
        WHEN o.payment_installments <= 6 THEN 'Medium Term'
        ELSE 'Long Term'
    END AS payment_term_category,
    c.customer_state,
    c.customer_segment,
    c.lifetime_tier,
    o.ingestion_timestamp
FROM fact_orders_iceberg o
JOIN vw_customer_summary c ON o.customer_sk = c.customer_sk
WHERE o.order_status = 'delivered';

SELECT 'View vw_order_summary created successfully' AS status;