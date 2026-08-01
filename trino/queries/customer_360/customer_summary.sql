-- =====================================================
-- Customer Summary Dashboard Query
-- Description: Aggregated customer metrics for executive dashboard
-- =====================================================

WITH customer_metrics AS (
    SELECT 
        c.customer_state,
        c.customer_city,
        c.customer_segment,
        COUNT(DISTINCT c.customer_sk) AS total_customers,
        SUM(c.lifetime_value) AS total_lifetime_value,
        AVG(c.lifetime_value) AS avg_lifetime_value,
        AVG(c.total_orders) AS avg_orders_per_customer,
        SUM(CASE WHEN c.customer_segment = 'VIP' THEN 1 ELSE 0 END) AS vip_customers,
        SUM(CASE WHEN c.customer_segment = 'Regular' THEN 1 ELSE 0 END) AS regular_customers,
        SUM(CASE WHEN c.customer_segment = 'New' THEN 1 ELSE 0 END) AS new_customers,
        SUM(CASE WHEN c.customer_segment = 'At Risk' THEN 1 ELSE 0 END) AS at_risk_customers,
        SUM(CASE WHEN c.total_orders >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
        SUM(CASE WHEN c.total_orders >= 5 THEN 1 ELSE 0 END) AS loyal_customers,
        AVG(DATEDIFF('day', c.last_order_date, CURRENT_DATE)) AS avg_days_since_last_order
    FROM olist_warehouse.dim_customers_iceberg c
    WHERE c.is_current = TRUE
    GROUP BY c.customer_state, c.customer_city, c.customer_segment
),
national_totals AS (
    SELECT 
        SUM(total_customers) AS national_customers,
        SUM(total_lifetime_value) AS national_ltv,
        AVG(avg_lifetime_value) AS national_avg_ltv
    FROM customer_metrics
)
SELECT 
    cm.customer_state,
    cm.customer_city,
    cm.customer_segment,
    cm.total_customers,
    ROUND(cm.total_lifetime_value / cm.total_customers, 2) AS avg_customer_value,
    cm.total_lifetime_value,
    ROUND(cm.total_lifetime_value / nt.national_ltv * 100, 2) AS pct_of_national_ltv,
    cm.avg_orders_per_customer,
    cm.vip_customers,
    cm.regular_customers,
    cm.new_customers,
    cm.at_risk_customers,
    ROUND(cm.repeat_customers * 100.0 / cm.total_customers, 2) AS repeat_customer_rate,
    ROUND(cm.loyal_customers * 100.0 / cm.total_customers, 2) AS loyal_customer_rate,
    cm.avg_days_since_last_order,
    nt.national_customers,
    ROUND(nt.national_avg_ltv, 2) AS national_avg_ltv
FROM customer_metrics cm
CROSS JOIN national_totals nt
ORDER BY cm.total_lifetime_value DESC
LIMIT 1000