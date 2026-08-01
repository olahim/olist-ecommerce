-- =====================================================
-- Customer Segmentation Analysis
-- Description: RFM-based customer segmentation
-- =====================================================

WITH customer_rfm AS (
    SELECT 
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment AS current_segment,
        c.total_orders AS frequency,
        c.lifetime_value AS monetary,
        DATEDIFF('day', c.last_order_date, CURRENT_DATE) AS recency,
        NTILE(5) OVER (ORDER BY DATEDIFF('day', c.last_order_date, CURRENT_DATE) DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY c.total_orders) AS frequency_score,
        NTILE(5) OVER (ORDER BY c.lifetime_value) AS monetary_score,
        c.first_order_date,
        c.last_order_date,
        AVG(f.review_score) OVER (PARTITION BY c.customer_unique_id) AS avg_review_score,
        COUNT(f.order_id) OVER (PARTITION BY c.customer_unique_id) AS total_orders_count
    FROM olist_warehouse.dim_customers_iceberg c
    LEFT JOIN olist_warehouse.fact_orders_iceberg f ON c.customer_sk = f.customer_sk
    WHERE c.is_current = TRUE
      AND f.order_purchase_date >= DATE '2024-01-01'
),
rfm_scores AS (
    SELECT 
        *,
        recency_score + frequency_score + monetary_score AS rfm_total,
        CONCAT(CAST(recency_score AS VARCHAR), CAST(frequency_score AS VARCHAR), CAST(monetary_score AS VARCHAR)) AS rfm_segment_code,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Potential Loyalists'
            WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
            WHEN recency_score >= 3 AND frequency_score <= 2 THEN 'Promising'
            ELSE 'Regular'
        END AS rfm_segment
    FROM customer_rfm
)
SELECT 
    rfm_segment,
    COUNT(DISTINCT customer_unique_id) AS customer_count,
    ROUND(COUNT(DISTINCT customer_unique_id) * 100.0 / SUM(COUNT(DISTINCT customer_unique_id)) OVER (), 2) AS pct_of_customers,
    ROUND(AVG(monetary), 2) AS avg_monetary,
    ROUND(AVG(frequency), 1) AS avg_frequency,
    ROUND(AVG(recency), 0) AS avg_recency_days,
    ROUND(AVG(avg_review_score), 2) AS avg_review_score,
    SUM(CASE WHEN current_segment != rfm_segment THEN 1 ELSE 0 END) AS segment_changed,
    COUNT(DISTINCT customer_state) AS states_covered,
    ROUND(SUM(monetary), 2) AS total_monetary_contribution,
    ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2) AS pct_of_total_value
FROM rfm_scores
GROUP BY rfm_segment
ORDER BY 
    CASE rfm_segment
        WHEN 'Champions' THEN 1
        WHEN 'Loyal Customers' THEN 2
        WHEN 'Potential Loyalists' THEN 3
        WHEN 'Promising' THEN 4
        WHEN 'New Customers' THEN 5
        WHEN 'At Risk' THEN 6
        WHEN 'Regular' THEN 7
        WHEN 'Lost' THEN 8
        ELSE 9
    END