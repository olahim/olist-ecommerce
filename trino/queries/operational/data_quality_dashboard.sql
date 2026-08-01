-- =====================================================
-- Data Quality Dashboard
-- Description: Real-time data quality monitoring across all datasets
-- =====================================================

WITH dq_current_status AS (
    SELECT 
        dataset_name,
        dq_date,
        dq_score,
        total_checks,
        failed_checks,
        sla_threshold,
        passed_sla,
        CASE 
            WHEN dq_score >= 99 THEN 'Excellent'
            WHEN dq_score >= 95 THEN 'Good'
            WHEN dq_score >= 90 THEN 'Warning'
            WHEN dq_score >= 80 THEN 'Poor'
            ELSE 'Critical'
        END AS quality_grade
    FROM olist_quality.dq_score_history
    WHERE dq_date = CURRENT_DATE
),
dq_trend AS (
    SELECT 
        dataset_name,
        dq_date,
        dq_score,
        AVG(dq_score) OVER (PARTITION BY dataset_name ORDER BY dq_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dq_7d_avg,
        dq_score - LAG(dq_score) OVER (PARTITION BY dataset_name ORDER BY dq_date) AS day_over_day_change
    FROM olist_quality.dq_score_history
    WHERE dq_date >= CURRENT_DATE - INTERVAL '30' DAY
),
dq_failures AS (
    SELECT 
        dataset_name,
        check_type,
        column_name,
        severity,
        failed_count,
        total_count,
        ROUND(failed_count * 100.0 / total_count, 2) AS failure_rate_pct
    FROM olist_quality.dq_check_results
    WHERE check_date = CURRENT_DATE
      AND passed = FALSE
)
SELECT 
    'Current DQ Scores' AS metric_type,
    dataset_name,
    dq_score AS value,
    quality_grade,
    total_checks,
    failed_checks,
    sla_threshold,
    CASE WHEN passed_sla THEN 'PASS' ELSE 'FAIL' END AS sla_status,
    NULL AS check_type,
    NULL AS column_name,
    NULL AS failure_rate_pct
FROM dq_current_status

UNION ALL

SELECT 
    'DQ Trend' AS metric_type,
    dataset_name,
    dq_score AS value,
    NULL AS quality_grade,
    NULL AS total_checks,
    NULL AS failed_checks,
    NULL AS sla_threshold,
    NULL AS sla_status,
    NULL AS check_type,
    NULL AS column_name,
    NULL AS failure_rate_pct
FROM dq_trend
WHERE dq_date = CURRENT_DATE

UNION ALL

SELECT 
    'Failed Checks' AS metric_type,
    dataset_name,
    failed_count AS value,
    NULL AS quality_grade,
    NULL AS total_checks,
    NULL AS failed_checks,
    NULL AS sla_threshold,
    NULL AS sla_status,
    check_type,
    column_name,
    failure_rate_pct
FROM dq_failures
ORDER BY metric_type, dataset_name;
