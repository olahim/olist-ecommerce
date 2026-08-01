-- =====================================================
-- View: Daily Data Quality Metrics
-- Description: Provides daily DQ scores and trends
-- =====================================================

USE olist_quality;

CREATE OR REPLACE VIEW vw_dq_daily_metrics AS
SELECT 
    dataset_name,
    dq_date,
    dq_score,
    dq_score_pct,
    total_checks,
    passed_checks,
    failed_checks,
    sla_threshold,
    passed_sla,
    severity_level,
    AVG(dq_score) OVER (
        PARTITION BY dataset_name 
        ORDER BY dq_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS dq_score_7d_avg,
    AVG(dq_score) OVER (
        PARTITION BY dataset_name 
        ORDER BY dq_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS dq_score_30d_avg,
    dq_score - LAG(dq_score) OVER (PARTITION BY dataset_name ORDER BY dq_date) AS dq_score_dod_change,
    ingestion_timestamp
FROM dq_score_history
WHERE dq_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY dq_date DESC, dataset_name;

CREATE OR REPLACE VIEW vw_dq_failure_summary AS
SELECT 
    dataset_name,
    check_type,
    column_name,
    severity,
    check_date,
    failed_count,
    total_count,
    (failed_count / total_count) * 100 AS failure_rate_pct,
    error_message,
    ROW_NUMBER() OVER (PARTITION BY dataset_name ORDER BY check_date DESC) AS recency_rank
FROM dq_check_results
WHERE passed = FALSE
  AND check_date >= CURRENT_DATE - INTERVAL '7' DAY;

CREATE OR REPLACE VIEW vw_fk_violation_summary AS
SELECT 
    foreign_key_table,
    foreign_key_column,
    foreign_key_value,
    primary_key_table,
    detection_date,
    violation_count,
    resolved,
    CASE 
        WHEN resolved = FALSE AND detection_date >= CURRENT_DATE - INTERVAL '7' DAY THEN 'ACTIVE'
        WHEN resolved = FALSE THEN 'PENDING'
        ELSE 'RESOLVED'
    END AS violation_status
FROM referential_integrity_violations
ORDER BY detection_date DESC;

SELECT 'Data quality views created successfully' AS status;