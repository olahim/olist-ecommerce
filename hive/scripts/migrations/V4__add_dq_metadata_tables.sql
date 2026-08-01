-- =====================================================
-- Migration V4: Add Data Quality Metadata Tables
-- Version: 4.0
-- Description: Create tables for tracking data quality metrics
-- =====================================================

USE olist_quality;

-- =====================================================
-- DQ Score History Table
-- =====================================================
CREATE TABLE IF NOT EXISTS dq_score_history (
    dataset_name STRING COMMENT 'Name of the dataset',
    dq_date DATE COMMENT 'Date of the DQ check',
    dq_score DECIMAL(5,2) COMMENT 'Overall data quality score (0-100)',
    dq_score_pct DECIMAL(5,2) COMMENT 'DQ score as percentage',
    total_checks INT COMMENT 'Total number of checks performed',
    passed_checks INT COMMENT 'Number of checks that passed',
    failed_checks INT COMMENT 'Number of checks that failed',
    sla_threshold DECIMAL(5,2) COMMENT 'SLA threshold for this dataset',
    passed_sla BOOLEAN COMMENT 'Whether DQ score met SLA',
    severity_level STRING COMMENT 'Critical, High, Medium, Low',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL timestamp'
)
STORED AS PARQUET
LOCATION '/opt/hadoop/data/quality/dq_metrics/dq_score_history'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- =====================================================
-- DQ Check Results Table
-- =====================================================
CREATE TABLE IF NOT EXISTS dq_check_results (
    check_id STRING COMMENT 'Unique check identifier',
    dataset_name STRING COMMENT 'Dataset being checked',
    check_type STRING COMMENT 'null_check, duplicate_check, referential_integrity, range_check, format_check',
    check_date DATE COMMENT 'Date the check was run',
    column_name STRING COMMENT 'Column being validated',
    severity STRING COMMENT 'ERROR, WARNING, INFO',
    expected_value STRING COMMENT 'Expected result',
    actual_value STRING COMMENT 'Actual result',
    failed_count BIGINT COMMENT 'Number of records that failed',
    total_count BIGINT COMMENT 'Total records checked',
    passed BOOLEAN COMMENT 'Whether the check passed',
    error_message STRING COMMENT 'Error message if failed',
    execution_time_ms BIGINT COMMENT 'Check execution time in milliseconds',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL timestamp'
)
STORED AS PARQUET
LOCATION '/opt/hadoop/data/quality/dq_metrics/dq_check_results'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- =====================================================
-- Referential Integrity Violations Table
-- =====================================================
CREATE TABLE IF NOT EXISTS referential_integrity_violations (
    violation_id STRING COMMENT 'Unique violation identifier',
    foreign_key_table STRING COMMENT 'Table containing foreign key',
    foreign_key_column STRING COMMENT 'Foreign key column name',
    foreign_key_value STRING COMMENT 'The invalid foreign key value',
    primary_key_table STRING COMMENT 'Referenced primary key table',
    primary_key_column STRING COMMENT 'Referenced primary key column',
    detection_date DATE COMMENT 'Date violation was detected',
    violation_count INT COMMENT 'Number of times this violation occurred',
    sample_records ARRAY<STRING> COMMENT 'Sample records with this violation',
    resolved BOOLEAN COMMENT 'Whether violation has been resolved',
    resolution_date DATE COMMENT 'Date violation was resolved',
    resolution_notes STRING COMMENT 'Notes about resolution',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL timestamp'
)
STORED AS PARQUET
LOCATION '/opt/hadoop/data/quality/dq_metrics/fk_violations'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- =====================================================
-- DQ Alert History Table
-- =====================================================
CREATE TABLE IF NOT EXISTS dq_alert_history (
    alert_id STRING COMMENT 'Unique alert identifier',
    alert_type STRING COMMENT 'SLACK, EMAIL, PAGERDUTY',
    alert_severity STRING COMMENT 'CRITICAL, HIGH, MEDIUM, LOW',
    dataset_name STRING COMMENT 'Dataset that triggered the alert',
    alert_message STRING COMMENT 'Alert message content',
    alert_trigger VARCHAR(512) COMMENT 'What triggered the alert (e.g., dq_score < threshold)',
    threshold_value DECIMAL(5,2) COMMENT 'Threshold that was breached',
    actual_value DECIMAL(5,2) COMMENT 'Actual value that triggered alert',
    alert_sent_at TIMESTAMP COMMENT 'When alert was sent',
    acknowledged_by STRING COMMENT 'Person who acknowledged the alert',
    acknowledged_at TIMESTAMP COMMENT 'When alert was acknowledged',
    resolved_at TIMESTAMP COMMENT 'When issue was resolved',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL timestamp'
)
STORED AS PARQUET
LOCATION '/opt/hadoop/data/quality/dq_metrics/alert_history'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- =====================================================
-- Create indexes and constraints (where supported)
-- =====================================================

-- Create views for DQ reporting
CREATE OR REPLACE VIEW vw_dq_daily_summary AS
SELECT 
    dataset_name,
    dq_date,
    dq_score,
    dq_score_pct,
    total_checks,
    passed_checks,
    failed_checks,
    CASE 
        WHEN dq_score_pct >= 99 THEN 'EXCELLENT'
        WHEN dq_score_pct >= 95 THEN 'GOOD'
        WHEN dq_score_pct >= 90 THEN 'WARNING'
        WHEN dq_score_pct >= 80 THEN 'POOR'
        ELSE 'CRITICAL'
    END AS dq_status,
    ingestion_timestamp
FROM dq_score_history
WHERE dq_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY dq_date DESC, dataset_name;

-- Create view for recent failures
CREATE OR REPLACE VIEW vw_recent_dq_failures AS
SELECT 
    check_id,
    dataset_name,
    check_type,
    check_date,
    column_name,
    severity,
    failed_count,
    total_count,
    (failed_count / total_count) * 100 AS failure_pct,
    error_message,
    ingestion_timestamp
FROM dq_check_results
WHERE passed = FALSE
  AND check_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY check_date DESC, severity;

-- =====================================================
-- Insert migration record
-- =====================================================
INSERT INTO olist_warehouse.schema_migrations (version, description, applied_at, success)
VALUES ('V4', 'Add DQ metadata tables', CURRENT_TIMESTAMP(), TRUE);

SELECT 'Migration V4 completed successfully' AS status;