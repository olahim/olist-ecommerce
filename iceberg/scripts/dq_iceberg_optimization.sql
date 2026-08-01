-- =====================================================
-- Data Quality-Aware Iceberg Optimization
-- Description: Optimizes tables with DQ metadata
-- =====================================================

-- Add DQ metadata columns to dimension tables
ALTER TABLE olist_warehouse.dim_customers_iceberg 
ADD COLUMNS (
    dq_last_checked TIMESTAMP COMMENT 'Last data quality check timestamp',
    dq_score DECIMAL(5,2) COMMENT 'Data quality score for this record'
);

ALTER TABLE olist_warehouse.dim_products_iceberg 
ADD COLUMNS (
    dq_last_checked TIMESTAMP,
    dq_score DECIMAL(5,2)
);

ALTER TABLE olist_warehouse.dim_sellers_iceberg 
ADD COLUMNS (
    dq_last_checked TIMESTAMP,
    dq_score DECIMAL(5,2)
);

-- Create DQ metadata table to track table-level quality
CREATE TABLE IF NOT EXISTS olist_warehouse.table_dq_metadata (
    table_name STRING,
    dq_check_date DATE,
    total_records BIGINT,
    records_with_issues BIGINT,
    dq_score DECIMAL(5,2),
    issue_summary MAP<STRING, BIGINT>,
    last_optimized TIMESTAMP
)
USING ICEBERG
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.partition-evolution.enabled' = 'true'
)
PARTITIONED BY (dq_check_date);

-- Create a view that excludes DQ-failed records for reporting
CREATE OR REPLACE VIEW olist_warehouse.vw_dq_valid_customers AS
SELECT *
FROM olist_warehouse.dim_customers_iceberg
WHERE dq_score >= 95 OR dq_score IS NULL
  AND is_current = TRUE;

-- Optimize tables with DQ priority (prioritize clean partitions)
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.dim_customers_iceberg',
    options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '536870912',
        'where', 'dq_score >= 95 OR dq_score IS NULL'
    )
);

-- Create DQ monitoring table for Iceberg metrics
CREATE OR REPLACE VIEW olist_warehouse.vw_iceberg_dq_metrics AS
SELECT 
    table_name,
    snapshot_id,
    timestamp,
    total_data_files,
    total_data_size_bytes,
    total_position_deletes,
    total_eq_deletes,
    DQ_metadata
FROM spark_catalog.olist_warehouse.snapshots
WHERE table_name IN ('dim_customers_iceberg', 'dim_products_iceberg', 'fact_orders_iceberg')
ORDER BY timestamp DESC;

SELECT 'DQ-aware Iceberg optimization completed' AS status;