-- =====================================================
-- Migration V3: Add Partitions
-- Version: 3.0
-- Description: Add partitions for better query performance
-- =====================================================

USE olist_raw;

-- =====================================================
-- Add partitions to raw tables
-- =====================================================

-- Add partitions for customers table (example for 2024 data)
ALTER TABLE customers ADD IF NOT EXISTS 
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='01')
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='02')
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='03');

-- Add partitions for orders table
ALTER TABLE orders ADD IF NOT EXISTS 
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='01')
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='02')
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='03');

-- Add partitions for products table
ALTER TABLE products ADD IF NOT EXISTS 
PARTITION (ingestion_year='2024', ingestion_month='01', ingestion_day='01');

-- =====================================================
-- Add partitions for warehouse Iceberg tables
-- =====================================================

-- Add partition metadata for fact_orders
ALTER TABLE olist_warehouse.fact_orders_iceberg 
SET TBLPROPERTIES (
    'write.partition-evolution.enabled' = 'true',
    'write.partition-evolution.partition-order' = 'order_purchase_date'
);

-- =====================================================
-- Create partition maintenance script
-- =====================================================

-- Create a table to track partition loads
CREATE TABLE IF NOT EXISTS olist_warehouse.partition_tracking (
    table_name STRING,
    partition_value STRING,
    load_date DATE,
    record_count BIGINT,
    status STRING
)
STORED AS PARQUET;

-- =====================================================
-- Insert migration record
-- =====================================================
INSERT INTO olist_warehouse.schema_migrations (version, description, applied_at, success)
VALUES ('V3', 'Add partitions', CURRENT_TIMESTAMP(), TRUE);

SELECT 'Migration V3 completed successfully' AS status;