-- =====================================================
-- Migration V2: Add Iceberg Support
-- Version: 2.0
-- Description: Configure Iceberg table properties
-- =====================================================

USE olist_warehouse;

-- =====================================================
-- Update Iceberg table properties for better performance
-- =====================================================

-- Set table properties for customer dimension
ALTER TABLE dim_customers_iceberg 
SET TBLPROPERTIES (
    'write.metadata.metrics.default' = 'full',
    'write.metadata.metrics.column.customer_id' = 'truncate(16)',
    'write.metadata.metrics.column.customer_state' = 'truncate(2)'
);

-- Set table properties for orders fact
ALTER TABLE fact_orders_iceberg 
SET TBLPROPERTIES (
    'write.metadata.metrics.default' = 'full',
    'write.metadata.metrics.column.order_status' = 'truncate(16)',
    'write.target-file-size-bytes' = '536870912'
);

-- Set table properties for product dimension
ALTER TABLE dim_products_iceberg 
SET TBLPROPERTIES (
    'write.metadata.metrics.column.product_category_name' = 'truncate(32)'
);

-- =====================================================
-- Create Iceberg partition evolution
-- =====================================================

-- Enable partition evolution for fact tables
ALTER TABLE fact_orders_iceberg 
SET TBLPROPERTIES ('write.partition-evolution.enabled' = 'true');

-- =====================================================
-- Create snapshots for Time Travel
-- =====================================================

-- Create initial snapshot for customers
CALL spark_catalog.system.create_snapshot(
    table => 'olist_warehouse.dim_customers_iceberg',
    comment => 'Initial snapshot - migration V2'
);

-- Create initial snapshot for orders
CALL spark_catalog.system.create_snapshot(
    table => 'olist_warehouse.fact_orders_iceberg',
    comment => 'Initial snapshot - migration V2'
);

-- =====================================================
-- Insert migration record
-- =====================================================
INSERT INTO olist_warehouse.schema_migrations (version, description, applied_at, success)
VALUES ('V2', 'Add Iceberg support', CURRENT_TIMESTAMP(), TRUE);

SELECT 'Migration V2 completed successfully' AS status;