-- =====================================================
-- Iceberg Migration to Version 2
-- Description: Migrates tables to Iceberg format version 2
-- Version 2 supports row-level deletes and updates
-- =====================================================

-- Check current versions
SELECT 
    table_name,
    table_version
FROM spark_catalog.olist_warehouse.tables
WHERE table_name LIKE '%iceberg';

-- Migrate Customer Dimension to V2
ALTER TABLE olist_warehouse.dim_customers_iceberg 
SET TBLPROPERTIES ('format-version' = '2');

-- Migrate Product Dimension to V2
ALTER TABLE olist_warehouse.dim_products_iceberg 
SET TBLPROPERTIES ('format-version' = '2');

-- Migrate Seller Dimension to V2
ALTER TABLE olist_warehouse.dim_sellers_iceberg 
SET TBLPROPERTIES ('format-version' = '2');

-- Migrate Orders Fact Table to V2
ALTER TABLE olist_warehouse.fact_orders_iceberg 
SET TBLPROPERTIES ('format-version' = '2');

-- Migrate Order Items Fact Table to V2
ALTER TABLE olist_warehouse.fact_order_items_iceberg 
SET TBLPROPERTIES ('format-version' = '2');

-- Migrate Payments Fact Table to V2
ALTER TABLE olist_warehouse.fact_order_payments_iceberg 
SET TBLPROPERTIES ('format-version' = '2');

-- Verify migration
SELECT 
    table_name,
    table_version
FROM spark_catalog.olist_warehouse.tables
WHERE table_name LIKE '%iceberg';

SELECT 'All Iceberg tables migrated to version 2 successfully' AS status;