-- =====================================================
-- Iceberg Manifest Rewrite Script
-- Description: Optimizes manifest file structure for better performance
-- =====================================================

-- Rewrite manifests for Customer Dimension
CALL spark_catalog.system.rewrite_manifests(
    table => 'olist_warehouse.dim_customers_iceberg',
    options => map(
        'target-compression-size-bytes', '134217728'  -- 128 MB
    )
);

-- Rewrite manifests for Product Dimension
CALL spark_catalog.system.rewrite_manifests(
    table => 'olist_warehouse.dim_products_iceberg',
    options => map('target-compression-size-bytes', '134217728')
);

-- Rewrite manifests for Seller Dimension
CALL spark_catalog.system.rewrite_manifests(
    table => 'olist_warehouse.dim_sellers_iceberg',
    options => map('target-compression-size-bytes', '134217728')
);

-- Rewrite manifests for Orders Fact Table
CALL spark_catalog.system.rewrite_manifests(
    table => 'olist_warehouse.fact_orders_iceberg',
    options => map('target-compression-size-bytes', '134217728')
);

-- Rewrite manifests for Order Items Fact Table
CALL spark_catalog.system.rewrite_manifests(
    table => 'olist_warehouse.fact_order_items_iceberg',
    options => map('target-compression-size-bytes', '134217728')
);

-- Rewrite manifests for Payments Fact Table
CALL spark_catalog.system.rewrite_manifests(
    table => 'olist_warehouse.fact_order_payments_iceberg',
    options => map('target-compression-size-bytes', '134217728')
);

-- Show manifest statistics
SELECT 
    table_name,
    manifest_count,
    total_entries_count
FROM spark_catalog.olist_warehouse.manifests
WHERE table_name LIKE 'fact_%'
ORDER BY manifest_count DESC;

SELECT 'Iceberg manifest rewrite completed successfully' AS status;