-- =====================================================
-- Iceberg Table Compaction Script
-- Description: Merges small files into optimal-sized files
-- =====================================================

-- Compaction for Customer Dimension
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.dim_customers_iceberg',
    strategy => 'binpack',
    options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '536870912',  -- 512 MB
        'rewrite-all', 'false'
    )
);

-- Compaction for Product Dimension
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.dim_products_iceberg',
    strategy => 'binpack',
    options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '536870912'
    )
);

-- Compaction for Seller Dimension
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.dim_sellers_iceberg',
    strategy => 'binpack',
    options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '536870912'
    )
);

-- Compaction for Orders Fact Table
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.fact_orders_iceberg',
    strategy => 'binpack',
    options => map(
        'min-input-files', '10',
        'target-file-size-bytes', '536870912'
    )
);

-- Compaction for Order Items Fact Table
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.fact_order_items_iceberg',
    strategy => 'binpack',
    options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '536870912'
    )
);

-- Compaction for Payments Fact Table
CALL spark_catalog.system.rewrite_data_files(
    table => 'olist_warehouse.fact_order_payments_iceberg',
    strategy => 'binpack',
    options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '536870912'
    )
);

SELECT 'Iceberg table compaction completed successfully' AS status;