-- =====================================================
-- Iceberg Snapshot Expiration Script
-- Description: Removes old snapshots to manage storage
-- =====================================================

-- Expire snapshots older than 7 days for Customer Dimension
CALL spark_catalog.system.expire_snapshots(
    table => 'olist_warehouse.dim_customers_iceberg',
    older_than => TIMESTAMP '{{ (now() - 7 days) }}',
    retain_last => 5
);

-- Expire snapshots for Product Dimension
CALL spark_catalog.system.expire_snapshots(
    table => 'olist_warehouse.dim_products_iceberg',
    older_than => TIMESTAMP '{{ (now() - 7 days) }}',
    retain_last => 5
);

-- Expire snapshots for Seller Dimension
CALL spark_catalog.system.expire_snapshots(
    table => 'olist_warehouse.dim_sellers_iceberg',
    older_than => TIMESTAMP '{{ (now() - 7 days) }}',
    retain_last => 5
);

-- Expire snapshots for Orders Fact Table (keep 14 days)
CALL spark_catalog.system.expire_snapshots(
    table => 'olist_warehouse.fact_orders_iceberg',
    older_than => TIMESTAMP '{{ (now() - 14 days) }}',
    retain_last => 10
);

-- Expire snapshots for Order Items Fact Table
CALL spark_catalog.system.expire_snapshots(
    table => 'olist_warehouse.fact_order_items_iceberg',
    older_than => TIMESTAMP '{{ (now() - 7 days) }}',
    retain_last => 5
);

-- Expire snapshots for Payments Fact Table
CALL spark_catalog.system.expire_snapshots(
    table => 'olist_warehouse.fact_order_payments_iceberg',
    older_than => TIMESTAMP '{{ (now() - 7 days) }}',
    retain_last => 5
);

-- Show snapshot count after expiration (for verification)
SHOW SNAPSHOTS olist_warehouse.fact_orders_iceberg;

SELECT 'Iceberg snapshot expiration completed successfully' AS status;