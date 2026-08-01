-- =====================================================
-- Apache Iceberg Tables for Olist Star Schema
-- This script creates Iceberg tables for the warehouse layer
-- =====================================================

-- Create database for Iceberg tables
CREATE DATABASE IF NOT EXISTS olist_warehouse
COMMENT 'Olist data warehouse - Iceberg tables'
LOCATION '/opt/hadoop/data/warehouse/iceberg';

USE olist_warehouse;

-- =====================================================
-- Customer Dimension Table (SCD Type 2)
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_customers_iceberg (
    customer_sk BIGINT COMMENT 'Surrogate key (SCD Type 2)',
    customer_id STRING COMMENT 'Natural key (per order)',
    customer_unique_id STRING COMMENT 'Unique customer identifier',
    customer_zip_code_prefix STRING COMMENT 'Customer zip code (first 3 digits)',
    customer_city STRING COMMENT 'Customer city',
    customer_state STRING COMMENT 'Customer state (2-letter code)',
    first_order_date DATE COMMENT 'Date of first order',
    last_order_date DATE COMMENT 'Date of most recent order',
    total_orders INT COMMENT 'Total number of orders',
    lifetime_value DECIMAL(15,2) COMMENT 'Total customer lifetime value',
    customer_segment STRING COMMENT 'Customer segment (VIP, Regular, New, At Risk)',
    start_date DATE COMMENT 'SCD Type 2 - record start date',
    end_date DATE COMMENT 'SCD Type 2 - record end date (null if current)',
    is_current BOOLEAN COMMENT 'True for current record',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Customer dimension table with SCD Type 2 tracking'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);

-- =====================================================
-- Product Dimension Table
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_products_iceberg (
    product_id STRING COMMENT 'Product unique identifier',
    product_category_name STRING COMMENT 'Product category (Portuguese)',
    product_category_name_english STRING COMMENT 'Product category (English translation)',
    product_name_length INT COMMENT 'Length of product name',
    product_description_length INT COMMENT 'Length of product description',
    product_photos_qty INT COMMENT 'Number of product photos',
    product_weight_g INT COMMENT 'Product weight in grams',
    product_length_cm INT COMMENT 'Product length in cm',
    product_height_cm INT COMMENT 'Product height in cm',
    product_width_cm INT COMMENT 'Product width in cm',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Product dimension table'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- =====================================================
-- Seller Dimension Table
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_sellers_iceberg (
    seller_id STRING COMMENT 'Seller unique identifier',
    seller_zip_code_prefix STRING COMMENT 'Seller zip code (first 3 digits)',
    seller_city STRING COMMENT 'Seller city',
    seller_state STRING COMMENT 'Seller state (2-letter code)',
    total_sales DECIMAL(15,2) COMMENT 'Total sales amount',
    total_orders INT COMMENT 'Total number of orders',
    avg_review_score DECIMAL(3,2) COMMENT 'Average seller rating',
    avg_fulfillment_days INT COMMENT 'Average days to fulfill orders',
    is_active BOOLEAN COMMENT 'Whether seller is currently active',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Seller dimension table'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- =====================================================
-- Orders Fact Table
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_orders_iceberg (
    order_id STRING COMMENT 'Order unique identifier',
    customer_sk BIGINT COMMENT 'Foreign key to dim_customers',
    order_purchase_date DATE COMMENT 'Date of purchase',
    order_approved_date DATE COMMENT 'Date order was approved',
    order_delivered_date DATE COMMENT 'Date order was delivered',
    order_estimated_date DATE COMMENT 'Estimated delivery date',
    order_status STRING COMMENT 'Order status',
    total_value DECIMAL(15,2) COMMENT 'Total order value',
    total_freight DECIMAL(15,2) COMMENT 'Total freight cost',
    item_count INT COMMENT 'Number of items in order',
    payment_value DECIMAL(15,2) COMMENT 'Payment amount',
    payment_installments INT COMMENT 'Number of payment installments',
    payment_type STRING COMMENT 'Payment method',
    review_score INT COMMENT 'Customer review score (1-5)',
    delivery_days INT COMMENT 'Days from purchase to delivery',
    is_delayed BOOLEAN COMMENT 'Whether delivery was delayed',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Orders fact table'
PARTITIONED BY (order_purchase_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.partition-evolution.enabled' = 'true'
);

-- =====================================================
-- Order Items Fact Table
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_order_items_iceberg (
    order_item_id STRING COMMENT 'Order line item unique identifier',
    order_id STRING COMMENT 'Foreign key to fact_orders',
    product_id STRING COMMENT 'Foreign key to dim_products',
    seller_id STRING COMMENT 'Foreign key to dim_sellers',
    price DECIMAL(10,2) COMMENT 'Item price',
    freight_value DECIMAL(10,2) COMMENT 'Item freight cost',
    quantity INT COMMENT 'Item quantity',
    total_line_value DECIMAL(15,2) COMMENT 'Line total (price * quantity)',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Order items fact table'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- =====================================================
-- Payments Fact Table
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_order_payments_iceberg (
    order_id STRING COMMENT 'Foreign key to fact_orders',
    payment_sequential INT COMMENT 'Payment sequence number',
    payment_type STRING COMMENT 'Payment method type',
    payment_installments INT COMMENT 'Number of installments',
    payment_value DECIMAL(15,2) COMMENT 'Payment amount',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Payments fact table'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- =====================================================
-- Category Translation Table (Iceberg)
-- =====================================================
CREATE TABLE IF NOT EXISTS category_translation_iceberg (
    product_category_name STRING COMMENT 'Category name in Portuguese',
    product_category_name_english STRING COMMENT 'Category name in English',
    ingestion_timestamp TIMESTAMP COMMENT 'ETL ingestion timestamp'
)
USING ICEBERG
COMMENT 'Product category translation lookup table'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- =====================================================
-- Show created tables
-- =====================================================
SHOW TABLES;

-- =====================================================
-- Verify table properties
-- =====================================================
SHOW TBLPROPERTIES dim_customers_iceberg;
SHOW TBLPROPERTIES fact_orders_iceberg;

SELECT 'All Iceberg tables created successfully!' AS status;
