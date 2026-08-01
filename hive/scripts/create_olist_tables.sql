-- =====================================================
-- Olist Ecommerce Tables DDL
-- This script creates managed tables for Olist data
-- =====================================================

-- Create database for Olist
CREATE DATABASE IF NOT EXISTS olist_raw
COMMENT 'Raw Olist ecommerce data'
LOCATION '/opt/hadoop/data/warehouse/olist_raw';

USE olist_raw;

-- =====================================================
-- Customers Table
-- =====================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Customer information table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'transient_lastDdlTime' = '${hiveconf:current_time}'
);

-- =====================================================
-- Orders Table
-- =====================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_timestamp TIMESTAMP,
    order_delivered_carrier_timestamp TIMESTAMP,
    order_delivered_customer_timestamp TIMESTAMP,
    order_estimated_delivery_date DATE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Order information table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Products Table
-- =====================================================
CREATE TABLE IF NOT EXISTS products (
    product_id STRING,
    product_category_name STRING,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Product catalog table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Sellers Table
-- =====================================================
CREATE TABLE IF NOT EXISTS sellers (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Seller information table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Geolocation Table
-- =====================================================
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code_prefix STRING,
    geolocation_lat DOUBLE,
    geolocation_lng DOUBLE,
    geolocation_city STRING,
    geolocation_state STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Geolocation mapping table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Order Reviews Table
-- =====================================================
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id STRING,
    order_id STRING,
    review_score INT,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Customer review table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Order Payments Table
-- =====================================================
CREATE TABLE IF NOT EXISTS order_payments (
    order_id STRING,
    payment_sequential INT,
    payment_type STRING,
    payment_installments INT,
    payment_value DOUBLE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Payment transactions table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Order Items Table
-- =====================================================
CREATE TABLE IF NOT EXISTS order_items (
    order_id STRING,
    order_item_id INT,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP,
    price DOUBLE,
    freight_value DOUBLE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Order line items table'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Category Translation Table
-- =====================================================
CREATE TABLE IF NOT EXISTS category_translation (
    product_category_name STRING,
    product_category_name_english STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
COMMENT 'Portuguese to English category mapping'
PARTITIONED BY (ingestion_year STRING, ingestion_month STRING, ingestion_day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =====================================================
-- Show created tables
-- =====================================================
SHOW TABLES;

SELECT 'All Olist tables created successfully!' AS status;