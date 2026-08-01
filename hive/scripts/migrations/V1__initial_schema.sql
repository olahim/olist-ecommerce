-- =====================================================
-- Migration V1: Initial Schema Setup
-- Version: 1.0
-- Description: Initial database schema for Olist Ecommerce
-- =====================================================

-- Create database
CREATE DATABASE IF NOT EXISTS olist_raw;
CREATE DATABASE IF NOT EXISTS olist_warehouse;
CREATE DATABASE IF NOT EXISTS olist_quality;

USE olist_raw;

-- Raw layer tables (already created in create_olist_tables.sql)
-- This migration ensures they exist

-- Create staging tables for quality metrics
CREATE TABLE IF NOT EXISTS olist_quality.dq_metrics (
    dataset_name STRING,
    check_type STRING,
    check_date DATE,
    passed BOOLEAN,
    failed_count BIGINT,
    total_count BIGINT,
    dq_score DECIMAL(5,2),
    details STRING,
    ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '/opt/hadoop/data/quality/dq_metrics/';

-- Create table for failed records tracking
CREATE TABLE IF NOT EXISTS olist_quality.failed_records (
    dataset_name STRING,
    record_id STRING,
    failure_reason STRING,
    failed_checks STRING,
    raw_record STRING,
    failure_date DATE,
    ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '/opt/hadoop/data/quality/failed_records/';

-- Insert migration record
CREATE TABLE IF NOT EXISTS olist_warehouse.schema_migrations (
    version STRING PRIMARY KEY,
    description STRING,
    applied_at TIMESTAMP,
    success BOOLEAN
)
STORED AS PARQUET;

INSERT INTO olist_warehouse.schema_migrations (version, description, applied_at, success)
VALUES ('V1', 'Initial schema setup', CURRENT_TIMESTAMP(), TRUE);

SELECT 'Migration V1 completed successfully' AS status;