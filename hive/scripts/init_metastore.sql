-- =====================================================
-- Hive Metastore Initialization Script
-- This script initializes the PostgreSQL database for Hive metastore
-- =====================================================

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS metastore;

-- Connect to metastore database
\c metastore;

-- Set search path
SET search_path TO public;

-- =====================================================
-- Create Hive Metastore Tables (Version 3.1.0)
-- These are the standard Hive metastore tables
-- =====================================================

-- Version table
CREATE TABLE IF NOT EXISTS "VERSION" (
    "VER_ID" BIGINT NOT NULL,
    "SCHEMA_VERSION" VARCHAR(127) NOT NULL,
    "VERSION_COMMENT" VARCHAR(255)
);

-- Database related tables
CREATE TABLE IF NOT EXISTS "DBS" (
    "DB_ID" BIGINT NOT NULL,
    "DESC" VARCHAR(4000),
    "DB_LOCATION_URI" VARCHAR(4000) NOT NULL,
    "NAME" VARCHAR(128),
    "OWNER_NAME" VARCHAR(128),
    "OWNER_TYPE" VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS "DATABASE_PARAMS" (
    "DB_ID" BIGINT NOT NULL,
    "PARAM_KEY" VARCHAR(180) NOT NULL,
    "PARAM_VALUE" VARCHAR(4000)
);

-- Table related tables
CREATE TABLE IF NOT EXISTS "TBLS" (
    "TBL_ID" BIGINT NOT NULL,
    "CREATE_TIME" INTEGER NOT NULL,
    "DB_ID" BIGINT,
    "LAST_ACCESS_TIME" INTEGER NOT NULL,
    "OWNER" VARCHAR(767),
    "OWNER_TYPE" VARCHAR(10),
    "RETENTION" INTEGER NOT NULL,
    "SD_ID" BIGINT,
    "TBL_NAME" VARCHAR(256),
    "TBL_TYPE" VARCHAR(128),
    "VIEW_EXPANDED_TEXT" TEXT,
    "VIEW_ORIGINAL_TEXT" TEXT,
    "IS_REWRITE_ENABLED" BOOLEAN DEFAULT FALSE NOT NULL
);

CREATE TABLE IF NOT EXISTS "TABLE_PARAMS" (
    "TBL_ID" BIGINT NOT NULL,
    "PARAM_KEY" VARCHAR(256) NOT NULL,
    "PARAM_VALUE" CLOB
);

-- Storage descriptor tables
CREATE TABLE IF NOT EXISTS "SDS" (
    "SD_ID" BIGINT NOT NULL,
    "INPUT_FORMAT" VARCHAR(4000),
    "IS_COMPRESSED" BOOLEAN NOT NULL,
    "LOCATION" VARCHAR(4000),
    "NUM_BUCKETS" INTEGER NOT NULL,
    "OUTPUT_FORMAT" VARCHAR(4000),
    "SERIALIZATION_LIB" VARCHAR(4000),
    "CD_ID" BIGINT
);

CREATE TABLE IF NOT EXISTS "SERDES" (
    "SERDE_ID" BIGINT NOT NULL,
    "NAME" VARCHAR(128),
    "SLIB" VARCHAR(4000),
    "DESCRIPTION" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS "SERDE_PARAMS" (
    "SERDE_ID" BIGINT NOT NULL,
    "PARAM_KEY" VARCHAR(256) NOT NULL,
    "PARAM_VALUE" CLOB
);

-- Column descriptors
CREATE TABLE IF NOT EXISTS "COLUMNS_V2" (
    "CD_ID" BIGINT NOT NULL,
    "COMMENT" VARCHAR(4000),
    "COLUMN_NAME" VARCHAR(767) NOT NULL,
    "TYPE_NAME" CLOB,
    "INTEGER_IDX" INTEGER NOT NULL
);

-- Partition tables
CREATE TABLE IF NOT EXISTS "PARTITIONS" (
    "PART_ID" BIGINT NOT NULL,
    "CREATE_TIME" INTEGER NOT NULL,
    "LAST_ACCESS_TIME" INTEGER NOT NULL,
    "PART_NAME" VARCHAR(767),
    "SD_ID" BIGINT,
    "TBL_ID" BIGINT
);

CREATE TABLE IF NOT EXISTS "PARTITION_PARAMS" (
    "PART_ID" BIGINT NOT NULL,
    "PARAM_KEY" VARCHAR(256) NOT NULL,
    "PARAM_VALUE" CLOB
);

CREATE TABLE IF NOT EXISTS "PARTITION_KEYS" (
    "TBL_ID" BIGINT NOT NULL,
    "PKEY_COMMENT" VARCHAR(4000),
    "PKEY_NAME" VARCHAR(128) NOT NULL,
    "PKEY_TYPE" VARCHAR(767) NOT NULL,
    "INTEGER_IDX" INTEGER NOT NULL
);

-- =====================================================
-- Create indexes for better performance
-- =====================================================

CREATE INDEX IF NOT EXISTS "DBS_NAME_IDX" ON "DBS" ("NAME");
CREATE INDEX IF NOT EXISTS "TBLS_NAME_IDX" ON "TBLS" ("TBL_NAME");
CREATE INDEX IF NOT EXISTS "TBLS_DB_IDX" ON "TBLS" ("DB_ID");
CREATE INDEX IF NOT EXISTS "PARTITIONS_TBL_IDX" ON "PARTITIONS" ("TBL_ID");
CREATE INDEX IF NOT EXISTS "PARTITIONS_SD_IDX" ON "PARTITIONS" ("SD_ID");

-- =====================================================
-- Insert version record
-- =====================================================

INSERT INTO "VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT")
VALUES (1, '3.1.0', 'Hive release version 3.1.0')
ON CONFLICT ("VER_ID") DO NOTHING;

-- =====================================================
-- Create sequences for auto-increment
-- =====================================================

CREATE SEQUENCE IF NOT EXISTS "SEQ_DB_ID" START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS "SEQ_TBL_ID" START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS "SEQ_SD_ID" START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS "SEQ_SERDE_ID" START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS "SEQ_CD_ID" START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS "SEQ_PART_ID" START 1 INCREMENT 1;

-- =====================================================
-- Grant privileges
-- =====================================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO hive;

SELECT 'Hive metastore initialized successfully!' AS status;