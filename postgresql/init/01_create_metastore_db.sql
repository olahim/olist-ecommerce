-- =====================================================
-- PostgreSQL: Create Hive Metastore Database
-- =====================================================

-- Create metastore database
CREATE DATABASE metastore
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE = template0;

-- Create user for Hive metastore
CREATE USER hive WITH PASSWORD 'hive_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

-- Connect to metastore database
\c metastore;

-- Set schema search path
ALTER DATABASE metastore SET search_path TO public;

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

SELECT 'Metastore database created successfully' AS status;