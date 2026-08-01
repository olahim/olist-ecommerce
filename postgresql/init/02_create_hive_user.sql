-- =====================================================
-- PostgreSQL: Create Hive User and Grant Privileges
-- =====================================================

\c metastore;

-- Create role with login
CREATE ROLE hive WITH LOGIN PASSWORD 'hive_password';

-- Grant connect privilege
GRANT CONNECT ON DATABASE metastore TO hive;

-- Grant schema privileges
GRANT USAGE ON SCHEMA public TO hive;
GRANT CREATE ON SCHEMA public TO hive;

-- Grant table privileges
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hive;

-- Grant sequence privileges
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO hive;

-- Set as owner of future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO hive;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO hive;

-- Create connection limits
ALTER ROLE hive WITH CONNECTION LIMIT 50;

-- Set session defaults
ALTER ROLE hive SET statement_timeout TO '30min';
ALTER ROLE hive SET idle_in_transaction_session_timeout TO '10min';

SELECT 'Hive user created and privileges granted' AS status;