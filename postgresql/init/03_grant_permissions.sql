-- =====================================================
-- PostgreSQL: Grant Additional Permissions
-- =====================================================

\c metastore;

-- Grant all privileges on sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO hive;

-- Grant create temporary table privilege
GRANT TEMP ON DATABASE metastore TO hive;

-- Create backup user
CREATE USER backup_user WITH LOGIN PASSWORD 'backup_password';

-- Grant read-only access for backups
GRANT CONNECT ON DATABASE metastore TO backup_user;
GRANT USAGE ON SCHEMA public TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO backup_user;

-- Create monitoring user
CREATE USER monitoring_user WITH LOGIN PASSWORD 'monitoring_password';

-- Grant limited access for monitoring
GRANT CONNECT ON DATABASE metastore TO monitoring_user;
GRANT USAGE ON SCHEMA public TO monitoring_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO monitoring_user;

-- Create pg_stat_statements extension for query monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Configure pg_stat_statements
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';
ALTER SYSTEM SET pg_stat_statements.max = 10000;

-- Reload configuration
SELECT pg_reload_conf();

SELECT 'Additional permissions granted successfully' AS status;
