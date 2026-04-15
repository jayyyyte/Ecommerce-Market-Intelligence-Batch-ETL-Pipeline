-- =============================================================================
-- 00_init_databases.sql
-- Runs automatically when the PostgreSQL container first starts.
-- Creates the two additional databases Airflow and Metabase need,
-- alongside the already-created ecommerce_db (from POSTGRES_DB env var).
-- =============================================================================

-- Airflow metadata database
CREATE DATABASE airflow_db;
CREATE DATABASE metabaseappdb;

SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow_db'
)\gexec

-- Metabase application database
SELECT 'CREATE DATABASE metabaseappdb'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'metabaseappdb'
)\gexec