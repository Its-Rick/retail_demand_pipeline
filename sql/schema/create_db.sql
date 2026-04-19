-- =============================================================================
-- create_db.sql  (runs FIRST as init script — 01_create_db.sql)
-- Creates the retail_dw database so all services can connect to it.
-- PostgreSQL init scripts run as the superuser inside the default DB.
-- =============================================================================

-- Create retail_dw database if it doesn't exist
SELECT 'CREATE DATABASE retail_dw'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'retail_dw'
)\gexec

-- Grant all privileges to the airflow user
GRANT ALL PRIVILEGES ON DATABASE retail_dw TO airflow;