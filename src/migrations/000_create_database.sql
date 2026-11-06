-- Bootstrap: 000_create_database
-- Purpose: Create the project database only if it does not already exist.
-- Run through psql against the postgres maintenance database, outside a transaction.

\set ON_ERROR_STOP on

SELECT 'CREATE DATABASE ticket_medallion'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'ticket_medallion'
)
\gexec
