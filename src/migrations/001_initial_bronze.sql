-- Migration: 001_initial_bronze
-- Purpose: Create migration history and lossless, idempotent bronze storage.

CREATE SCHEMA migration;
CREATE SCHEMA bronze;

CREATE TABLE migration.history (
    migration_id text PRIMARY KEY,
    sql_checksum text NOT NULL CHECK (sql_checksum ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bronze.ingested_files (
    file_sha256 text PRIMARY KEY CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    original_filename text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count bigint NOT NULL CHECK (row_count >= 0)
);

CREATE TABLE bronze.raw_tickets (
    file_sha256 text NOT NULL REFERENCES bronze.ingested_files (file_sha256),
    record_number bigint NOT NULL CHECK (record_number > 0),
    row_hash text NOT NULL CHECK (row_hash ~ '^[0-9a-f]{64}$'),
    raw_record text NOT NULL,
    parsed_fields jsonb NOT NULL CHECK (jsonb_typeof(parsed_fields) = 'object'),
    PRIMARY KEY (file_sha256, record_number)
);
