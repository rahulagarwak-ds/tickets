-- Migration: 002_initial_silver
-- Purpose: Create typed silver tickets and rejected-record lineage.

CREATE SCHEMA silver;

CREATE TABLE silver.tickets (
    ticket_id text PRIMARY KEY CHECK (ticket_id ~ '^TKT-[0-9]+$'),
    created_at timestamp without time zone,
    resolved_at timestamp without time zone,
    category text CHECK (category IN (
        'electrical', 'hvac', 'plumbing', 'cleaning', 'security', 'fire_safety',
        'pest_control', 'elevator', 'it_network', 'general_maintenance', 'other'
    )),
    priority text CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    status text CHECK (status IN (
        'open', 'in_progress', 'pending_vendor', 'escalated', 'resolved', 'closed'
    )),
    building text,
    description text,
    submitted_by text,
    assigned_to text,
    resolution_notes text,
    cost numeric(18,2) CHECK (cost >= 0),
    sla_hours integer CHECK (sla_hours >= 0),
    source_file_sha256 text NOT NULL,
    source_record_number bigint NOT NULL,
    quality_flags text[] NOT NULL DEFAULT '{}',
    created_at_precision text CHECK (created_at_precision IN ('date', 'minute', 'second')),
    resolved_at_precision text CHECK (resolved_at_precision IN ('date', 'minute', 'second')),
    created_at_timezone text CHECK (created_at_timezone = 'UTC'),
    resolved_at_timezone text CHECK (resolved_at_timezone = 'UTC'),
    CHECK ((created_at IS NULL) = (created_at_precision IS NULL)),
    CHECK ((resolved_at IS NULL) = (resolved_at_precision IS NULL)),
    CHECK (created_at IS NOT NULL OR created_at_timezone IS NULL),
    CHECK (resolved_at IS NOT NULL OR resolved_at_timezone IS NULL),
    FOREIGN KEY (source_file_sha256, source_record_number)
        REFERENCES bronze.raw_tickets (file_sha256, record_number)
);

CREATE TABLE silver.rejected_tickets (
    source_file_sha256 text NOT NULL,
    source_record_number bigint NOT NULL,
    rejection_reason text NOT NULL CHECK (rejection_reason IN (
        'ticket_id_missing', 'ticket_id_invalid'
    )),
    PRIMARY KEY (source_file_sha256, source_record_number),
    FOREIGN KEY (source_file_sha256, source_record_number)
        REFERENCES bronze.raw_tickets (file_sha256, record_number)
);
