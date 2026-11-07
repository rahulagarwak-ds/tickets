import logging

from psycopg import Connection
from psycopg.pq import TransactionStatus


def refresh_gold(connection: Connection) -> int:
    """Publish one fact per silver ticket and return the committed fact count."""
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("refresh_gold requires a connection without an active transaction")

    with connection.transaction():
        if not connection.execute("SELECT pg_try_advisory_xact_lock(1414087499)").fetchone()[0]:
            raise RuntimeError("Another migration or pipeline run holds the database lock")

        buildings = connection.execute("""
            INSERT INTO gold.dim_building (building_name)
            SELECT building FROM silver.tickets WHERE building IS NOT NULL
            EXCEPT SELECT building_name FROM gold.dim_building
            ORDER BY 1
            ON CONFLICT (building_name) DO NOTHING
        """).rowcount
        dates = connection.execute("""
            INSERT INTO gold.dim_date (calendar_date)
            SELECT created_at::date FROM silver.tickets WHERE created_at IS NOT NULL
            UNION
            SELECT resolved_at::date FROM silver.tickets WHERE resolved_at IS NOT NULL
            ON CONFLICT (calendar_date) DO NOTHING
        """).rowcount

        connection.execute("DELETE FROM gold.fact_ticket")
        # A missing lookup fails the fact's NOT NULL constraint instead of losing a ticket.
        facts = connection.execute("""
            INSERT INTO gold.fact_ticket (
                ticket_id, building_key, category_key, priority_key, status_key,
                created_date, resolved_date, recorded_cost, source_file_sha256, source_record_number
            )
            SELECT t.ticket_id, b.building_key, c.category_key, p.priority_key, s.status_key,
                   t.created_at::date, t.resolved_at::date, t.cost, t.source_file_sha256, t.source_record_number
            FROM silver.tickets t
            LEFT JOIN gold.dim_building b ON b.building_name = COALESCE(t.building, 'Unknown')
            LEFT JOIN gold.dim_category c ON c.category_name = COALESCE(t.category, 'Unknown')
            LEFT JOIN gold.dim_priority p ON p.priority_name = COALESCE(t.priority, 'Unknown')
            LEFT JOIN gold.dim_status s ON s.status_name = COALESCE(t.status, 'Unknown')
        """).rowcount

    logging.getLogger(__name__).info("Gold published tickets=%d new_buildings=%d new_dates=%d", facts, buildings, dates)
    return facts
