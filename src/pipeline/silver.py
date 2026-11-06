from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import logging
import re

from psycopg import Connection, sql
from psycopg.pq import TransactionStatus
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


CATEGORY_ALIASES = {
    alias: category
    for category, aliases in {
        "electrical": ("electrical", "elec", "electrical systems", "power", "power issue"),
        "hvac": ("hvac", "hvac system", "ac", "a/c", "climate control", "heating/cooling", "heating cooling", "air conditioning"),
        "plumbing": ("plumbing", "plumbing issue", "water/plumbing", "water issue"),
        "cleaning": ("cleaning", "housekeeping", "janitorial"),
        "security": ("security", "security systems", "access control", "badge/access"),
        "fire_safety": ("fire safety", "fire/safety", "fire alarm", "sprinkler"),
        "pest_control": ("pest control", "pest", "exterminator"),
        "elevator": ("elevator", "lift", "vertical transport", "elevator/escalator"),
        "it_network": ("it", "it support", "it/network", "network", "connectivity", "wifi"),
        "general_maintenance": ("general", "maintenance", "general maintenance"),
        "other": ("misc", "other"),
    }.items()
    for alias in aliases
}
PRIORITY_ALIASES = {
    "low": "low", "lo": "low",
    "medium": "medium", "med": "medium", "normal": "medium",
    "high": "high", "hi": "high",
    "critical": "critical", "crit": "critical",
}
STATUSES = {"open", "in_progress", "pending_vendor", "escalated", "resolved", "closed"}
SILVER_SCHEMA = (
    "ticket_id STRING, created_at TIMESTAMP_NTZ, resolved_at TIMESTAMP_NTZ, "
    "category STRING, priority STRING, status STRING, building STRING, description STRING, "
    "submitted_by STRING, assigned_to STRING, resolution_notes STRING, cost DECIMAL(18,2), "
    "sla_hours INT, created_at_precision STRING, resolved_at_precision STRING, "
    "created_at_timezone STRING, resolved_at_timezone STRING, quality_flags ARRAY<STRING>, "
    "rejection_reason STRING"
)


class SilverValidationError(ValueError):
    """Silver validation messages that contain no ticket payloads or credentials."""


def parse_timestamp(value: str | None) -> tuple[datetime | None, str | None, str | None, str | None]:
    """Return (timestamp, precision, timezone label, issue) for silver storage."""
    if value is None or not value.strip():
        return None, None, None, "missing"
    value = value.strip()

    if re.fullmatch(r"[0-9]{10}", value):
        try:
            timestamp = datetime.fromtimestamp(int(value), timezone.utc).replace(tzinfo=None)
            return timestamp, "second", "UTC", None
        except (ValueError, OverflowError, OSError):
            return None, None, None, "invalid"

    formats = (
        ("%Y-%m-%d %H:%M:%S", "second"),
        ("%Y-%m-%dT%H:%M:%S", "second"),
        ("%d-%b-%Y %H:%M", "minute"),
        ("%Y-%m-%d", "date"),
        ("%m/%d/%Y", "date"),
        ("%m/%d/%Y %I:%M %p", "minute"),
        ("%m-%d-%Y %H:%M:%S", "second"),
    )
    for date_format, precision in formats:
        try:
            return datetime.strptime(value, date_format), precision, None, None
        except ValueError:
            continue
    return None, None, None, "invalid"


def parse_cost(value: str | None) -> tuple[Decimal | None, str | None]:
    """Return (decimal cost, issue), preserving missing and invalid distinctions."""
    if value is None or not value.strip():
        return None, "missing"
    value = value.strip()
    if value.casefold() in {"n/a", "tbd", "null"}:
        return None, "missing"
    if value.startswith("$"):
        value = value[1:]

    try:
        amount = Decimal(value)
        # Reject values numeric(18,2) would overflow or silently round.
        if not amount.is_finite() or amount < 0 or amount >= 10**16:
            return None, "invalid"
        if amount != amount.quantize(Decimal("0.01")):
            return None, "invalid"
    except InvalidOperation:
        return None, "invalid"
    return amount, None


def parse_sla_hours(value: str | None) -> tuple[int | None, str | None]:
    """Return (integer hours, issue); preserve zero and 999 for confirmation."""
    if value is None or not value.strip():
        return None, "missing"
    value = value.strip()
    if value.casefold() in {"n/a", "null"}:
        return None, "missing"
    if not re.fullmatch(r"[+-]?[0-9]+", value):
        return None, "invalid"
    try:
        hours = int(value)
    except ValueError:
        return None, "invalid"
    if hours < 0 or hours > 2147483647:
        return None, "invalid"
    return hours, "requires_confirmation" if hours in (0, 999) else None


def clean_ticket(fields: dict[str, str]) -> dict:
    """Clean the 13 business columns or return an unusable-ID rejection."""
    text = {name: value.strip() if value is not None else "" for name, value in fields.items()}
    ticket_id = text.get("ticket_id", "")
    if ticket_id.casefold() in {"", "n/a", "null"}:
        return {"rejection_reason": "ticket_id_missing"}
    if not re.fullmatch(r"TKT-[0-9]+", ticket_id):
        return {"rejection_reason": "ticket_id_invalid"}

    result = {"ticket_id": ticket_id, "rejection_reason": None}
    flags = []
    category = text.get("category", "")
    description = text.get("description", "")
    missing_categories = {"", "null", "???", "unknown"}
    junk_categories = {"delete me", "asdf", "test"}
    if category.casefold() not in CATEGORY_ALIASES and description.casefold() in CATEGORY_ALIASES:
        if not category:
            category, description = description, ""
            flags.append("category_recovered_from_description")
        elif category.casefold() not in missing_categories | junk_categories:
            category, description = description, category
            flags.append("category_description_swapped")
    result["category"] = CATEGORY_ALIASES.get(category.casefold())
    if result["category"] is None:
        flags.append("category_missing" if category.casefold() in missing_categories else "category_unmapped")

    priority = text.get("priority", "").casefold()
    result["priority"] = PRIORITY_ALIASES.get(priority)
    if result["priority"] is None:
        flags.append("priority_missing" if priority in {"", "null", "???"} else "priority_unmapped")

    status = text.get("status", "").casefold().replace(" ", "_")
    result["status"] = status if status in STATUSES else None
    if result["status"] is None:
        flags.append("status_missing" if status in {"", "null", "unknown", "???"} else "status_unmapped")

    building = text.get("building", "")
    result["building"] = building if building.casefold() not in {"", "unknown", "???"} else None
    if result["building"] is None:
        flags.append("building_missing")
    elif building.casefold() == "test building":
        flags.append("building_suspected_test")

    result["description"] = description if description.casefold() not in {"", "null"} else None
    if result["description"] is None:
        flags.append("description_missing")
    elif description.casefold() in {".", "test", "asdfasdfasdf", "ignore this row"}:
        flags.append("description_suspected_junk")

    submitted_by = text.get("submitted_by", "")
    result["submitted_by"] = submitted_by.title() if submitted_by else None
    if not submitted_by:
        flags.append("submitted_by_missing")
    elif submitted_by.casefold() == "test":
        flags.append("submitted_by_suspected_test")

    assigned_to = text.get("assigned_to", "")
    result["assigned_to"] = {
        "maintenance team": "Maintenance Team", "overnight crew": "Overnight Crew", "vendor tech": "Vendor Tech",
    }.get(assigned_to.casefold(), assigned_to) or None
    if result["assigned_to"] is None:
        flags.append("assigned_to_missing")

    result["resolution_notes"] = text.get("resolution_notes", "") or None
    if result["resolution_notes"] is None:
        flags.append("resolution_notes_missing")

    terminal = result["status"] in {"closed", "resolved"}
    for column in ("created_at", "resolved_at"):
        timestamp, precision, zone, issue = parse_timestamp(text.get(column))
        result[column] = timestamp
        result[f"{column}_precision"] = precision
        result[f"{column}_timezone"] = zone
        if issue and (column == "created_at" or issue == "invalid" or terminal):
            flags.append(f"{column}_{issue}")
        if precision == "date":
            flags.append(f"{column}_date_only")
        if timestamp is not None and zone is None:
            flags.append(f"{column}_timezone_unknown")

    if result["resolved_at"] is not None and not terminal:
        flags.append("status_resolution_conflict")
    if result["created_at"] is not None and result["resolved_at"] is not None:
        if result["created_at_timezone"] != result["resolved_at_timezone"]:
            flags.append("timestamp_timezone_mismatch")
        if (
            result["created_at_timezone"] == result["resolved_at_timezone"] == "UTC"
            and result["created_at_precision"] != "date" and result["resolved_at_precision"] != "date"
            and result["resolved_at"] < result["created_at"]
        ):
            flags.append("resolved_before_created")

    result["cost"], issue = parse_cost(text.get("cost"))
    if issue:
        flags.append(f"cost_{issue}")
    result["sla_hours"], issue = parse_sla_hours(text.get("sla_hours"))
    if issue:
        flags.append(f"sla_{issue}")
    result["quality_flags"] = sorted(flags)
    return result


def build_silver_dataframes(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform bronze records, rejecting bad IDs and refusing conflicting versions."""
    cleaned = bronze.withColumn("_silver", F.udf(clean_ticket, SILVER_SCHEMA)("parsed_fields")).select(
        "_silver.*", F.col("file_sha256").alias("source_file_sha256"),
        F.col("record_number").alias("source_record_number"), "row_hash",
    )
    accepted = cleaned.filter(F.col("rejection_reason").isNull())
    if accepted.groupBy("ticket_id").agg(F.countDistinct("row_hash").alias("versions")).filter(
        F.col("versions") > 1
    ).limit(1).count():
        raise SilverValidationError("Conflicting source payloads share a valid ticket ID; silver was not published")

    order = Window.partitionBy("ticket_id").orderBy("source_file_sha256", "source_record_number")
    tickets = accepted.withColumn("_ordinal", F.row_number().over(order)).filter(
        F.col("_ordinal") == 1
    ).drop("_ordinal", "row_hash", "rejection_reason")
    rejected = cleaned.filter(F.col("rejection_reason").isNotNull()).select(
        "source_file_sha256", "source_record_number", "rejection_reason",
    )
    return tickets, rejected


def refresh_silver(connection: Connection, spark: SparkSession) -> dict[str, int]:
    """Rebuild both silver tables from bronze and commit their replacement together."""
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise SilverValidationError("refresh_silver requires a connection without an active transaction")
    logger = logging.getLogger(__name__)
    counts = {}
    with connection.transaction():
        if not connection.execute("SELECT pg_try_advisory_xact_lock(1414087499)").fetchone()[0]:
            raise SilverValidationError("Another migration or pipeline run holds the database lock")
        rows = connection.execute(
            "SELECT file_sha256, record_number, row_hash, raw_record, parsed_fields "
            "FROM bronze.raw_tickets ORDER BY file_sha256, record_number"
        ).fetchall()
        # shortcut: local bronze records fit in driver memory; use distributed reads at larger scale.
        bronze = spark.createDataFrame(rows, schema=(
            "file_sha256 STRING, record_number BIGINT, row_hash STRING, "
            "raw_record STRING, parsed_fields MAP<STRING, STRING>"
        ))
        tickets, rejected = build_silver_dataframes(bronze)
        with connection.cursor() as cursor:
            for table, dataframe in (("tickets", tickets), ("rejected_tickets", rejected)):
                staging = f"silver_{table}_stage"
                cursor.execute(sql.SQL(
                    "CREATE TEMP TABLE {} (LIKE silver.{} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP"
                ).format(sql.Identifier(staging), sql.Identifier(table)))
                counts[table] = 0
                with cursor.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(
                    sql.Identifier(staging), sql.SQL(", ").join(map(sql.Identifier, dataframe.columns)),
                )) as copy:
                    for record in dataframe.toLocalIterator():
                        copy.write_row(tuple(record))
                        counts[table] += 1

            for table in ("tickets", "rejected_tickets"):
                cursor.execute(sql.SQL("DELETE FROM silver.{}").format(sql.Identifier(table)))
                cursor.execute(sql.SQL("INSERT INTO silver.{} SELECT * FROM {}").format(
                    sql.Identifier(table), sql.Identifier(f"silver_{table}_stage"),
                ))
    counts["source_rows"] = len(rows)
    counts["duplicates"] = len(rows) - counts["tickets"] - counts["rejected_tickets"]
    logger.info("Silver published source_rows=%d tickets=%d rejected=%d duplicates=%d",
                counts["source_rows"], counts["tickets"], counts["rejected_tickets"], counts["duplicates"])
    return counts
