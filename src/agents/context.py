import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
import statistics
import subprocess

from ..pipeline.silver import parse_timestamp


def profile_bronze(connection) -> dict:
    # shortcut: bounded local profiling; replace with database aggregates for larger sources.
    rows = connection.execute("SELECT parsed_fields FROM bronze.raw_tickets LIMIT 50001").fetchall()
    if len(rows) > 50000:
        raise ValueError("Profile exceeds 50,000 rows; use database aggregates before generating")
    profile = {}
    for column in sorted({column for row, in rows for column in row}):
        values = Counter(row.get(column) or "" for row, in rows)
        item = {"distinct": len(values), "empty": values[""],
                "empty_rate": values[""] / len(rows) if rows else 0,
                "top_values": values.most_common(10)}
        if column in {"cost", "sla_hours"}:
            numbers, invalid = [], Counter()
            for value, count in values.items():
                if not value.strip():
                    continue
                try:
                    number = Decimal(value.strip().removeprefix("$"))
                    if not number.is_finite():
                        raise InvalidOperation
                    numbers.extend([float(number)] * count)
                except InvalidOperation:
                    invalid[value] += count
            item["numeric"] = {"invalid": dict(invalid), "negative": sum(n < 0 for n in numbers),
                               "zero": numbers.count(0)}
            if numbers:
                quartiles = statistics.quantiles(numbers, n=4, method="inclusive") if len(numbers) > 1 else numbers * 3
                q1, _, q3 = quartiles
                item["numeric"].update(min=min(numbers), max=max(numbers), median=statistics.median(numbers),
                                       mean=statistics.mean(numbers),
                                       iqr_outliers=sum(n < q1 - 1.5 * (q3-q1) or n > q3 + 1.5 * (q3-q1) for n in numbers))
        if column in {"created_at", "resolved_at"}:
            parsed = Counter()
            for value, count in values.items():
                _, precision, zone, issue = parse_timestamp(value)
                parsed[issue or f"{precision}:{zone or 'unknown_timezone'}"] += count
            item["date_formats"] = dict(parsed)
            patterns = Counter()
            for value, count in values.items():
                patterns[re.sub(r"[0-9]", "D", value)] += count
            item["format_patterns"] = patterns.most_common(15)
        profile[column] = item
    return {"rows": len(rows), "columns": profile}


def build_context(connection, agent: str, instruction: str, feedback: str = "", previous=None) -> dict:
    if not instruction.strip():
        raise ValueError("A user instruction is required")
    schema = connection.execute("""SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns WHERE table_schema IN ('silver','gold')
        ORDER BY table_schema, table_name, ordinal_position""").fetchall()
    batches = connection.execute("SELECT file_sha256, row_count FROM bronze.ingested_files ORDER BY file_sha256").fetchall()
    return {
        "version": 1, "agent": agent, "instruction": instruction, "feedback": feedback,
        "baseline_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "batches": batches, "profile": profile_bronze(connection), "schema": schema,
        "knowledge_graph": json.loads(Path(__file__).with_name("knowledge.json").read_text()),
        "approved_rules": ["Keep zero cost; missing and negative costs are null with separate flags.",
            "SLA -1 is invalid; retain 0 and 999 and flag for confirmation.",
            "silver.tickets.status uses canonical values open, in_progress, pending_vendor, escalated, resolved, closed. "
            "Use these exact values in silver queries, not the raw bronze spellings. "
            "Terminal means resolved or closed; open, in_progress, pending_vendor and escalated are unresolved; SQL NULL is unknown.",
            "Cost coverage includes recorded zeros. Averages exclude missing costs. Currency is unknown.",
            "Unknown groups remain visible. Date-only precision and unknown timezones prevent hourly metrics.",
            "Do not merge people's identities. Applied migrations remain versioned."],
        "open_questions": ["SLA 0/999 meaning", "Source timezone", "Common currency"],
        "previous_proposal": previous,
    }
