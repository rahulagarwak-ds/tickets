from datetime import datetime, timezone
from decimal import Decimal
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

import psycopg
from psycopg import sql

from ..pipeline.config import DATABASE_CONFIG
from .workflow import fingerprint, validate_code


def check_approval(directory: Path) -> dict:
    approval = json.loads((directory / "approval.json").read_text())
    trace = json.loads((directory / "trace.json").read_text())
    if not approval["reviewer"].strip() or approval["fingerprint"] != fingerprint(directory) or trace["fingerprint"] != fingerprint(directory):
        raise ValueError("Approval does not match current artifacts")
    return trace


def evaluate_dq(directory: Path) -> None:
    from decimal import Decimal
    from pyspark.sql import SparkSession

    check_approval(directory)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spec = importlib.util.spec_from_file_location("candidate", directory / "candidate.py")
    candidate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(candidate)
    spark = SparkSession.builder.master("local[2]").appName("agent-eval").config("spark.ui.enabled", "false").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        cases = [
            ("$12.50", "4", Decimal("12.50"), 4, []),
            ("0", "0", Decimal("0"), 0, ["sla_requires_confirmation"]),
            ("-999", "999", None, 999, ["cost_invalid", "sla_requires_confirmation"]),
            ("-1", "-1", None, None, ["cost_invalid", "sla_invalid"]),
            ("", "", None, None, ["cost_missing", "sla_missing"]),
            ("N/A", "NULL", None, None, ["cost_missing", "sla_missing"]),
            ("TBD", "4.5", None, None, ["cost_missing", "sla_invalid"]),
            ("error", "72", None, 72, ["cost_invalid"]),
            ("14996.24", "24", Decimal("14996.24"), 24, []),
            (None, None, None, None, ["cost_missing", "sla_missing"]),
            ("NaN", "2147483648", None, None, ["cost_invalid", "sla_invalid"]),
            ("0.001", "4", None, 4, ["cost_invalid"]),
            ("10000000000000000", "8", None, 8, ["cost_invalid"]),
        ]
        frame = spark.createDataFrame([(str(i), c, s) for i, (c, s, *_rest) in enumerate(cases)], "id STRING,cost STRING,sla_hours STRING")
        output = candidate.transform(frame)
        assert dict(output.dtypes)["cost"] == "decimal(18,2)", "cost type"
        assert dict(output.dtypes)["sla_hours"] == "int", "SLA type"
        actual = output.limit(len(cases) + 1).collect()
        assert len(actual) == len(cases), "row preservation"
        assert {row.id for row in actual} == {str(i) for i in range(len(cases))}, "identifier preservation"
        for row in actual:
            _, _, cost, sla, flags = cases[int(row.id)]
            assert (row.cost, row.sla_hours, sorted(row.quality_flags)) == (cost, sla, flags), f"fixture {row.id}"
        empty = candidate.transform(spark.createDataFrame([], frame.schema))
        assert empty.limit(1).count() == 0, "empty input"
    finally:
        spark.stop()


def evaluate_gold(directory: Path) -> None:
    code = (directory / "candidate.sql").read_text()
    validate_code("gold_design", code)
    name = "ticket_agent_eval_" + uuid.uuid4().hex
    admin = DATABASE_CONFIG | {"dbname": "postgres"}
    with psycopg.connect(**admin, autocommit=True) as control:
        control.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with psycopg.connect(**(DATABASE_CONFIG | {"dbname": name})) as connection:
                connection.execute("CREATE SCHEMA silver")
                connection.execute("""CREATE TABLE silver.tickets AS
                    SELECT * FROM (VALUES ('A','open','critical',0::numeric),
                    ('A','closed','low',100::numeric),('A',NULL,NULL,NULL),
                    (NULL,NULL,NULL,NULL)) v(building,status,priority,cost)""")
                connection.execute("CREATE SCHEMA candidate")
                connection.execute("SET LOCAL statement_timeout = '10s'")
                connection.execute(code, prepare=True)
            with psycopg.connect(**(DATABASE_CONFIG | {"dbname": name})) as connection:
                connection.execute("SET TRANSACTION READ ONLY")
                connection.execute("SET LOCAL statement_timeout = '10s'")
                assert connection.execute("SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'candidate' AND matviewname = 'report'").fetchone()[0] == 1, "materialized view creation"
                cursor = connection.execute("SELECT * FROM candidate.report LIMIT 1001")
                columns = [c.name for c in cursor.description]
                assert columns == ["building", "total_tickets", "terminal_tickets", "unresolved_tickets", "unknown_status_tickets", "critical_unresolved_tickets", "tickets_with_recorded_cost", "total_recorded_cost", "average_recorded_cost", "cost_coverage_pct", "terminal_rate_pct"], "report columns"
                actual = sorted(cursor.fetchall())
                assert len(actual) == 2, "unknown group preservation"
                assert actual[0][:9] == ('A', 3, 1, 1, 1, 1, 2, 100, 50), "known group KPIs"
                assert abs(actual[0][9] - Decimal(200) / 3) < Decimal("0.00001") and actual[0][10] == 50, "denominators"
                assert actual[1] == ('Unknown', 1, 0, 0, 1, 0, 0, None, None, 0, None), "missing cost/status semantics"
        finally:
            control.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def evaluate_run(directory: Path) -> dict:
    trace = check_approval(directory)
    started = time.monotonic()
    result = {"fingerprint": fingerprint(directory), "agent": trace["agent"],
              "evaluated_at": datetime.now(timezone.utc).isoformat()}
    try:
        if trace["agent"] == "data_quality":
            validate_code("data_quality", (directory / "candidate.py").read_text())
            env = {key: value for key, value in os.environ.items() if key not in {"OPENAI_API_KEY", "PGPASSWORD"}}
            process = subprocess.run([sys.executable, "-m", "src.agents.evaluate", str(directory.resolve())], env=env, capture_output=True, text=True, timeout=120)
            if process.returncode:
                raise ValueError("DQ candidate failed fixtures; inspect the reviewed code")
            result["checks"] = "13 value fixtures, types, identifiers, row preservation, empty input"
        else:
            evaluate_gold(directory)
            result["checks"] = "materialized view creation, grain, report columns, reconciliation, zero/missing cost, unknown groups, rate denominators"
        result["passed"] = True
    except Exception as error:
        result.update(passed=False, error_type=type(error).__name__)
        raise
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (directory / "evaluation.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    evaluate_dq(Path(sys.argv[1]))
