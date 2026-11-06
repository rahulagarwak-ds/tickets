import hashlib
import logging
from pathlib import Path

import psycopg
from psycopg import sql

from ..pipeline.config import DATABASE_CONFIG


MIGRATIONS_DIR = Path(__file__).resolve().parent


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)
    current_migration = "<database initialization>"
    try:
        with psycopg.connect(**(DATABASE_CONFIG | {"dbname": "postgres"}), autocommit=True) as connection:
            if not connection.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE_CONFIG["dbname"],)
            ).fetchone():
                connection.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE_CONFIG["dbname"]))
                )
                logger.info("Created database=%s", DATABASE_CONFIG["dbname"])

        with psycopg.connect(**DATABASE_CONFIG, autocommit=True) as connection:
            if not connection.execute("SELECT pg_try_advisory_lock(1414087499)").fetchone()[0]:
                logger.error("Another migration or bronze run holds the database lock")
                return 1

            has_history = connection.execute(
                "SELECT to_regclass('migration.history') IS NOT NULL"
            ).fetchone()[0]
            for path in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql")):
                if path.name.startswith("000_"):
                    continue
                current_migration = path.stem
                payload = path.read_bytes()
                checksum = hashlib.sha256(payload).hexdigest()
                applied = connection.execute(
                    "SELECT sql_checksum FROM migration.history WHERE migration_id = %s",
                    (current_migration,),
                ).fetchone() if has_history else None
                if applied:
                    if applied[0] != checksum:
                        logger.error("Applied migration checksum mismatch migration=%s", current_migration)
                        return 1
                    logger.info("Already applied migration=%s", current_migration)
                    continue

                with connection.transaction():
                    connection.execute(payload.decode("utf-8"))
                    connection.execute(
                        "INSERT INTO migration.history (migration_id, sql_checksum) VALUES (%s, %s)",
                        (current_migration, checksum),
                    )
                has_history = True
                logger.info("Applied migration=%s checksum=%s", current_migration, checksum)
        return 0
    except Exception as error:
        logger.error(
            "Migration failed migration=%s error_type=%s sqlstate=%s",
            current_migration, type(error).__name__, getattr(error, "sqlstate", None),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
