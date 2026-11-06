import logging
import os
import sys

import psycopg
from pyspark.sql import SparkSession

from .pipeline.bronze import process_file
from .pipeline.config import DATA_DIR, DATABASE_CONFIG


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)
    current_file = None
    try:
        if not DATA_DIR.is_dir():
            raise FileNotFoundError("The data directory does not exist")

        with psycopg.connect(**DATABASE_CONFIG, autocommit=True) as connection:
            # A session lock lasts across file transactions and releases on disconnect.
            if not connection.execute("SELECT pg_try_advisory_lock(1414087499)").fetchone()[0]:
                logger.error("Another bronze run holds the database lock")
                return 1

            paths = sorted(
                path for path in DATA_DIR.iterdir()
                if path.is_file() and path.suffix.lower() == ".csv"
            )
            if not paths:
                logger.info("No pending CSV files")
                return 0

            os.environ["PYSPARK_PYTHON"] = sys.executable
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("ticket-bronze")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            try:
                spark.sparkContext.setLogLevel("ERROR")
                for current_file in paths:
                    logger.info("Processing file=%s", current_file.name)
                    row_count = process_file(connection, spark, current_file)
                    if row_count is None:
                        logger.info("Skipped duplicate and archived file=%s", current_file.name)
                    else:
                        logger.info("Ingested and archived file=%s rows=%d", current_file.name, row_count)
                logger.info("Bronze run completed files=%d", len(paths))
            finally:
                spark.stop()
        return 0
    except Exception as error:
        logger.error(
            "Bronze run failed file=%s error_type=%s",
            current_file.name if current_file else "<startup>",
            type(error).__name__,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
