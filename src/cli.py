import csv
import logging
import os
import sys

import psycopg
from pyspark.sql import SparkSession

from .pipeline.bronze import BronzeValidationError, process_file
from .pipeline.config import DATA_DIR, DATABASE_CONFIG
from .pipeline.gold import refresh_gold
from .pipeline.silver import SilverValidationError, refresh_silver


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)
    current_file = None
    stage = "startup"
    try:
        if not DATA_DIR.is_dir():
            raise FileNotFoundError("The data directory does not exist")

        with psycopg.connect(**DATABASE_CONFIG, autocommit=True) as connection:
            # A session lock lasts across file transactions and releases on disconnect.
            if not connection.execute("SELECT pg_try_advisory_lock(1414087499)").fetchone()[0]:
                logger.error("Another pipeline or migration run holds the database lock")
                return 1

            paths = sorted(
                path for path in DATA_DIR.iterdir()
                if path.is_file() and path.suffix.lower() == ".csv"
            )
            if not paths:
                logger.info("No pending CSV files. Refreshing silver and gold from existing data.")

            os.environ["PYSPARK_PYTHON"] = sys.executable
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("ticket-pipeline")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            try:
                spark.sparkContext.setLogLevel("ERROR")
                stage = "bronze"
                for current_file in paths:
                    logger.info("Processing file=%s", current_file.name)
                    row_count = process_file(connection, spark, current_file)
                    if row_count is None:
                        logger.info("Skipped duplicate and archived file=%s", current_file.name)
                    else:
                        logger.info("Ingested and archived file=%s rows=%d", current_file.name, row_count)
                logger.info("Bronze run completed files=%d", len(paths))
                stage = "silver"
                current_file = None
                refresh_silver(connection, spark)
                stage = "gold"
                refresh_gold(connection)
                logger.info("Pipeline completed")
            finally:
                spark.stop()
        return 0
    except Exception as error:
        logger.error(
            "Pipeline failed stage=%s file=%s error_type=%s reason=%s sqlstate=%s",
            stage,
            current_file.name if current_file else "<none>",
            type(error).__name__,
            str(error) if isinstance(error, (BronzeValidationError, SilverValidationError, csv.Error, OSError))
            else "Error details withheld to avoid exposing credentials or ticket content",
            getattr(error, "sqlstate", None),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
