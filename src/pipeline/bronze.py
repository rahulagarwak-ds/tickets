import csv
import hashlib
import json
import os
from collections.abc import Iterator
from pathlib import Path

from psycopg import Connection
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb
from pyspark.sql import DataFrame, SparkSession

from .config import DATA_DIR, PROCESSED_DIR


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_file_ingested(connection: Connection, file_hash: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM bronze.ingested_files WHERE file_sha256 = %s)",
            (file_hash,),
        )
        return cursor.fetchone()[0]


def row_sha256(parsed_fields: dict[str, str]) -> str:
    payload = json.dumps(parsed_fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_csv_records(path: Path) -> Iterator[tuple[int, str, dict[str, str]]]:
    # A second stream preserves original line endings and quoted multiline records.
    with (
        path.open("r", encoding="utf-8-sig", newline="") as source,
        path.open("r", encoding="utf-8-sig", newline="") as raw_source,
    ):
        reader = csv.reader(source, strict=True)
        headers = next(reader, None)
        if not headers or any(not header.strip() for header in headers):
            raise ValueError("CSV must have nonempty column names")
        if len(set(headers)) != len(headers):
            raise ValueError("CSV must have unique column names")

        previous_line = reader.line_num
        for _ in range(previous_line):
            raw_source.readline()

        for record_number, values in enumerate(reader, start=1):
            raw_record = "".join(
                raw_source.readline() for _ in range(reader.line_num - previous_line)
            )
            previous_line = reader.line_num
            if len(values) != len(headers):
                raise ValueError(f"CSV record {record_number} has an unexpected field count")
            yield record_number, raw_record, dict(zip(headers, values))


def build_bronze_dataframe(spark: SparkSession, path: Path, file_hash: str) -> DataFrame:
    records = (
        (file_hash, number, row_sha256(fields), raw_record, fields)
        for number, raw_record, fields in read_csv_records(path)
    )
    # shortcut: Spark materializes local records on the driver; use distributed reads when files outgrow driver memory.
    return spark.createDataFrame(
        records,
        schema=(
            "file_sha256 STRING, record_number BIGINT, row_hash STRING, "
            "raw_record STRING, parsed_fields MAP<STRING, STRING>"
        ),
    )


def ingest_file(connection: Connection, spark: SparkSession, path: Path) -> int | None:
    """Return the committed row count, or None if the file was already ingested."""
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("ingest_file requires a connection without an active transaction")

    file_hash = file_sha256(path)
    with connection.transaction():
        if is_file_ingested(connection, file_hash):
            return None

        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO bronze.ingested_files (file_sha256, original_filename, row_count) "
                "VALUES (%s, %s, 0) ON CONFLICT (file_sha256) DO NOTHING RETURNING file_sha256",
                (file_hash, path.name),
            )
            if cursor.fetchone() is None:
                return None

            dataframe = build_bronze_dataframe(spark, path, file_hash)
            row_count = 0
            with cursor.copy(
                "COPY bronze.raw_tickets "
                "(file_sha256, record_number, row_hash, raw_record, parsed_fields) FROM STDIN"
            ) as copy:
                for record in dataframe.toLocalIterator():
                    copy.write_row(
                        (
                            record.file_sha256,
                            record.record_number,
                            record.row_hash,
                            record.raw_record,
                            Jsonb(record.parsed_fields),
                        )
                    )
                    row_count += 1

            if file_sha256(path) != file_hash:
                raise ValueError("Source file changed during ingestion; transaction rolled back")

            cursor.execute(
                "UPDATE bronze.ingested_files SET row_count = %s WHERE file_sha256 = %s",
                (row_count, file_hash),
            )

    return row_count


def process_file(connection: Connection, spark: SparkSession, path: Path) -> int | None:
    """Ingest and archive a pending file; return the inserted count or None if skipped."""
    if path.is_symlink() or not path.is_file() or path.resolve().parent != DATA_DIR.resolve():
        raise ValueError("Input must be a regular file directly inside data/")

    file_hash = file_sha256(path)
    row_count = ingest_file(connection, spark, path)
    if file_sha256(path) != file_hash:
        raise ValueError("Source file changed; left in data/ for retry")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    destination = PROCESSED_DIR / path.name
    try:
        # Link before unlinking: never overwrite, and retain the source if archiving fails.
        os.link(path, destination)
    except FileExistsError:
        if (
            destination.is_symlink()
            or not destination.is_file()
            or file_sha256(destination) != file_hash
        ):
            raise FileExistsError(f"Processed destination has different content: {destination}") from None

    path.unlink()
    return row_count
