# Ticket medallion

The bronze pipeline ingests CSV files into PostgreSQL using PySpark and psycopg.
Silver, gold, and assessment agents are not implemented yet.
No Docker or Compose is used.

## Prerequisites

- uv; Python 3.12 is selected through `.python-version` and `pyproject.toml`.
- Java 17 or 21 on `PATH`, or a valid `JAVA_HOME`.
- A running PostgreSQL server. Initial setup requires a user with permission to
  create the project database and schemas, and access to the `postgres` maintenance database.

## Dependencies

| Dependency | Purpose |
| --- | --- |
| Python 3.12 (tested: 3.12.7) | Project runtime |
| PySpark 4.3.0 | Builds the bronze DataFrame with explicit types |
| psycopg 3.3.8 with binary extras | Transactional PostgreSQL ingestion; no local compilation |
| LangGraph 1.2.20 | Agent workflows |
| LangChain 1.4.9 | LLM integration |
| langchain-openai 1.7.4 | Installed provider adapter; provider/model choice remains provisional |

Exact Python library versions are recorded in `uv.lock`. Java and PostgreSQL
are managed separately from uv. The current pipeline does not require JDBC.

Install the Python dependencies:

```bash
uv sync --locked
```

## Initialize the database

Run from the repository root:

```bash
uv run --locked python -m src.migrations.runner
```

The runner creates the configured database if absent, then applies numbered SQL
migrations in order. Each migration and its checksum record commit together;
failure rolls back that migration and stops execution. Rerunning skips matching
applied migrations and rejects changed checksums. Add a new migration to make
further structural changes; do not edit applied SQL files.

Database creation runs separately because PostgreSQL requires it outside a
transaction. `000_create_database.sql` remains a standalone manual bootstrap;
the runner creates the configured database directly and does not execute or
record that helper. The first transactional migration creates the history table.
Initialization and bronze runs share a database lock so they cannot overlap.

## Run bronze

Place incoming CSV files directly inside `data/`, then run from the repository root:

```bash
uv run --locked python -m src.cli
```

The default connection is host `::1`, port `5432`, database `ticket_medallion`,
and the operating-system username. Override it using `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, and `PGPASSWORD`. Keep credentials outside Git.
The runner does not automatically load `.env` files.

## File lifecycle and retries

- CSV files directly in `data/` are pending, active, or interrupted inputs.
  Subdirectories are not scanned, and files run in filename order.
- Each file's SHA-256 identifies its content, regardless of filename.
  A committed file hash is skipped on subsequent arrivals.
- New file metadata and all its records commit in one transaction. Raw CSV
  record text and string-valued parsed fields are retained. Row hashes identify
  duplicate content; duplicate rows are preserved in bronze.
- Only after ingestion succeeds does the file move into `data/processed/`.
  A different file at the destination is never overwritten.
- Failed ingestion rolls back that file and leaves it pending. Earlier files
  remain committed, and the run stops with a nonzero exit status.
- If archiving fails after commit, retry the same command. The committed hash
  prevents duplicate ingestion while archiving is retried. An identical file
  already at the destination is safe to recover; different content needs a
  filename conflict resolved before retrying.
- A PostgreSQL session lock prevents concurrent CLI runs. Logs report filenames,
  inserted counts, skips, safe validation explanations, and database error codes
  (SQLSTATE). Raw database error messages are withheld to avoid exposing credentials
  or ticket content. No pending CSVs is a successful no-op.

Incoming files must be fully written before running the pipeline and remain
unchanged during processing. Archiving uses a hard link followed by source removal,
so `data/` and `data/processed/` must be on the same filesystem. Local DataFrame
construction holds records in driver memory; larger inputs need distributed reads.

The sample CSV remains version-controlled at `data/raw_tickets.csv`. Processing
moves its local copy into the ignored `data/processed/` directory, so Git reports
the original path as deleted afterward. Do not commit that deletion if retaining
the sample for fresh clones.

## Verified behavior

On 7 November 2026, the supplied CSV was ingested on macOS ARM64 with Java
17.0.16 and PostgreSQL 14.17. All 10,280 database records matched source text,
parsed fields, and hashes; the archived file was byte-identical. A rerun added
no records. Temporary checks also covered duplicate arrivals, transaction rollback,
archive recovery, and CLI failure handling; no permanent test suite is included.
