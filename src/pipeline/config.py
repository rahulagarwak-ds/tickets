import os
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parents[2] / "data"
PROCESSED_DIR = DATA_DIR / "processed"

DATABASE_CONFIG = {
    "host": os.environ.get("PGHOST", "::1"),
    "port": os.environ.get("PGPORT", "5432"),
    "dbname": os.environ.get("PGDATABASE", "ticket_medallion"),
    "connect_timeout": 5,
}

# psycopg reads PGUSER and PGPASSWORD directly from the environment.
