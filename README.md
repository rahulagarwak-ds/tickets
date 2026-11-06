# Ticket medallion

This repository currently contains the agreed dependency stack.
The pipeline and assessment agents are not implemented yet.
No Docker or Compose is used.

## Prerequisites

- uv; Python 3.12 is selected through `.python-version` and `pyproject.toml`.
- Java 17 or 21 on `PATH`, or a valid `JAVA_HOME`.
- A running PostgreSQL server and an existing database. The connecting user needs
  permission to create the project's bronze, silver, and gold schemas.

## Dependencies

| Dependency | Purpose |
| --- | --- |
| Python 3.12 (tested: 3.12.7) | Project runtime |
| PySpark 4.3.0 | Local transformations and JDBC access |
| psycopg 3.3.8 with binary extras | PostgreSQL control queries; no local compilation |
| LangGraph 1.2.20 | Agent workflows |
| LangChain 1.4.9 | LLM integration |
| langchain-openai 1.7.4 | Installed provider adapter; provider/model choice remains provisional |
| PostgreSQL JDBC 42.7.14 | Spark's JVM database connection; pinned separately from uv |

Exact Python library versions are recorded in `uv.lock`. Java, PostgreSQL, and
Spark's JDBC driver are managed separately from uv.

Install the Python dependencies:

```bash
uv sync --locked
```

The environment was verified on 6 November 2026 on macOS ARM64, including local
Spark execution and PostgreSQL JDBC connectivity. The one-off verification
script has been removed. Pipeline run instructions will be added with the implementation.
