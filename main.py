#!/usr/bin/env python3
"""
CSV to Parquet loader for Trino/MinIO.

Loads a CSV file into a Trino-managed Iceberg table stored as Parquet
in MinIO (S3-compatible storage).
"""

import logging
import os
from typing import Any

import pandas as pd
from trino.dbapi import Connection, connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
TRINO_HOST = os.getenv("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "trino")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "nessie")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "healthcare")
CSV_PATH = os.getenv("CSV_PATH", "./fdny_firehouse_listing.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))


def get_trino_connection() -> Connection:
    """Create and return a Trino database connection."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
    )


def sql_escape(value: Any) -> str:
    """Escape a Python value for safe inclusion in a SQL literal.

    Args:
        value: The value to escape.

    Returns:
        A SQL-safe string representation (e.g., 'NULL' or 'value').
    """
    if pd.isna(value):
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def load_csv_to_trino(csv_path: str, table_name: str) -> None:
    """Load a CSV file into a Trino table as Parquet.

    Args:
        csv_path: Path to the CSV file.
        table_name: Fully qualified Trino table name (catalog.schema.table).
    """
    logger.info("Loading CSV from %s", csv_path)
    df = pd.read_csv(csv_path)
    df.columns = [c.replace(" ", "_").replace("/", "_").lower() for c in df.columns]
    rows = df.to_numpy().tolist()

    conn = get_trino_connection()
    cur = conn.cursor()
    logger.info("Connected to Trino as user: %s", TRINO_USER)

    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        columns_sql = ", ".join(f'"{col}" VARCHAR' for col in df.columns)
        cur.execute(
            f"""
            CREATE TABLE {table_name} (
                {columns_sql}
            )
            WITH (
                format = 'PARQUET'
            )
            """
        )
        logger.info("Table %s created", table_name)

        total = len(rows)
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            values_sql = ", ".join(
                "(" + ", ".join(sql_escape(v) for v in row) + ")" for row in batch
            )
            cur.execute(f"INSERT INTO {table_name} VALUES {values_sql}")
            logger.info("Inserted %d / %d rows", i + len(batch), total)

        logger.info("Successfully loaded %d rows into %s", total, table_name)
    finally:
        conn.close()


def main() -> None:
    """Main entry point."""
    table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.fdny_firehouse_silver"
    load_csv_to_trino(CSV_PATH, table_name)


if __name__ == "__main__":
    main()
