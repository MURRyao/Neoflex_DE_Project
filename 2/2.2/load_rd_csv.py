#!/usr/bin/env python3
"""Load task 2.2 CSV extracts with simple ETL logging.

The script follows the approach from task 1:
1. load CSV files to staging tables;
2. load target RD tables from staging;
3. write one run log and one table log per loaded table.
"""

from __future__ import annotations

import argparse
import csv
import os
import uuid
from pathlib import Path

import psycopg2
from psycopg2.extensions import quote_ident


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENCODING = "cp1251"

TABLES = [
    {
        "name": "deal_info",
        "source": PROJECT_ROOT / "data/loan_holiday_info/deal_info.csv",
        "staging": "stg.deal_info_csv",
        "target": "rd.deal_info",
    },
    {
        "name": "product",
        "source": PROJECT_ROOT / "data/loan_holiday_info/product_info.csv",
        "staging": "stg.product_csv",
        "target": "rd.product",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load task 2.2 CSV extracts.")
    parser.add_argument(
        "--step",
        choices=("stage", "rd", "all"),
        required=True,
        help="stage = CSV to stg, rd = stg to RD, all = both steps",
    )
    return parser.parse_args()


def db_params() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "15432")),
        "dbname": os.getenv("DB_NAME", "bank_2"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }


def qname(cursor, table_name: str) -> str:
    schema_name, relation_name = table_name.split(".", 1)
    return ".".join(
        [
            quote_ident(schema_name, cursor.connection),
            quote_ident(relation_name, cursor.connection),
        ]
    )


def qcols(cursor, columns: list[str]) -> str:
    return ", ".join(quote_ident(column, cursor.connection) for column in columns)


def csv_columns(path: Path) -> list[str]:
    with path.open("r", encoding=ENCODING, newline="") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        return [column.strip().lower() for column in next(reader)]


def csv_rows_count(path: Path) -> int:
    with path.open("r", encoding=ENCODING, newline="") as csv_file:
        return max(sum(1 for _ in csv_file) - 1, 0)


def ensure_objects(cursor) -> None:
    cursor.execute("CREATE SCHEMA IF NOT EXISTS stg")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS logs")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS logs.etl_run_log (
            run_id uuid PRIMARY KEY,
            dag_id text NOT NULL,
            run_type text NOT NULL DEFAULT 'manual',
            status text NOT NULL,
            started_at timestamp NOT NULL,
            ended_at timestamp,
            duration_sec numeric(16, 3),
            total_rows_read bigint DEFAULT 0,
            total_rows_loaded bigint DEFAULT 0,
            error_message text
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS logs.etl_table_log (
            id bigserial PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES logs.etl_run_log(run_id),
            table_name text NOT NULL,
            source_file text NOT NULL,
            load_mode text NOT NULL,
            status text NOT NULL,
            started_at timestamp NOT NULL,
            ended_at timestamp,
            rows_read bigint DEFAULT 0,
            rows_loaded bigint DEFAULT 0,
            bad_date_rows bigint DEFAULT 0,
            error_message text
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_etl_table_log_run_id
            ON logs.etl_table_log(run_id)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stg.deal_info_csv
        (LIKE rd.deal_info INCLUDING DEFAULTS)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stg.product_csv
        (LIKE rd.product INCLUDING DEFAULTS)
        """
    )


def start_run(cursor) -> uuid.UUID:
    run_id = uuid.uuid4()
    cursor.execute(
        """
        INSERT INTO logs.etl_run_log (
            run_id, dag_id, run_type, status, started_at
        )
        VALUES (%s, 'task_2_2.load_rd_csv', 'python', 'STARTED', clock_timestamp())
        """,
        (str(run_id),),
    )
    return run_id


def start_table_log(cursor, run_id: uuid.UUID, table_name: str, source_file: str, load_mode: str) -> None:
    cursor.execute(
        """
        INSERT INTO logs.etl_table_log (
            run_id, table_name, source_file, load_mode, status, started_at
        )
        VALUES (%s, %s, %s, %s, 'STARTED', clock_timestamp())
        """,
        (str(run_id), table_name, source_file, load_mode),
    )


def finish_table_log(
    cursor,
    run_id: uuid.UUID,
    table_name: str,
    status: str,
    rows_read: int,
    rows_loaded: int,
    error_message: str | None = None,
) -> None:
    cursor.execute(
        """
        UPDATE logs.etl_table_log
           SET status = %s,
               ended_at = clock_timestamp(),
               rows_read = %s,
               rows_loaded = %s,
               error_message = %s
         WHERE run_id = %s
           AND table_name = %s
           AND status = 'STARTED'
        """,
        (status, rows_read, rows_loaded, error_message, str(run_id), table_name),
    )


def finish_run_log(
    cursor,
    run_id: uuid.UUID,
    status: str,
    rows_read: int,
    rows_loaded: int,
    error_message: str | None = None,
) -> None:
    cursor.execute(
        """
        UPDATE logs.etl_run_log
           SET status = %s,
               ended_at = clock_timestamp(),
               duration_sec = extract(epoch FROM clock_timestamp() - started_at),
               total_rows_read = %s,
               total_rows_loaded = %s,
               error_message = %s
         WHERE run_id = %s
        """,
        (status, rows_read, rows_loaded, error_message, str(run_id)),
    )


def load_csv_to_staging(cursor, table_cfg: dict) -> tuple[int, int]:
    source = table_cfg["source"]
    target = qname(cursor, table_cfg["staging"])
    columns = csv_columns(source)
    column_list = qcols(cursor, columns)

    rows_read = csv_rows_count(source)
    cursor.execute(f"TRUNCATE TABLE {target}")

    copy_sql = (
        f"COPY {target} ({column_list}) "
        "FROM STDIN WITH CSV HEADER DELIMITER ','"
    )
    with source.open("r", encoding=ENCODING, newline="") as csv_file:
        cursor.copy_expert(copy_sql, csv_file)

    return rows_read, rows_read


def load_staging_to_rd(cursor, table_cfg: dict) -> tuple[int, int]:
    source = qname(cursor, table_cfg["staging"])
    target = qname(cursor, table_cfg["target"])

    cursor.execute(f"SELECT count(*) FROM {source}")
    rows_read = cursor.fetchone()[0]

    cursor.execute(f"TRUNCATE TABLE {target}")
    cursor.execute(f"INSERT INTO {target} SELECT * FROM {source}")
    rows_loaded = cursor.rowcount

    return rows_read, rows_loaded


def run_step(cursor, run_id: uuid.UUID, step: str) -> tuple[int, int]:
    total_rows_read = 0
    total_rows_loaded = 0

    for table_cfg in TABLES:
        if step == "stage":
            table_name = table_cfg["staging"]
            source_file = f"csv:{table_cfg['source']}"
            load_mode = "truncate_copy_from_csv"
            loader = load_csv_to_staging
        else:
            table_name = table_cfg["target"]
            source_file = table_cfg["staging"]
            load_mode = "truncate_insert_from_stg"
            loader = load_staging_to_rd

        start_table_log(cursor, run_id, table_name, source_file, load_mode)
        try:
            rows_read, rows_loaded = loader(cursor, table_cfg)
            finish_table_log(cursor, run_id, table_name, "SUCCESS", rows_read, rows_loaded)
        except Exception as exc:
            finish_table_log(cursor, run_id, table_name, "FAILED", 0, 0, str(exc)[:4000])
            raise

        total_rows_read += rows_read
        total_rows_loaded += rows_loaded

    return total_rows_read, total_rows_loaded


def main() -> None:
    args = parse_args()
    steps = ["stage", "rd"] if args.step == "all" else [args.step]

    with psycopg2.connect(**db_params()) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            ensure_objects(cursor)
            run_id = start_run(cursor)
            total_rows_read = 0
            total_rows_loaded = 0

            try:
                for step in steps:
                    rows_read, rows_loaded = run_step(cursor, run_id, step)
                    total_rows_read += rows_read
                    total_rows_loaded += rows_loaded

                finish_run_log(cursor, run_id, "SUCCESS", total_rows_read, total_rows_loaded)
                print(
                    f"Run {run_id} finished: "
                    f"rows_read={total_rows_read}, rows_loaded={total_rows_loaded}"
                )
            except Exception as exc:
                finish_run_log(
                    cursor,
                    run_id,
                    "FAILED",
                    total_rows_read,
                    total_rows_loaded,
                    str(exc)[:4000],
                )
                raise


if __name__ == "__main__":
    main()
