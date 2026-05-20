from __future__ import annotations

import logging
import os
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg2


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPORTS_DIR = PROJECT_ROOT / "exports"
JARS_DIR = PROJECT_ROOT / "jars"
DEFAULT_POSTGRES_JDBC_JAR = JARS_DIR / "postgresql.jar"
LOGS_DIR = PROJECT_ROOT / "logs"
DEFAULT_EXPORT_FILE = EXPORTS_DIR / "dm_f101_round_f.csv"
DEFAULT_MODIFIED_FILE = EXPORTS_DIR / "dm_f101_round_f_modified.csv"
LOG_FILE = LOGS_DIR / "f101_csv_exchange.log"


def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("f101_csv_exchange")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s - %(message)s"
        )

        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def db_params() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "15432")),
        "dbname": os.getenv("DB_NAME", "bank"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }


def jdbc_url() -> str:
    params = db_params()
    return os.getenv(
        "JDBC_URL",
        f"jdbc:postgresql://{params['host']}:{params['port']}/{params['dbname']}",
    )


def jdbc_properties() -> dict[str, str]:
    params = db_params()
    return {
        "user": params["user"],
        "password": params["password"],
        "driver": "org.postgresql.Driver",
    }


def create_spark(app_name: str):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
    )

    jdbc_jar = (
        os.getenv("POSTGRES_JDBC_JAR")
        or os.getenv("JDBC_JAR")
        or (
            str(DEFAULT_POSTGRES_JDBC_JAR)
            if DEFAULT_POSTGRES_JDBC_JAR.exists()
            else None
        )
    )
    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)

    return builder.getOrCreate()


def write_single_csv(df, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = output_path.parent / f".{output_path.name}.spark_tmp"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_path.is_dir():
        shutil.rmtree(output_path)
    elif output_path.exists():
        output_path.unlink()

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(str(temp_dir))
    )

    part_files = sorted(temp_dir.glob("part-*.csv"))
    if len(part_files) != 1:
        raise RuntimeError(f"Expected one Spark CSV part file in {temp_dir}")

    shutil.move(str(part_files[0]), output_path)
    shutil.rmtree(temp_dir)


@contextmanager
def connect():
    conn = psycopg2.connect(**db_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def count_rows(cursor, table_name: str) -> int:
    cursor.execute(f"SELECT count(*) FROM {table_name}")
    return cursor.fetchone()[0]


def insert_run_log(cursor, dag_id: str, run_type: str = "python") -> uuid.UUID:
    run_id = uuid.uuid4()
    cursor.execute(
        """
        INSERT INTO logs.etl_run_log (
            run_id, dag_id, run_type, status, started_at
        )
        VALUES (%s, %s, %s, 'STARTED', clock_timestamp())
        """,
        (str(run_id), dag_id, run_type),
    )
    return run_id


def insert_table_log(
    cursor,
    run_id: uuid.UUID,
    table_name: str,
    source_file: str,
    load_mode: str,
) -> None:
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
    rows_read: int = 0,
    rows_loaded: int = 0,
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
        (
            status,
            rows_read,
            rows_loaded,
            error_message,
            str(run_id),
            table_name,
        ),
    )


def finish_run_log(
    cursor,
    run_id: uuid.UUID,
    status: str,
    rows_read: int = 0,
    rows_loaded: int = 0,
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
