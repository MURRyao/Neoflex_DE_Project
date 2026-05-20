import os
import time
import uuid
from pathlib import Path

import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DAG_ID = "bank_csv_to_postgres_pyspark"

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "bank")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
JDBC_URL = os.getenv("JDBC_URL", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")

PROJECT_ROOT = Path("/opt/airflow")
SQL_DIR = PROJECT_ROOT / "sql"
SPARK_JOB = PROJECT_ROOT / "spark_jobs" / "load_table.py"
SPARK_SUBMIT = Path("/home/airflow/.local/bin/spark-submit")
JDBC_JAR = PROJECT_ROOT / "jars" / "postgresql.jar"

TABLE_ORDER = [
    "md_currency_d",
    "md_exchange_rate_d",
    "md_ledger_account_s",
    "md_account_d",
    "ft_balance_f",
    "ft_posting_f",
]


def pg_connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def execute_sql(sql, params=None, fetchone=False):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetchone:
                return cur.fetchone()
            return cur.rowcount


def init_database():
    for path in sorted(SQL_DIR.glob("*.sql")):
        execute_sql(path.read_text(encoding="utf-8"))


def start_run(**context):
    run_id = str(uuid.uuid4())
    execute_sql(
        """
        INSERT INTO logs.etl_run_log (
            run_id, dag_id, run_type, status, started_at
        )
        VALUES (%s, %s, %s, 'STARTED', now())
        """,
        (
            run_id,
            DAG_ID,
            context["dag_run"].run_type if context.get("dag_run") else "manual",
        ),
    )
    return run_id


def finish_run_success(**context):
    run_id = context["ti"].xcom_pull(task_ids="start_log")
    execute_sql(
        """
        UPDATE logs.etl_run_log run_log
           SET status = 'SUCCESS',
               ended_at = now(),
               duration_sec = extract(epoch from now() - started_at),
               total_rows_read = stats.total_rows_read,
               total_rows_loaded = stats.total_rows_loaded
          FROM (
                SELECT coalesce(sum(rows_read), 0) AS total_rows_read,
                       coalesce(sum(rows_loaded), 0) AS total_rows_loaded
                  FROM logs.etl_table_log
                 WHERE run_id = %s
                   AND status = 'SUCCESS'
               ) stats
         WHERE run_log.run_id = %s
        """,
        (run_id, run_id),
    )


def mark_run_failed(context):
    task_instance = context.get("ti")
    run_id = task_instance.xcom_pull(task_ids="start_log") if task_instance else None
    if not run_id:
        return

    exception = context.get("exception")
    message = f"{type(exception).__name__}: {exception}" if exception else "Unknown Airflow failure"
    execute_sql(
        """
        UPDATE logs.etl_run_log
           SET status = 'FAILED',
               ended_at = now(),
               duration_sec = extract(epoch from now() - started_at),
               error_message = left(%s, 4000)
         WHERE run_id = %s
           AND status = 'STARTED'
        """,
        (message, run_id),
    )


default_args = {
    "owner": "neoflex",
    "retries": 0,
    "on_failure_callback": mark_run_failed,
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["neoflex", "pyspark", "postgres"],
) as dag:
    init_db = PythonOperator(
        task_id="init_database",
        python_callable=init_database,
    )

    start_log = PythonOperator(
        task_id="start_log",
        python_callable=start_run,
    )

    sleep_after_start_log = PythonOperator(
        task_id="sleep_5_seconds_after_start_log",
        python_callable=lambda: time.sleep(5),
    )

    previous_task = sleep_after_start_log
    load_tasks = []

    for table_name in TABLE_ORDER:
        task = BashOperator(
            task_id=f"load_{table_name}",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "--master local[*] "
                f"--jars {JDBC_JAR} "
                f"{SPARK_JOB} "
                f"--table {table_name} "
                "--run-id '{{ ti.xcom_pull(task_ids=\"start_log\") }}'"
            ),
            env={
                "DB_HOST": DB_HOST,
                "DB_PORT": DB_PORT,
                "DB_NAME": DB_NAME,
                "DB_USER": DB_USER,
                "DB_PASSWORD": DB_PASSWORD,
                "JDBC_URL": JDBC_URL,
            },
            append_env=True,
        )
        previous_task >> task
        previous_task = task
        load_tasks.append(task)

    finish_log = PythonOperator(
        task_id="finish_log_success",
        python_callable=finish_run_success,
    )

    init_db >> start_log >> sleep_after_start_log
    previous_task >> finish_log
