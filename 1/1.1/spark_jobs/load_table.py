#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, monotonically_increasing_id, row_number, to_date, trim, when
from pyspark.sql.types import DecimalType, IntegerType, StringType
from pyspark.sql.window import Window


DATE_FORMATS = ("yyyy-MM-dd", "dd.MM.yyyy", "dd-MM-yyyy", "MM/dd/yyyy")

TABLES = {
    "ft_balance_f": {
        "source": "/opt/airflow/files/ft_balance_f.csv",
        "target": "ds.ft_balance_f",
        "mode": "upsert",
        "keys": ["on_date", "account_rk"],
        "columns": {
            "on_date": "date",
            "account_rk": "decimal20",
            "currency_rk": "decimal20",
            "balance_out": "decimal23_8",
        },
    },
    "ft_posting_f": {
        "source": "/opt/airflow/files/ft_posting_f.csv",
        "target": "ds.ft_posting_f",
        "mode": "truncate_insert",
        "keys": [],
        "columns": {
            "oper_date": "date",
            "credit_account_rk": "decimal20",
            "debet_account_rk": "decimal20",
            "credit_amount": "decimal23_8",
            "debet_amount": "decimal23_8",
        },
    },
    "md_account_d": {
        "source": "/opt/airflow/files/md_account_d.csv",
        "target": "ds.md_account_d",
        "mode": "upsert",
        "keys": ["data_actual_date", "account_rk"],
        "columns": {
            "data_actual_date": "date",
            "data_actual_end_date": "date",
            "account_rk": "decimal20",
            "account_number": "decimal25",
            "char_type": "string",
            "currency_rk": "decimal20",
            "currency_code": "string",
        },
    },
    "md_currency_d": {
        "source": "/opt/airflow/files/md_currency_d.csv",
        "target": "ds.md_currency_d",
        "mode": "upsert",
        "keys": ["currency_rk", "data_actual_date"],
        "columns": {
            "currency_rk": "decimal20",
            "data_actual_date": "date",
            "data_actual_end_date": "date",
            "currency_code": "string",
            "code_iso_char": "string",
        },
    },
    "md_exchange_rate_d": {
        "source": "/opt/airflow/files/md_exchange_rate_d.csv",
        "target": "ds.md_exchange_rate_d",
        "mode": "upsert",
        "keys": ["data_actual_date", "currency_rk"],
        "columns": {
            "data_actual_date": "date",
            "data_actual_end_date": "date",
            "currency_rk": "decimal20",
            "reduced_cource": "decimal23_8",
            "code_iso_num": "string",
        },
    },
    "md_ledger_account_s": {
        "source": "/opt/airflow/files/md_ledger_account_s.csv",
        "target": "ds.md_ledger_account_s",
        "mode": "upsert",
        "keys": ["ledger_account", "start_date"],
        "columns": {
            "chapter": "string",
            "chapter_name": "string",
            "section_number": "integer",
            "section_name": "string",
            "subsection_name": "string",
            "ledger1_account": "integer",
            "ledger1_account_name": "string",
            "ledger_account": "integer",
            "ledger_account_name": "string",
            "characteristic": "string",
            "start_date": "date",
            "end_date": "date",
        },
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description="Load bank CSV file to PostgreSQL with PySpark.")
    parser.add_argument("--table", required=True, choices=sorted(TABLES))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source")
    parser.add_argument("--jdbc-url", default=os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/bank"))
    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "postgres"))
    parser.add_argument("--db-port", default=os.getenv("DB_PORT", "5432"))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "bank"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "postgres"))
    return parser.parse_args()


def pg_connect(args):
    return psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )


def execute_sql(args, sql, params=None, fetchone=False):
    with pg_connect(args) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetchone:
                return cur.fetchone()
            return cur.rowcount


def log_table_start(args, cfg, source):
    execute_sql(
        args,
        """
        INSERT INTO logs.etl_table_log (
            run_id, table_name, source_file, load_mode, status, started_at
        )
        VALUES (%s, %s, %s, %s, 'STARTED', now())
        """,
        (args.run_id, cfg["target"], source, cfg["mode"]),
    )


def log_table_finish(args, cfg, rows_read, rows_loaded, bad_date_rows):
    execute_sql(
        args,
        """
        UPDATE logs.etl_table_log
           SET status = 'SUCCESS',
               ended_at = now(),
               rows_read = %s,
               rows_loaded = %s,
               bad_date_rows = %s
         WHERE run_id = %s
           AND table_name = %s
           AND status = 'STARTED'
        """,
        (rows_read, rows_loaded, bad_date_rows, args.run_id, cfg["target"]),
    )


def log_table_failure(args, cfg, message):
    execute_sql(
        args,
        """
        UPDATE logs.etl_table_log
           SET status = 'FAILED',
               ended_at = now(),
               error_message = left(%s, 4000)
         WHERE run_id = %s
           AND table_name = %s
           AND status = 'STARTED'
        """,
        (message, args.run_id, cfg["target"]),
    )


def parse_date_expr(column_name):
    cleaned = trim(col(column_name).cast(StringType()))
    return coalesce(*[to_date(cleaned, fmt) for fmt in DATE_FORMATS])


def cast_column(column_name, column_type):
    cleaned = trim(col(column_name).cast(StringType()))
    nullable_value = when(cleaned == "", lit(None)).otherwise(cleaned)

    if column_type == "date":
        return parse_date_expr(column_name).alias(column_name)
    if column_type == "decimal20":
        return nullable_value.cast(DecimalType(20, 0)).alias(column_name)
    if column_type == "decimal25":
        return nullable_value.cast(DecimalType(25, 0)).alias(column_name)
    if column_type == "decimal23_8":
        return nullable_value.cast(DecimalType(23, 8)).alias(column_name)
    if column_type == "integer":
        return nullable_value.cast(IntegerType()).alias(column_name)
    if column_type == "string":
        return nullable_value.cast(StringType()).alias(column_name)
    raise ValueError(f"Unsupported column type: {column_type}")


def build_upsert_sql(target, staging_table, columns, keys):
    column_list = ", ".join(columns)
    update_columns = [c for c in columns if c not in keys]
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns])
    conflict_columns = ", ".join(keys)

    return f"""
        INSERT INTO {target} ({column_list})
        SELECT {column_list}
          FROM stg.{staging_table}
        ON CONFLICT ({conflict_columns})
        DO UPDATE SET {update_set}
    """


def build_truncate_insert_sql(target, staging_table, columns):
    column_list = ", ".join(columns)
    return f"""
        TRUNCATE TABLE {target};

        INSERT INTO {target} ({column_list})
        SELECT {column_list}
          FROM stg.{staging_table}
    """


def load_to_target(args, cfg, staging_table, columns):
    if cfg["mode"] == "upsert":
        sql = build_upsert_sql(cfg["target"], staging_table, columns, cfg["keys"])
    elif cfg["mode"] == "truncate_insert":
        sql = build_truncate_insert_sql(cfg["target"], staging_table, columns)
    else:
        raise ValueError(f"Unsupported load mode: {cfg['mode']}")

    return execute_sql(args, sql)


def table_row_count(args, target):
    result = execute_sql(args, f"SELECT count(*) FROM {target}", fetchone=True)
    return int(result[0])


def main():
    args = parse_args()
    cfg = TABLES[args.table]
    source = args.source or cfg["source"]
    columns = list(cfg["columns"].keys())
    staging_table = f"{args.table}_{args.run_id.replace('-', '')[:24]}"

    spark = (
        SparkSession.builder.appName(f"bank-etl-{args.table}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    rows_read = 0
    rows_loaded = 0
    bad_date_rows = 0

    try:
        log_table_start(args, cfg, source)

        raw_df = (
            spark.read.option("header", "true")
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .csv(source)
        )
        raw_df = raw_df.toDF(*[c.lower() for c in raw_df.columns])

        missing_columns = sorted(set(columns) - set(raw_df.columns))
        if missing_columns:
            raise ValueError(f"Missing columns in {source}: {', '.join(missing_columns)}")

        raw_df = raw_df.select(*columns).withColumn("_etl_row_id", monotonically_increasing_id())
        rows_read = raw_df.count()
        parsed_df = raw_df.select(
            "_etl_row_id",
            *[cast_column(name, cfg["columns"][name]) for name in columns],
        )

        bad_date_conditions = []
        for name, column_type in cfg["columns"].items():
            if column_type == "date":
                raw_value = trim(col(f"raw_{name}").cast(StringType()))
                bad_date_conditions.append(raw_value.isNotNull() & (raw_value != "") & col(name).isNull())

        if bad_date_conditions:
            date_columns = [name for name, column_type in cfg["columns"].items() if column_type == "date"]
            validation_df = raw_df.select(
                "_etl_row_id",
                *[col(name).alias(f"raw_{name}") for name in date_columns],
            ).join(parsed_df.select("_etl_row_id", *date_columns), on="_etl_row_id", how="inner")
            condition = bad_date_conditions[0]
            for item in bad_date_conditions[1:]:
                condition = condition | item
            bad_date_rows = validation_df.where(condition).count()

        if bad_date_rows:
            raise ValueError(f"Found {bad_date_rows} rows with invalid date values in {source}")

        final_df = parsed_df
        if cfg["mode"] == "upsert":
            window = Window.partitionBy(*cfg["keys"]).orderBy(col("_etl_row_id").desc())
            final_df = (
                final_df.withColumn("_etl_row_number", row_number().over(window))
                .where(col("_etl_row_number") == 1)
                .drop("_etl_row_number")
            )

        (
            final_df.drop("_etl_row_id").write.format("jdbc")
            .option("url", args.jdbc_url)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", f"stg.{staging_table}")
            .option("user", args.db_user)
            .option("password", args.db_password)
            .mode("overwrite")
            .save()
        )

        load_to_target(args, cfg, staging_table, columns)
        rows_loaded = table_row_count(args, cfg["target"])
        log_table_finish(args, cfg, rows_read, rows_loaded, bad_date_rows)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        try:
            log_table_failure(args, cfg, message)
        except Exception:
            pass
        raise
    finally:
        try:
            execute_sql(args, f"DROP TABLE IF EXISTS stg.{staging_table}")
        except Exception:
            pass
        spark.stop()

    print(
        f"{datetime.utcnow().isoformat()}Z loaded {args.table}: "
        f"rows_read={rows_read}, target_rows={rows_loaded}, bad_date_rows={bad_date_rows}"
    )


if __name__ == "__main__":
    sys.exit(main())
