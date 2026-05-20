import argparse
import os
from pathlib import Path

from db_utils import (
    DEFAULT_MODIFIED_FILE,
    PROJECT_ROOT,
    connect,
    count_rows,
    create_spark,
    finish_run_log,
    finish_table_log,
    insert_run_log,
    insert_table_log,
    jdbc_properties,
    jdbc_url,
    setup_logging,
)


TARGET_TABLE = "dm.dm_f101_round_f_v2"
DAG_ID = "python.import_f101_from_csv"
CREATE_TABLE_SQL = PROJECT_ROOT / "sql" / "create_f101_v2_table.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import modified dm.dm_f101_round_f CSV to dm.dm_f101_round_f_v2."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_MODIFIED_FILE,
        help="CSV input path. Default: exports/dm_f101_round_f_modified.csv",
    )
    return parser.parse_args()


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as csv_file:
        return max(sum(1 for _ in csv_file) - 1, 0)


def main() -> None:
    args = parse_args()
    input_path = args.input.resolve()

    logger = setup_logging()
    logger.info("Starting import from %s to %s", input_path, TARGET_TABLE)

    if not input_path.exists():
        raise FileNotFoundError(
            f"{input_path} does not exist. Run modify_f101_csv_sample.py first."
        )

    rows_read = count_csv_rows(input_path)
    run_id = None
    spark = None
    staging_table = f"dm.dm_f101_round_f_v2_stg_{rows_read}_{os.getpid()}"

    with connect() as conn:
        with conn.cursor() as cursor:
            run_id = insert_run_log(cursor, DAG_ID)
            insert_table_log(
                cursor,
                run_id,
                TARGET_TABLE,
                f"csv:{input_path}",
                "truncate_insert_from_csv",
            )
            conn.commit()

            try:
                cursor.execute(CREATE_TABLE_SQL.read_text(encoding="utf-8"))
                conn.commit()

                spark = create_spark("f101-import-from-csv")
                target_df = spark.read.jdbc(
                    url=jdbc_url(),
                    table=TARGET_TABLE,
                    properties=jdbc_properties(),
                )
                csv_df = (
                    spark.read.option("header", "true")
                    .option("encoding", "UTF-8")
                    .schema(target_df.schema)
                    .csv(str(input_path))
                )

                spark_rows = csv_df.count()
                if spark_rows != rows_read:
                    raise RuntimeError(
                        f"CSV rows count ({rows_read}) differs from "
                        f"Spark rows count ({spark_rows})"
                    )

                (
                    csv_df.write.format("jdbc")
                    .option("url", jdbc_url())
                    .option("driver", "org.postgresql.Driver")
                    .option("dbtable", staging_table)
                    .option("user", jdbc_properties()["user"])
                    .option("password", jdbc_properties()["password"])
                    .mode("overwrite")
                    .save()
                )

                columns = ", ".join(csv_df.columns)
                cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
                cursor.execute(
                    f"""
                    INSERT INTO {TARGET_TABLE} ({columns})
                    SELECT {columns}
                      FROM {staging_table}
                    """
                )

                rows_loaded = count_rows(cursor, TARGET_TABLE)
                if rows_loaded != rows_read:
                    raise RuntimeError(
                        f"CSV rows count ({rows_read}) differs from "
                        f"loaded rows count ({rows_loaded})"
                    )

                finish_table_log(
                    cursor,
                    run_id,
                    TARGET_TABLE,
                    "SUCCESS",
                    rows_read=rows_read,
                    rows_loaded=rows_loaded,
                )
                finish_run_log(
                    cursor,
                    run_id,
                    "SUCCESS",
                    rows_read=rows_read,
                    rows_loaded=rows_loaded,
                )
                logger.info("Import completed successfully: %s rows", rows_loaded)
            except Exception as exc:
                conn.rollback()
                error_message = str(exc)[:4000]
                finish_table_log(
                    cursor,
                    run_id,
                    TARGET_TABLE,
                    "FAILED",
                    rows_read=rows_read,
                    rows_loaded=0,
                    error_message=error_message,
                )
                finish_run_log(
                    cursor,
                    run_id,
                    "FAILED",
                    rows_read=rows_read,
                    rows_loaded=0,
                    error_message=error_message,
                )
                conn.commit()
                logger.exception("Import failed")
                raise
            finally:
                if spark is not None:
                    spark.stop()
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    pass


if __name__ == "__main__":
    main()
