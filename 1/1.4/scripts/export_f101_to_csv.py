import argparse
from pathlib import Path

from db_utils import (
    DEFAULT_EXPORT_FILE,
    EXPORTS_DIR,
    connect,
    create_spark,
    finish_run_log,
    finish_table_log,
    insert_run_log,
    insert_table_log,
    jdbc_properties,
    jdbc_url,
    setup_logging,
    write_single_csv,
)


SOURCE_TABLE = "dm.dm_f101_round_f"
DAG_ID = "python.export_f101_to_csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export dm.dm_f101_round_f to CSV with a header row."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_EXPORT_FILE,
        help="CSV output path. Default: exports/dm_f101_round_f.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    logger = setup_logging()
    logger.info("Starting export from %s to %s", SOURCE_TABLE, output_path)

    run_id = None
    rows_count = 0
    spark = None

    with connect() as conn:
        with conn.cursor() as cursor:
            run_id = insert_run_log(cursor, DAG_ID)
            insert_table_log(
                cursor,
                run_id,
                SOURCE_TABLE,
                f"csv:{output_path}",
                "export_to_csv",
            )
            conn.commit()

            try:
                spark = create_spark("f101-export-to-csv")
                df = spark.read.jdbc(
                    url=jdbc_url(),
                    table=SOURCE_TABLE,
                    properties=jdbc_properties(),
                )

                rows_count = df.count()
                if rows_count == 0:
                    raise RuntimeError(
                        f"{SOURCE_TABLE} is empty. Run task 1.3 before export."
                    )

                ordered_df = df.orderBy(
                    "from_date", "to_date", "ledger_account", "characteristic"
                )
                write_single_csv(ordered_df, output_path)

                finish_table_log(
                    cursor,
                    run_id,
                    SOURCE_TABLE,
                    "SUCCESS",
                    rows_read=rows_count,
                    rows_loaded=rows_count,
                )
                finish_run_log(
                    cursor,
                    run_id,
                    "SUCCESS",
                    rows_read=rows_count,
                    rows_loaded=rows_count,
                )
                logger.info("Export completed successfully: %s rows", rows_count)
            except Exception as exc:
                conn.rollback()
                error_message = str(exc)[:4000]
                finish_table_log(
                    cursor,
                    run_id,
                    SOURCE_TABLE,
                    "FAILED",
                    rows_read=rows_count,
                    rows_loaded=0,
                    error_message=error_message,
                )
                finish_run_log(
                    cursor,
                    run_id,
                    "FAILED",
                    rows_read=rows_count,
                    rows_loaded=0,
                    error_message=error_message,
                )
                conn.commit()
                logger.exception("Export failed")
                raise
            finally:
                if spark is not None:
                    spark.stop()


if __name__ == "__main__":
    main()
