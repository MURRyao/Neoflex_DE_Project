import argparse
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pyspark.sql.functions import (
    col,
    format_string,
    lit,
    monotonically_increasing_id,
    row_number,
    when,
)
from pyspark.sql.window import Window

from db_utils import (
    DEFAULT_EXPORT_FILE,
    DEFAULT_MODIFIED_FILE,
    create_spark,
    setup_logging,
    write_single_csv,
)


CHANGES = (
    ("balance_in_total", Decimal("1000.00000000")),
    ("turn_deb_total", Decimal("2000.00000000")),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a modified copy of exported dm.dm_f101_round_f CSV."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_EXPORT_FILE,
        help="Source CSV path. Default: exports/dm_f101_round_f.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_MODIFIED_FILE,
        help="Modified CSV path. Default: exports/dm_f101_round_f_modified.csv",
    )
    return parser.parse_args()


def add_delta(value: str, delta: Decimal) -> str:
    try:
        current = Decimal(value or "0")
    except InvalidOperation as exc:
        raise ValueError(f"Cannot modify non-numeric value {value!r}") from exc
    return f"{current + delta:.8f}"


def changed_value_expr(column_name: str, delta: Decimal):
    return format_string(
        "%.8f",
        (
            col(column_name).cast("decimal(38,8)")
            + lit(str(delta)).cast("decimal(38,8)")
        ).cast("double"),
    )


def main() -> None:
    args = parse_args()
    input_path = args.input.resolve()
    output_path = args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger = setup_logging()
    logger.info("Starting CSV modification: %s -> %s", input_path, output_path)

    if not input_path.exists():
        raise FileNotFoundError(
            f"{input_path} does not exist. Run export_f101_to_csv.py first."
        )

    spark = create_spark("f101-modify-csv-sample")

    try:
        df = (
            spark.read.option("header", "true")
            .option("encoding", "UTF-8")
            .csv(str(input_path))
        )

        if not df.columns:
            raise RuntimeError(f"{input_path} has no CSV header")
        row_count = df.count()
        if row_count < len(CHANGES):
            raise RuntimeError(
                f"{input_path} must contain at least {len(CHANGES)} data rows"
            )

        window = Window.orderBy(monotonically_increasing_id())
        modified_df = df.coalesce(1).withColumn(
            "_etl_row_number", row_number().over(window)
        )

        for index, (column_name, delta) in enumerate(CHANGES, start=1):
            if column_name not in df.columns:
                raise RuntimeError(f"CSV has no required column {column_name!r}")

            modified_df = modified_df.withColumn(
                column_name,
                when(
                    col("_etl_row_number") == index,
                    changed_value_expr(column_name, delta),
                ).otherwise(col(column_name)),
            )
            logger.info("Changed row %s, column %s by %s", index, column_name, delta)

        final_df = modified_df.orderBy("_etl_row_number").drop("_etl_row_number")
        write_single_csv(final_df, output_path)
        logger.info("CSV modification completed: %s rows written", row_count)
    finally:
        spark.stop()


def validate_decimal_modifier() -> None:
    for _, delta in CHANGES:
        add_delta("0", delta)


if __name__ == "__main__":
    validate_decimal_modifier()
    main()
