"""Build privacy-protected daily disease surveillance aggregates."""

import argparse
import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

DIMENSIONS = [
    "encounter_date",
    "district_code",
    "disease",
    "case_classification",
]


def aggregate_daily_surveillance(
    silver: DataFrame,
    min_public_count: int = 5,
) -> DataFrame:
    """Aggregate daily surveillance with primary and complementary suppression."""

    if min_public_count < 2:
        raise ValueError("min_public_count must be at least 2")

    required = set(DIMENSIONS)
    missing = required - set(silver.columns)
    if missing:
        raise ValueError(
            f"Silver data is missing required columns: {sorted(missing)}"
        )

    parent_dimensions = ["encounter_date", "district_code", "disease"]
    parent_window = Window.partitionBy(*parent_dimensions)
    visible_rank_window = (
        Window.partitionBy(*parent_dimensions)
        .orderBy(
            F.col("_exact_case_count").asc(),
            F.col("case_classification").asc(),
        )
    )

    aggregated = (
        silver
        .select(*DIMENSIONS)
        .groupBy(*DIMENSIONS)
        .agg(F.count(F.lit(1)).alias("_exact_case_count"))
        .withColumn(
            "_parent_count",
            F.sum("_exact_case_count").over(parent_window),
        )
        .withColumn(
            "_primary_suppressed",
            F.col("_exact_case_count") < min_public_count,
        )
        .withColumn(
            "_primary_suppressed_count",
            F.sum(
                F.when(F.col("_primary_suppressed"), 1).otherwise(0)
            ).over(parent_window),
        )
        .withColumn(
            "_visible_rank",
            F.when(
                ~F.col("_primary_suppressed"),
                F.row_number().over(visible_rank_window),
            ),
        )
    )

    # If exactly one child count is hidden but its parent total is public,
    # hide the smallest visible child count as well.
    protected = aggregated.withColumn(
        "_complementary_suppressed",
        (
            (F.col("_parent_count") >= min_public_count)
            & (F.col("_primary_suppressed_count") == 1)
            & (~F.col("_primary_suppressed"))
            & (F.col("_visible_rank") == 1)
        ),
    )

    return protected.select(
        *DIMENSIONS,
        F.when(
            ~F.col("_primary_suppressed")
            & ~F.col("_complementary_suppressed"),
            F.col("_exact_case_count"),
        ).otherwise(F.lit(None).cast("long")).alias("case_count"),
        (
            F.col("_primary_suppressed")
            | F.col("_complementary_suppressed")
        ).alias("count_suppressed"),
    )


def build_daily_surveillance(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    batch_id: str,
    min_public_count: int = 5,
) -> dict:
    """Read Silver Parquet and write a batch-specific Gold aggregate."""

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", batch_id):
        raise ValueError("Invalid batch ID")

    silver = spark.read.parquet(silver_path)
    public_output = aggregate_daily_surveillance(
        silver,
        min_public_count=min_public_count,
    )

    output_dir = (
        Path(output_path)
        / "daily_surveillance"
        / f"batch_id={batch_id}"
    )

    # A rerun replaces this batch's output instead of appending duplicates.
    public_output.write.mode("overwrite").parquet(str(output_dir))

    row_count = public_output.count()
    suppressed_count = public_output.filter(
        F.col("count_suppressed")
    ).count()

    metrics = {
        "batch_id": batch_id,
        "source_records": silver.count(),
        "aggregate_rows": row_count,
        "suppressed_aggregate_rows": suppressed_count,
        "minimum_public_count": min_public_count,
        "output_path": str(output_dir),
    }

    metrics_dir = (
        Path(output_path)
        / "quality"
        / "daily_surveillance"
        / f"batch_id={batch_id}"
    )
    metrics_df = spark.createDataFrame(
        [(json.dumps(metrics),)],
        ["metrics_json"],
    )
    metrics_df.coalesce(1).write.mode("overwrite").text(str(metrics_dir))

    print(json.dumps(metrics, indent=2))
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the Gold daily disease surveillance aggregate."
    )
    parser.add_argument(
        "--silver",
        default=(
            "data/processed/silver/"
            "batch_id=sample-20261021"
        ),
    )
    parser.add_argument("--output", default="data/processed/gold")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--min-public-count", type=int, default=5)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Gold-Daily-Surveillance")
        .getOrCreate()
    )

    try:
        build_daily_surveillance(
            spark,
            args.silver,
            args.output,
            args.batch_id,
            args.min_public_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
