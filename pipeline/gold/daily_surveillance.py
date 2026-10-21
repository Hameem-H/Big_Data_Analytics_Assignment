"""Build privacy-protected daily disease surveillance aggregates."""

import argparse
import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


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
    """Aggregate Silver records and suppress public counts below threshold.

    The output contains no patient identifiers or encounter-level records.
    Counts from 1 to min_public_count - 1 are replaced with null.
    """

    if min_public_count < 2:
        raise ValueError("min_public_count must be at least 2")

    required = set(DIMENSIONS)
    missing = required - set(silver.columns)
    if missing:
        raise ValueError(
            f"Silver data is missing required columns: {sorted(missing)}"
        )

    # Aggregate at the district, disease, date, and classification level.
    aggregated = (
        silver
        .select(*DIMENSIONS)
        .groupBy(*DIMENSIONS)
        .agg(F.count(F.lit(1)).alias("_exact_case_count"))
    )

    # Never write the exact value for a group below the public threshold.
    public_output = (
        aggregated
        .withColumn(
            "case_count",
            F.when(
                F.col("_exact_case_count") >= min_public_count,
                F.col("_exact_case_count"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "count_suppressed",
            F.col("_exact_case_count") < min_public_count,
        )
        .drop("_exact_case_count")
    )

    return public_output.select(
        *DIMENSIONS,
        "case_count",
        "count_suppressed",
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
