"""Build daily and weekly district-level disease surveillance aggregates."""

import argparse
import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

DAILY_DIMENSIONS = ["encounter_date", "district_code", "disease"]


def _validate_input(silver: DataFrame) -> None:
    missing = set(DAILY_DIMENSIONS) - set(silver.columns)
    if missing:
        raise ValueError(
            f"Silver data is missing required columns: {sorted(missing)}"
        )


def _suppress_small_counts(
    aggregated: DataFrame,
    min_public_count: int,
) -> DataFrame:
    if min_public_count < 2:
        raise ValueError("min_public_count must be at least 2")

    dimensions = [
        column for column in aggregated.columns
        if column != "_exact_case_count"
    ]
    # The final dimension is the child category; other dimensions define
    # the parent group whose total must not reveal a hidden child count.
    parent_dimensions = dimensions[:-1]

    if not parent_dimensions:
        parent_dimensions = dimensions

    parent_window = Window.partitionBy(*parent_dimensions)
    rank_window = (
        Window.partitionBy(*parent_dimensions)
        .orderBy(
            F.col("_exact_case_count").asc(),
            F.col(dimensions[-1]).asc(),
        )
    )

    protected = (
        aggregated
        .withColumn(
            "_parent_count",
            F.sum("_exact_case_count").over(parent_window),
        )
        .withColumn(
            "_primary",
            F.col("_exact_case_count") < min_public_count,
        )
        .withColumn(
            "_primary_count",
            F.sum(F.when(F.col("_primary"), 1).otherwise(0))
            .over(parent_window),
        )
        .withColumn(
            "_rank",
            F.when(
                ~F.col("_primary"),
                F.row_number().over(rank_window),
            ),
        )
        .withColumn(
            "_complementary",
            (F.col("_parent_count") >= min_public_count)
            & (F.col("_primary_count") == 1)
            & (~F.col("_primary"))
            & (F.col("_rank") == 1),
        )
    )

    return (
        protected
        .withColumn(
            "case_count",
            F.when(
                ~F.col("_primary") & ~F.col("_complementary"),
                F.col("_exact_case_count"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "count_suppressed",
            F.col("_primary") | F.col("_complementary"),
        )
        .drop(
            "_exact_case_count",
            "_parent_count",
            "_primary",
            "_primary_count",
            "_rank",
            "_complementary",
        )
    )

def aggregate_district_daily(
    silver: DataFrame,
    min_public_count: int = 5,
) -> DataFrame:
    """Aggregate by encounter date, district, and disease."""

    _validate_input(silver)

    aggregated = (
        silver
        .groupBy(*DAILY_DIMENSIONS)
        .agg(F.count(F.lit(1)).alias("_exact_case_count"))
    )

    return _suppress_small_counts(
        aggregated, min_public_count
    ).select(
        *DAILY_DIMENSIONS,
        "case_count",
        "count_suppressed",
    )


def aggregate_district_weekly(
    silver: DataFrame,
    min_public_count: int = 5,
) -> DataFrame:
    """Aggregate by Monday-start week, district, and disease."""

    _validate_input(silver)

    weekly = silver.withColumn(
        "week_start",
        F.to_date(
            F.date_trunc("week", F.col("encounter_date"))
        ),
    )

    aggregated = (
        weekly
        .groupBy("week_start", "district_code", "disease")
        .agg(F.count(F.lit(1)).alias("_exact_case_count"))
    )

    return _suppress_small_counts(
        aggregated, min_public_count
    ).select(
        "week_start",
        "district_code",
        "disease",
        "case_count",
        "count_suppressed",
    )


def build_district_surveillance(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    batch_id: str,
    min_public_count: int = 5,
) -> dict:
    """Write daily and weekly surveillance outputs for one batch."""

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", batch_id):
        raise ValueError("Invalid batch ID")

    silver = spark.read.parquet(silver_path)

    outputs = {
        "district_daily": aggregate_district_daily(
            silver, min_public_count
        ),
        "district_weekly": aggregate_district_weekly(
            silver, min_public_count
        ),
    }

    metrics = {
        "batch_id": batch_id,
        "source_records": silver.count(),
        "minimum_public_count": min_public_count,
        "outputs": {},
    }

    for name, output in outputs.items():
        output_dir = (
            Path(output_path) / name / f"batch_id={batch_id}"
        )
        output.write.mode("overwrite").parquet(str(output_dir))

        metrics["outputs"][name] = {
            "aggregate_rows": output.count(),
            "suppressed_aggregate_rows": output.filter(
                F.col("count_suppressed")
            ).count(),
            "output_path": str(output_dir),
        }

    metrics_dir = (
        Path(output_path)
        / "quality"
        / "district_surveillance"
        / f"batch_id={batch_id}"
    )
    spark.createDataFrame(
        [(json.dumps(metrics),)],
        ["metrics_json"],
    ).coalesce(1).write.mode("overwrite").text(str(metrics_dir))

    print(json.dumps(metrics, indent=2))
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build daily and weekly district surveillance Gold tables."
    )
    parser.add_argument(
        "--silver",
        default="data/processed/silver/batch_id=sample-20261021",
    )
    parser.add_argument("--output", default="data/processed/gold")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--min-public-count", type=int, default=5)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Gold-District-Surveillance")
        .getOrCreate()
    )

    try:
        build_district_surveillance(
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
