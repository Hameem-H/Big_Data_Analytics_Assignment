"""Gold analytics for facility reporting activity and reporting delays.

Important: encounter activity is a proxy for reporting. It cannot establish
whether a facility submitted an official zero-case report.
"""

import argparse
import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


def build_facility_daily_reporting(
    silver: DataFrame,
    facilities: DataFrame,
) -> DataFrame:
    """Create one expected facility-day row for every date in the data range."""

    required_silver = {
        "facility_id", "district_code", "encounter_date", "report_date"
    }
    missing = required_silver - set(silver.columns)
    if missing:
        raise ValueError(
            f"Silver data is missing required columns: {sorted(missing)}"
        )

    required_facilities = {"facility_id", "district_code"}
    missing = required_facilities - set(facilities.columns)
    if missing:
        raise ValueError(
            f"Facility registry is missing columns: {sorted(missing)}"
        )

    # Use the registered facilities as the expected reporting population.
    registry = (
        facilities
        .select("facility_id", "district_code")
        .dropDuplicates(["facility_id"])
    )

    date_bounds = silver.agg(
        F.min("encounter_date").alias("min_date"),
        F.max("encounter_date").alias("max_date"),
    ).first()

    if date_bounds["min_date"] is None:
        raise ValueError("Silver contains no valid encounter dates")

    calendar = silver.sparkSession.range(1).select(
        F.explode(
            F.sequence(
                F.lit(date_bounds["min_date"]),
                F.lit(date_bounds["max_date"]),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("report_date")
    )

    expected = registry.crossJoin(calendar)

    observed = (
        silver
        .select(
            "facility_id",
            F.col("encounter_date").alias("report_date"),
        )
        .dropDuplicates(["facility_id", "report_date"])
        .withColumn("report_observed", F.lit(True))
    )

    return (
        expected
        .join(observed, ["facility_id", "report_date"], "left")
        .withColumn(
            "report_observed",
            F.coalesce(F.col("report_observed"), F.lit(False)),
        )
        .withColumn("report_expected", F.lit(True))
        .select(
            "report_date",
            "district_code",
            "facility_id",
            "report_expected",
            "report_observed",
        )
    )


def aggregate_district_reporting(
    facility_daily: DataFrame,
) -> DataFrame:
    """Summarize observed facility activity by district and date."""

    return (
        facility_daily
        .groupBy("report_date", "district_code")
        .agg(
            F.countDistinct("facility_id").alias("expected_facilities"),
            F.countDistinct(
                F.when(F.col("report_observed"), F.col("facility_id"))
            ).alias("observed_facilities"),
        )
        .withColumn(
            "missing_facilities",
            F.col("expected_facilities") - F.col("observed_facilities"),
        )
        .withColumn(
            "reporting_completeness",
            F.when(
                F.col("expected_facilities") > 0,
                F.col("observed_facilities")
                / F.col("expected_facilities"),
            ),
        )
        .withColumn(
            "reporting_completeness_pct",
            F.round(F.col("reporting_completeness") * 100, 2),
        )
        .drop("reporting_completeness")
    )


def aggregate_reporting_delays(silver: DataFrame) -> DataFrame:
    """Summarize report_date minus encounter_date by facility and month."""

    required = {
        "facility_id", "district_code", "encounter_date", "report_date"
    }
    missing = required - set(silver.columns)
    if missing:
        raise ValueError(
            f"Silver data is missing required columns: {sorted(missing)}"
        )

    dated = silver.withColumn(
        "delay_days",
        F.datediff(F.col("report_date"), F.col("encounter_date")),
    ).withColumn(
        "encounter_month",
        F.to_date(F.date_trunc("month", F.col("encounter_date"))),
    )

    return (
        dated
        .groupBy("encounter_month", "district_code", "facility_id")
        .agg(
            F.count("*").alias("encounter_records"),
            F.round(F.avg("delay_days"), 2).alias("mean_delay_days"),
            F.percentile_approx(
                "delay_days", 0.5, 10000
            ).alias("median_delay_days"),
            F.max("delay_days").alias("max_delay_days"),
            F.sum(
                F.when(F.col("delay_days") > 7, 1).otherwise(0)
            ).alias("records_reported_after_7_days"),
        )
    )


def run_facility_reporting(
    spark: SparkSession,
    silver_path: str,
    facilities_path: str,
    output_path: str,
    batch_id: str,
) -> dict:
    """Write facility activity, district completeness, delays, and metrics."""

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", batch_id):
        raise ValueError("Invalid batch ID")

    silver = spark.read.parquet(silver_path)
    facilities = (
        spark.read.option("header", True)
        .csv(facilities_path)
        .select("facility_id", "district_code")
    )

    facility_daily = build_facility_daily_reporting(silver, facilities)
    district_daily = aggregate_district_reporting(facility_daily)
    delay_summary = aggregate_reporting_delays(silver)

    outputs = {
        "facility_reporting_daily": facility_daily,
        "district_reporting_daily": district_daily,
        "facility_reporting_delays": delay_summary,
    }

    metrics = {
        "batch_id": batch_id,
        "source_encounter_records": silver.count(),
        "registered_facilities": facilities.select("facility_id").distinct().count(),
        "reporting_measurement": (
            "Encounter-based activity proxy; not verified administrative submissions"
        ),
        "outputs": {},
    }

    for name, frame in outputs.items():
        destination = (
            Path(output_path) / name / f"batch_id={batch_id}"
        )
        frame.write.mode("overwrite").parquet(str(destination))
        metrics["outputs"][name] = {
            "rows": frame.count(),
            "path": str(destination),
        }

    metrics_dir = (
        Path(output_path)
        / "quality"
        / "facility_reporting"
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
        description="Build facility reporting activity and delay aggregates."
    )
    parser.add_argument(
        "--silver",
        default="data/processed/silver/batch_id=sample-20261021",
    )
    parser.add_argument(
        "--facilities",
        default="data/reference/synthetic_facilities.csv",
    )
    parser.add_argument("--output", default="data/processed/gold")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Gold-Facility-Reporting")
        .getOrCreate()
    )
    try:
        run_facility_reporting(
            spark,
            args.silver,
            args.facilities,
            args.output,
            args.batch_id,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
