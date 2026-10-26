
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def safe_records(df, count_column=None):
    """Suppress counts from 1–4 before dashboard publication."""
    if count_column and count_column in df.columns:
        df = df.withColumn(
            count_column,
            F.when(
                F.col(count_column).between(1, 4),
                F.lit(None).cast("long"),
            ).otherwise(F.col(count_column)),
        )
    return [row.asDict(recursive=True) for row in df.collect()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--weekly", required=True)
    parser.add_argument("--alerts", required=True)
    parser.add_argument("--forecasts", required=True)
    parser.add_argument("--allocation", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("EpiScopeDashboardExport").getOrCreate()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    weekly = spark.read.parquet(args.weekly)
    alerts = spark.read.parquet(args.alerts)
    forecasts = spark.read.parquet(args.forecasts)
    allocation = spark.read.parquet(args.allocation)

    # Suppress small observed counts. Do not export encounter-level records.
   
    # Keep the most recent 52 weeks of weekly data and alerts.
    latest_week = weekly.agg(F.max("week_start")).first()[0]
    if latest_week is not None:
        cutoff = F.date_sub(F.to_date(F.lit(str(latest_week))), 364)
        weekly = weekly.filter(F.to_date("week_start") >= cutoff)
        alerts = alerts.filter(F.to_date("week_start") >= cutoff)

    # Primary suppression for small observed counts.
    weekly = weekly.withColumn(
        "case_count",
        F.when(F.col("case_count").between(1, 4), F.lit(None).cast("long"))
        .otherwise(F.col("case_count")),
    )
    alerts = alerts.withColumn(
        "case_count",
        F.when(F.col("case_count").between(1, 4), F.lit(None).cast("long"))
        .otherwise(F.col("case_count")),
    )

    # Suppress small forecast and resource-demand values.
    for column in ["forecast_cases", "lower80", "upper80"]:
        forecasts = forecasts.withColumn(
            column,
            F.when(F.col(column).between(1, 4), F.lit(None).cast("double"))
            .otherwise(F.col(column)),
        )

    for column in [
        "forecast_cases_4w", "beds_required",
        "iv_saline_units", "test_kits_required",
    ]:
        allocation = allocation.withColumn(
            column,
            F.when(F.col(column).between(1, 4), F.lit(None).cast("double"))
            .otherwise(F.col(column)),
        )

    exports = {
        "weekly_incidence.json": safe_records(weekly),
        "outbreak_alerts.json": safe_records(alerts),
        "forecasts.json": safe_records(forecasts),
        "resource_allocation.json": safe_records(allocation),
    }

    for filename, records in exports.items():
        with (output / filename).open("w", encoding="utf-8") as file:
            json.dump(records, file, default=str, separators=(",", ":"))


    total_bytes = sum(path.stat().st_size for path in output.glob("*.json"))
    print(f"Dashboard exports written to: {output}")
    print(f"Combined JSON size: {total_bytes / (1024 * 1024):.2f} MB")
    spark.stop()


if __name__ == "__main__":
    main()
