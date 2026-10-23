import argparse
from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--alerts", required=True)
    parser.add_argument("--ground-truth", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("EpiScopeOutbreakEvaluation").getOrCreate()

    alerts = (
        spark.read.parquet(args.alerts)
        .filter(F.col("alert"))
        .select(
            "district_code",
            "disease",
            F.col("week_start").alias("alert_date"),
        )
        .dropDuplicates()
    )

    truth = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(args.ground_truth)
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
    )

    # An outbreak is detected if an alert occurs between its start and end.
    matches = (
        truth.alias("g")
        .join(
            alerts.alias("a"),
            (F.col("g.district_code") == F.col("a.district_code"))
            & (F.col("g.disease") == F.col("a.disease"))
            & (F.col("a.alert_date") >= F.col("g.start_date"))
            & (F.col("a.alert_date") <= F.col("g.end_date")),
            "left",
        )
        .groupBy(
            F.col("g.outbreak_id"),
            F.col("g.district_code"),
            F.col("g.start_date"),
        )
        .agg(F.min("a.alert_date").alias("first_alert_date"))
        .withColumn(
            "detected",
            F.col("first_alert_date").isNotNull(),
        )
        .withColumn(
            "detection_delay_days",
            F.datediff("first_alert_date", "start_date"),
        )
    )

    total_outbreaks = truth.count()
    detected_count = matches.filter("detected").count()
    sensitivity = detected_count / total_outbreaks if total_outbreaks else 0

    # Count alerts that fall outside every known outbreak interval.
    non_outbreak_alerts = alerts.alias("a").join(
        truth.alias("g"),
        (F.col("a.district_code") == F.col("g.district_code"))
        & (F.col("a.disease") == F.col("g.disease"))
        & (F.col("a.alert_date") >= F.col("g.start_date"))
        & (F.col("a.alert_date") <= F.col("g.end_date")),
        "left_anti",
    )

    district_years = alerts.select(
        "district_code", F.year("alert_date").alias("year")
    ).distinct().count()

    false_alert_count = non_outbreak_alerts.count()
    false_alerts_per_district_year = (
        false_alert_count / district_years if district_years else 0
    )

    median_delay = (
        matches.filter("detected")
        .approxQuantile("detection_delay_days", [0.5], 0.01)
    )
    median_delay = median_delay[0] if median_delay else None

    results = [
        ("total_ground_truth_outbreaks", float(total_outbreaks)),
        ("detected_outbreaks", float(detected_count)),
        ("sensitivity", float(sensitivity)),
        ("false_alerts", float(false_alert_count)),
        ("district_years_with_alert_data", float(district_years)),
        ("false_alerts_per_district_year", float(false_alerts_per_district_year)),
        ("median_detection_delay_days", float(median_delay) if median_delay is not None else None),
    ]

    (
        spark.createDataFrame(results, ["metric", "value"])
        .write.mode("overwrite").option("header", True).csv(args.output)
    )

    for metric, value in results:
        print(f"{metric}: {value}")

    spark.stop()


if __name__ == "__main__":
    main()
