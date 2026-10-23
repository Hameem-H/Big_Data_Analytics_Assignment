import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--threshold", type=float, default=3.0)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("EpiScopeOutbreakDetection").getOrCreate()

    df = (
        spark.read.parquet(args.input)
        .filter(~F.col("count_suppressed"))
        .filter(F.col("case_count").isNotNull())
        .select("week_start", "district_code", "disease", "case_count")
    )

    # Compare each week against the previous 3 weeks.
    w = (
        Window.partitionBy("district_code", "disease")
        .orderBy("week_start")
    )
    history = w.rowsBetween(-3, -1)

    scored = (
        df.withColumn("baseline_mean", F.avg("case_count").over(history))
        .withColumn("baseline_sd", F.stddev_pop("case_count").over(history))
        .withColumn(
            "c2_score",
            F.when(
                F.col("baseline_sd") > 0,
                (F.col("case_count") - F.col("baseline_mean"))
                / F.col("baseline_sd"),
            ),
        )
        .withColumn(
            "alert",
            (F.col("baseline_mean").isNotNull())
            & (F.col("case_count") > F.col("baseline_mean"))
            & (F.col("c2_score") >= args.threshold),
        )
    )

    (
        scored.select(
            "week_start",
            "district_code",
            "disease",
            "case_count",
            "baseline_mean",
            "baseline_sd",
            "c2_score",
            "alert",
        )
        .write.mode("overwrite")
        .parquet(args.output)
    )

    print(f"Outbreak detection results written to: {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
