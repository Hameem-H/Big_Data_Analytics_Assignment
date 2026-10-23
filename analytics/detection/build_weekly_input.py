import argparse
from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("BuildWeeklyAnalyticsInput").getOrCreate()

silver = spark.read.parquet(args.input)

weekly = (
    silver
    .filter(
        F.col("district_code").isNotNull()
        & F.col("disease").isNotNull()
        & F.col("encounter_date").isNotNull()
    )
    .withColumn("week_start", F.to_date(F.date_trunc("week", F.col("encounter_date").cast("timestamp"))))
    .groupBy("week_start", "district_code", "disease")
    .agg(F.count("*").alias("case_count"))
    .withColumn("count_suppressed", F.lit(False))
)

weekly.write.mode("overwrite").parquet(args.output)
print(f"Weekly analytics input written to: {args.output}")
print(f"Weekly aggregate rows: {weekly.count()}")

spark.stop()
