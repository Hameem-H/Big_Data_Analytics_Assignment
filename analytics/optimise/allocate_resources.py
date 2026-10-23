import argparse
from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--forecasts", required=True)
parser.add_argument("--districts", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--budget", type=float, default=1000000.0)
args = parser.parse_args()

spark = SparkSession.builder.appName("EpiScopeResourceAllocation").getOrCreate()

forecasts = spark.read.parquet(args.forecasts)
districts = spark.read.option("header", True).csv(args.districts)

# Use the next four weeks' average forecast as the planning horizon.
demand = forecasts.groupBy("district_code").agg(
    F.sum("forecast_cases").alias("forecast_cases_4w")
)

# Illustrative planning assumptions; replace with locally validated values.
demand = (
    demand
    .withColumn("beds_required", F.ceil(F.col("forecast_cases_4w") * 0.10))
    .withColumn("iv_saline_units", F.ceil(F.col("forecast_cases_4w") * 1.5))
    .withColumn("test_kits_required", F.ceil(F.col("forecast_cases_4w") * 1.0))
    .withColumn(
        "estimated_cost",
        F.col("beds_required") * F.lit(1500.0)
        + F.col("iv_saline_units") * F.lit(80.0)
        + F.col("test_kits_required") * F.lit(120.0),
    )
)

# No official population column exists in the provided district reference.
# Use equal district weights as an explicit provisional baseline.
district_count = districts.select("district_code").distinct().count()
equal_budget = args.budget / max(district_count, 1)

baseline = districts.select("district_code").distinct().withColumn(
    "baseline_budget", F.lit(equal_budget)
)

# Allocate the budget in proportion to estimated demand cost.
total_cost = demand.agg(F.sum("estimated_cost").alias("total")).first()["total"] or 0.0

allocation = demand.withColumn(
    "recommended_budget",
    F.when(
        F.lit(total_cost) > 0,
        F.col("estimated_cost") / F.lit(total_cost) * F.lit(args.budget),
    ).otherwise(F.lit(0.0)),
)

result = (
    allocation.join(baseline, "district_code", "left")
    .join(districts.select("district_code", "district_name_en", "division_name_en"),
          "district_code", "left")
    .select(
        "district_code",
        "district_name_en",
        "division_name_en",
        "forecast_cases_4w",
        "beds_required",
        "iv_saline_units",
        "test_kits_required",
        "estimated_cost",
        "recommended_budget",
        "baseline_budget",
    )
)

result.write.mode("overwrite").parquet(args.output)
print(f"Resource demand and allocation written to: {args.output}")
print(f"Budget: {args.budget:.2f}")
print(f"Districts in baseline: {district_count}")

spark.stop()
