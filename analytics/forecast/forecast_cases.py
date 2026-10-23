import argparse
from datetime import timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("EpiScopeForecast").getOrCreate()

# Read weekly aggregated counts.
source = (
    spark.read.parquet(args.input)
    .groupBy("week_start", "district_code", "disease")
    .agg(F.sum("case_count").alias("case_count"))
)

bounds = source.agg(
    F.min("week_start").alias("first_week"),
    F.max("week_start").alias("last_week"),
).first()

if bounds.first_week is None:
    raise ValueError("Input contains no weekly data.")

# Create a complete weekly series, filling weeks without cases with zero.
pairs = source.select("district_code", "disease").distinct()

weeks = spark.createDataFrame(
    [(bounds.first_week, bounds.last_week)],
    ["first_week", "last_week"],
).select(
    F.explode(
        F.sequence(
            "first_week",
            "last_week",
            F.expr("INTERVAL 7 DAYS"),
        )
    ).alias("week_start")
)

dense = (
    pairs.crossJoin(weeks)
    .join(source, ["week_start", "district_code", "disease"], "left")
    .fillna({"case_count": 0})
)

history = (
    Window.partitionBy("district_code", "disease")
    .orderBy("week_start")
)

# At each origin, use the four weeks ending at that origin.
history_data = (
    dense
    .withColumn(
        "mean_4w",
        F.avg("case_count").over(history.rowsBetween(-3, 0)),
    )
    .withColumn(
        "sd_4w",
        F.stddev_pop("case_count").over(history.rowsBetween(-3, 0)),
    )
)

# Evaluate actual target weeks within the most recent 52 weeks.
targets = (
    dense
    .filter(
        F.col("week_start")
        > F.date_sub(F.lit(bounds.last_week), 364)
    )
    .crossJoin(
        spark.range(1, 5).select(
            F.col("id").cast("int").alias("horizon_weeks")
        )
    )
    .withColumn(
        "origin_week",
        F.date_add(
            F.col("week_start"),
            -7 * F.col("horizon_weeks"),
        ),
    )
    .alias("t")
)

origins = history_data.select(
    "district_code",
    "disease",
    F.col("week_start").alias("origin_week"),
    "mean_4w",
    "sd_4w",
).alias("o")

# Seasonal-naive prediction: the observed count 52 weeks before the target.
seasonal = dense.select(
    "district_code",
    "disease",
    F.col("week_start").alias("seasonal_week"),
    F.col("case_count").alias("seasonal_prediction"),
).alias("s")

evaluation = (
    targets
    .join(
        origins,
        (F.col("t.district_code") == F.col("o.district_code"))
        & (F.col("t.disease") == F.col("o.disease"))
        & (F.col("t.origin_week") == F.col("o.origin_week")),
        "inner",
    )
    .join(
        seasonal,
        (F.col("t.district_code") == F.col("s.district_code"))
        & (F.col("t.disease") == F.col("s.disease"))
        & (
            F.col("s.seasonal_week")
            == F.date_sub(F.col("t.week_start"), 364)
        ),
        "left",
    )
    .select(
        F.col("t.disease").alias("disease"),
        F.col("t.case_count").alias("actual"),
        F.col("t.horizon_weeks").alias("horizon_weeks"),
        F.col("o.mean_4w").alias("mean_4w"),
        F.col("o.sd_4w").alias("sd_4w"),
        F.col("s.seasonal_prediction").alias("seasonal_prediction"),
    )
    .filter(F.col("mean_4w").isNotNull())
)

# Score both models. Missing seasonal-naive values are excluded from its metrics.
moving_average = evaluation.select(
    "disease",
    "horizon_weeks",
    "actual",
    F.lit("four_week_average").alias("model"),
    F.col("mean_4w").alias("prediction"),
    "sd_4w",
)

seasonal_naive = (
    evaluation
    .filter(F.col("seasonal_prediction").isNotNull())
    .select(
        "disease",
        "horizon_weeks",
        "actual",
        F.lit("seasonal_naive").alias("model"),
        F.col("seasonal_prediction").alias("prediction"),
        "sd_4w",
    )
)

scored = (
    moving_average.unionByName(seasonal_naive)
    .withColumn(
        "lower80",
        F.greatest(
            F.lit(0.0),
            F.col("prediction")
            - F.lit(1.28) * F.coalesce(F.col("sd_4w"), F.lit(0.0)),
        ),
    )
    .withColumn(
        "upper80",
        F.col("prediction")
        + F.lit(1.28) * F.coalesce(F.col("sd_4w"), F.lit(0.0)),
    )
)

metrics = scored.groupBy(
    "disease", "horizon_weeks", "model"
).agg(
    F.count("*").alias("evaluation_points"),
    F.avg(
        F.abs(F.col("actual") - F.col("prediction"))
    ).alias("mae"),
    F.avg(
        F.when(
            (F.col("actual") >= F.col("lower80"))
            & (F.col("actual") <= F.col("upper80")),
            1.0,
        ).otherwise(0.0)
    ).alias("prediction_interval_coverage_80"),
)

# Produce distinct forecasts for the next four weeks.
latest_week = bounds.last_week

latest = history_data.filter(
    F.col("week_start") == F.lit(latest_week)
).select(
    "district_code", "disease", "mean_4w", "sd_4w"
)

future_dates = spark.createDataFrame(
    [
        (latest_week + timedelta(days=7 * horizon), horizon)
        for horizon in range(1, 5)
    ],
    ["forecast_week", "horizon_weeks"],
)

forecasts = (
    latest.crossJoin(future_dates)
    .withColumn("forecast_cases", F.greatest(F.lit(0.0), F.col("mean_4w")))
    .withColumn(
        "lower80",
        F.greatest(
            F.lit(0.0),
            F.col("forecast_cases")
            - F.lit(1.28) * F.coalesce(F.col("sd_4w"), F.lit(0.0)),
        ),
    )
    .withColumn(
        "upper80",
        F.col("forecast_cases")
        + F.lit(1.28) * F.coalesce(F.col("sd_4w"), F.lit(0.0)),
    )
    .select(
        "district_code",
        "disease",
        "forecast_week",
        "horizon_weeks",
        "forecast_cases",
        "lower80",
        "upper80",
    )
)

forecasts.write.mode("overwrite").parquet(args.output + "/forecasts")
metrics.write.mode("overwrite").option("header", True).csv(
    args.output + "/backtest_metrics"
)

print("Forecasts and backtest metrics written to:", args.output)
spark.stop()
