"""Ingest raw encounter CSV records into the EpiScope Bronze layer."""

import argparse
import re
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def ingest_encounters(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    batch_id: str,
) -> int:
    """Ingest one CSV batch into a deterministic, overwriteable Parquet path."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError(
            "batch_id may contain only letters, numbers, underscores, and hyphens"
        )

    if not Path(input_path).is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    # Do not infer types in Bronze. Preserve source values for Silver validation.
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )

    if not raw_df.columns:
        raise ValueError("Input CSV has no columns")

    bronze_df = (
        raw_df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
    )

    # One deterministic path per batch makes rerunning the same batch idempotent.
    batch_output = str(Path(output_path) / f"batch_id={batch_id}")

    row_count = bronze_df.count()

    (
        bronze_df.write
        .mode("overwrite")
        .parquet(batch_output)
    )

    print(f"Batch ID: {batch_id}")
    print(f"Input: {input_path}")
    print(f"Bronze output: {batch_output}")
    print(f"Rows ingested: {row_count}")

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest encounter CSV data into the EpiScope Bronze layer."
    )
    parser.add_argument(
        "--input",
        default="data/synthetic/encounters.csv",
        help="Path to the source encounter CSV",
    )
    parser.add_argument(
        "--output",
        default="data/processed/bronze/encounters",
        help="Base directory for Bronze Parquet batches",
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Stable identifier for this ingestion batch",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Bronze-Ingestion")
        .getOrCreate()
    )

    try:
        ingest_encounters(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            batch_id=args.batch_id,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
