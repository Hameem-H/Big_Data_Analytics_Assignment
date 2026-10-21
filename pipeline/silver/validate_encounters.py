"""Validate Bronze encounter records and write Silver and quarantine outputs."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DateType, IntegerType


REQUIRED_FIELDS = [
    "encounter_id",
    "facility_id",
    "district_code",
    "disease",
    "encounter_date",
    "report_date",
    "age_years",
    "sex",
    "case_classification",
    "source_system",
    "ingestion_timestamp",
]

ALLOWED_SEX = [
    "female", "male", "other", "unknown", "not_recorded"
]
ALLOWED_CLASSIFICATIONS = [
    "suspected", "probable", "confirmed", "unknown"
]


def validate_encounters(
    spark: SparkSession,
    bronze_path: str,
    districts_path: str,
    facilities_path: str,
    output_path: str,
    batch_id: str,
) -> dict:
    """Validate a Bronze batch and write accepted, quarantine, and metrics."""

    bronze = spark.read.parquet(bronze_path)

    required_columns = set(REQUIRED_FIELDS)
    missing_columns = required_columns - set(bronze.columns)
    if missing_columns:
        raise ValueError(
            f"Bronze schema is missing required columns: {sorted(missing_columns)}"
        )

    # These contract fields are optional. Add typed nulls when absent.
    optional_string_fields = [
        "patient_id_hash",
        "symptom_onset_date",
        "source_record_id",
    ]
    for field in optional_string_fields:
        if field not in bronze.columns:
            bronze = bronze.withColumn(
                field, F.lit(None).cast("string")
            )

    # Keep only approved contract fields and known ingestion metadata
    # in Silver and Quarantine; never propagate arbitrary input columns.
    contract_fields = [
        "encounter_id",
        "patient_id_hash",
        "facility_id",
        "district_code",
        "disease",
        "encounter_date",
        "symptom_onset_date",
        "report_date",
        "age_years",
        "sex",
        "case_classification",
        "source_system",
        "source_record_id",
        "ingestion_timestamp",
    ]
    metadata_fields = [
        field
        for field in (
            "_batch_id",
            "_source_file",
            "_ingested_at",
            "_ingest_date",
        )
        if field in bronze.columns
    ]

    # Stable final tie-breaker for duplicate ordering.
    # Completely identical rows have the same hash and are equivalent
    # in contract content and available metadata.
    hash_columns = sorted(bronze.columns)
    bronze = bronze.withColumn(
        "_record_tiebreaker",
        F.sha2(
            F.to_json(F.struct(*[F.col(c) for c in hash_columns])),
            256,
        ),
    )

    districts = (
        spark.read.option("header", True)
        .csv(districts_path)
        .select("district_code")
        .distinct()
    )
    facilities = (
        spark.read.option("header", True)
        .csv(facilities_path)
        .select("facility_id", "district_code")
        .distinct()
    )

    # Use registry joins rather than hard-coded district or facility lists.
    known_districts = districts.withColumn("_known_district", F.lit(True))
    known_facilities = facilities.select(
        F.col("facility_id").alias("_registry_facility_id"),
        F.col("district_code").alias("_registry_district_code"),
    ).withColumn("_known_facility", F.lit(True))

    df = (
        bronze
        .join(
            F.broadcast(known_districts),
            on="district_code",
            how="left",
        )
        .join(
            F.broadcast(known_facilities),
            (
                F.col("facility_id") == F.col("_registry_facility_id")
            ) & (
                F.col("district_code") == F.col("_registry_district_code")
            ),
            how="left",
        )
    )

    # Parse without failing the whole job on malformed input.
    df = (
        df
        .withColumn(
            "_encounter_date",
            F.expr("try_cast(encounter_date AS DATE)"),
        )
        .withColumn(
            "_report_date",
            F.expr("try_cast(report_date AS DATE)"),
        )
        .withColumn(
            "_symptom_onset_date",
            F.expr("try_cast(symptom_onset_date AS DATE)"),
        )
        .withColumn(
            "_age_integer",
            F.when(
                F.col("age_years").isNull()
                | (F.trim("age_years") == ""),
                F.lit(None).cast(IntegerType()),
            ).when(
                F.trim("age_years").rlike(r"^[0-9]+$"),
                F.expr("try_cast(age_years AS INT)"),
            ).otherwise(F.lit(None).cast(IntegerType())),
        )
        .withColumn(
            "_ingestion_timestamp",
            F.expr(
                "try_to_timestamp(ingestion_timestamp, "
                "'yyyy-MM-dd''T''HH:mm:ssX')"
            ),
        )
    )

    # Assign a deterministic order for duplicate handling.
    # The source record ID and source file break ties consistently.
    duplicate_window = Window.partitionBy(
        "source_system", "encounter_id"
    ).orderBy(
        F.col("source_record_id").asc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        F.col("_record_tiebreaker").asc(),
    )

    df = df.withColumn(
        "_duplicate_rank",
        F.row_number().over(duplicate_window),
    )

    blank = lambda name: (
        F.col(name).isNull() | (F.trim(F.col(name)) == "")
    )

    # Each record can have multiple rejection reasons.
    reason_candidates = [
        F.when(blank("encounter_id"), "MISSING_ENCOUNTER_ID"),
        F.when(blank("facility_id"), "MISSING_FACILITY_ID"),
        F.when(blank("district_code"), "MISSING_DISTRICT_CODE"),
        F.when(blank("disease"), "MISSING_DISEASE"),
        F.when(blank("source_system"), "MISSING_SOURCE_SYSTEM"),
        F.when(
            F.col("_known_district").isNull()
            & F.col("district_code").isNotNull(),
            "UNKNOWN_DISTRICT",
        ),
        F.when(
            F.col("_known_facility").isNull()
            & F.col("facility_id").isNotNull()
            & F.col("district_code").isNotNull(),
            "UNKNOWN_FACILITY_OR_DISTRICT_MISMATCH",
        ),
        F.when(
            F.col("_encounter_date").isNull(),
            "INVALID_ENCOUNTER_DATE",
        ),
        F.when(
            F.col("_report_date").isNull(),
            "INVALID_REPORT_DATE",
        ),
        F.when(
            F.col("_encounter_date").isNotNull()
            & F.col("_report_date").isNotNull()
            & (F.col("_report_date") < F.col("_encounter_date")),
            "REPORT_BEFORE_ENCOUNTER",
        ),
        F.when(
            ~(
                F.col("age_years").isNull()
                | (F.trim("age_years") == "")
                | (
                    F.col("_age_integer").between(0, 120)
                    & F.trim("age_years").rlike(r"^[0-9]+$")
                )
            ),
            "INVALID_AGE",
        ),
        F.when(
            ~F.lower(F.trim("sex")).isin(*ALLOWED_SEX)
            | F.col("sex").isNull(),
            "INVALID_SEX",
        ),
        F.when(
            ~F.lower(F.trim("case_classification")).isin(
                *ALLOWED_CLASSIFICATIONS
            ) | F.col("case_classification").isNull(),
            "INVALID_CASE_CLASSIFICATION",
        ),
        F.when(
            F.col("symptom_onset_date").isNotNull()
            & (F.trim("symptom_onset_date") != "")
            & F.col("_symptom_onset_date").isNull(),
            "INVALID_SYMPTOM_ONSET_DATE",
        ),
        F.when(
            F.col("_symptom_onset_date").isNotNull()
            & F.col("_encounter_date").isNotNull()
            & (
                F.col("_symptom_onset_date")
                > F.col("_encounter_date")
            ),
            "SYMPTOM_ONSET_AFTER_ENCOUNTER",
        ),
        F.when(
            F.col("_ingestion_timestamp").isNull(),
            "INVALID_INGESTION_TIMESTAMP",
        ),
        F.when(
            F.col("_duplicate_rank") > 1,
            "DUPLICATE_SOURCE_ENCOUNTER_ID",
        ),
    ]

    df = df.withColumn(
        "validation_reasons",
        F.filter(
            F.array(*reason_candidates),
            lambda item: item.isNotNull(),
        ),
    )

    df = df.withColumn(
        "_is_valid",
        F.size("validation_reasons") == 0,
    )

    output_root = Path(output_path)
    silver_path = str(output_root / "silver" / f"batch_id={batch_id}")
    quarantine_path = str(
        output_root / "quarantine" / f"batch_id={batch_id}"
    )
    metrics_path = str(
        output_root / "quality" / f"batch_id={batch_id}"
    )

    # Silver contains only approved contract fields and safe metadata.
    # Date, age, and timestamp fields use their validated typed values.
    accepted = (
        df.filter(F.col("_is_valid"))
        .select(
            "encounter_id",
            "patient_id_hash",
            "facility_id",
            "district_code",
            "disease",
            F.col("_encounter_date").alias("encounter_date"),
            F.col("_symptom_onset_date").alias("symptom_onset_date"),
            F.col("_report_date").alias("report_date"),
            F.col("_age_integer").alias("age_years"),
            "sex",
            "case_classification",
            "source_system",
            "source_record_id",
            F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
            *metadata_fields,
        )
    )

    # Quarantine retains raw approved contract values and validation
    # reasons, but does not propagate arbitrary input columns.
    quarantined = (
        df.filter(~F.col("_is_valid"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .select(
            *[F.col(field) for field in contract_fields],
            *[F.col(field) for field in metadata_fields],
            F.col("validation_reasons"),
            F.col("_quarantined_at"),
        )
    )

    # Materialize counts before writing, for an auditable reconciliation.
    input_count = bronze.count()
    accepted_count = accepted.count()
    quarantine_count = quarantined.count()

    reason_counts = (
        quarantined
        .select(F.explode("validation_reasons").alias("reason_code"))
        .groupBy("reason_code")
        .count()
        .orderBy("reason_code")
        .collect()
    )

    metrics = {
        "batch_id": batch_id,
        "input_records": input_count,
        "accepted_records": accepted_count,
        "quarantined_records": quarantine_count,
        "rejection_reason_counts": {
            row["reason_code"]: row["count"]
            for row in reason_counts
        },
    }

    if input_count != accepted_count + quarantine_count:
        raise RuntimeError(
            "Reconciliation failed: input count does not equal "
            "accepted plus quarantined counts"
        )

    accepted.write.mode("overwrite").parquet(silver_path)
    quarantined.write.mode("overwrite").parquet(quarantine_path)

    # Write one small JSON metrics file for this batch.
    metrics_df = spark.createDataFrame(
        [(json.dumps(metrics),)],
        ["metrics_json"],
    )
    metrics_df.coalesce(1).write.mode("overwrite").text(metrics_path)

    print(json.dumps(metrics, indent=2))
    print(f"Silver output: {silver_path}")
    print(f"Quarantine output: {quarantine_path}")
    print(f"Metrics output: {metrics_path}")

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Bronze encounters and write Silver outputs."
    )
    parser.add_argument(
        "--bronze",
        default=(
            "data/processed/bronze/encounters/"
            "batch_id=sample-20261021"
        ),
    )
    parser.add_argument(
        "--districts",
        default="data/reference/bangladesh_districts.csv",
    )
    parser.add_argument(
        "--facilities",
        default="data/reference/synthetic_facilities.csv",
    )
    parser.add_argument("--output", default="data/processed")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Silver-Validation")
        .getOrCreate()
    )

    try:
        validate_encounters(
            spark,
            args.bronze,
            args.districts,
            args.facilities,
            args.output,
            args.batch_id,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
