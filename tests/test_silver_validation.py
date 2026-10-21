import csv
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType

from pipeline.bronze.ingest_encounters import ingest_encounters
from pipeline.silver.validate_encounters import validate_encounters
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]

FIELDS = [
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


def make_row(
    encounter_id,
    source_record_id,
    *,
    facility_id="FAC-00001",
    district_code="BD-01",
    age="30",
    encounter_date="2023-01-10",
    symptom_onset_date="2023-01-08",
    report_date="2023-01-12",
):
    return {
        "encounter_id": encounter_id,
        "patient_id_hash": f"PID-{source_record_id}",
        "facility_id": facility_id,
        "district_code": district_code,
        "disease": "dengue",
        "encounter_date": encounter_date,
        "symptom_onset_date": symptom_onset_date,
        "report_date": report_date,
        "age_years": age,
        "sex": "male",
        "case_classification": "suspected",
        "source_system": "test_source",
        "source_record_id": source_record_id,
        "ingestion_timestamp": "2023-01-12T10:00:00Z",
    }


class SilverValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("EpiScope-Silver-Tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_path = Path(cls.temp_dir.name)

        csv_path = cls.temp_path / "controlled_encounters.csv"

        rows = [
            # Valid record.
            make_row("ENC-001", "SRC-001"),

            # Duplicate key: same source system and encounter ID.
            make_row("ENC-001", "SRC-002"),

            # Invalid age.
            make_row("ENC-002", "SRC-003", age="130"),

            # Unknown facility.
            make_row(
                "ENC-003",
                "SRC-004",
                facility_id="FAC-99999",
            ),

            # Report date before encounter date.
            make_row(
                "ENC-004",
                "SRC-005",
                encounter_date="2023-01-10",
                report_date="2023-01-09",
            ),

            # Null age is allowed by the contract.
            make_row("ENC-005", "SRC-006", age=""),
        ]

        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)

        cls.bronze_path = cls.temp_path / "bronze"
        ingest_encounters(
            cls.spark,
            str(csv_path),
            str(cls.bronze_path),
            "silver-test",
        )

        cls.output_path = cls.temp_path / "processed"

        cls.metrics = validate_encounters(
            cls.spark,
            str(cls.bronze_path / "batch_id=silver-test"),
            str(ROOT / "data/reference/bangladesh_districts.csv"),
            str(ROOT / "data/reference/synthetic_facilities.csv"),
            str(cls.output_path),
            "silver-test",
        )

        cls.silver_path = (
            cls.output_path / "silver" / "batch_id=silver-test"
        )
        cls.quarantine_path = (
            cls.output_path / "quarantine" / "batch_id=silver-test"
        )

        cls.silver = cls.spark.read.parquet(str(cls.silver_path))
        cls.quarantine = cls.spark.read.parquet(str(cls.quarantine_path))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp_dir.cleanup()

    def test_input_reconciles_to_accepted_and_quarantined(self):
        self.assertEqual(self.metrics["input_records"], 6)
        self.assertEqual(self.metrics["accepted_records"], 2)
        self.assertEqual(self.metrics["quarantined_records"], 4)
        self.assertEqual(
            self.metrics["input_records"],
            self.metrics["accepted_records"]
            + self.metrics["quarantined_records"],
        )

    def test_silver_dates_and_age_are_typed(self):
        self.assertIsInstance(
            self.silver.schema["encounter_date"].dataType,
            DateType,
        )
        self.assertIsInstance(
            self.silver.schema["age_years"].dataType,
            IntegerType,
        )

    def test_null_age_is_accepted(self):
        row = self.silver.filter(
            self.silver.encounter_id == "ENC-005"
        ).first()
        self.assertIsNotNone(row)
        self.assertIsNone(row["age_years"])

    def test_duplicate_encounter_is_quarantined(self):
        duplicate = self.quarantine.filter(
            self.quarantine.encounter_id == "ENC-001"
        ).first()

        self.assertIsNotNone(duplicate)
        self.assertIn(
            "DUPLICATE_SOURCE_ENCOUNTER_ID",
            duplicate["validation_reasons"],
        )

        # The deterministic ordering retains SRC-001.
        retained = self.silver.filter(
            self.silver.encounter_id == "ENC-001"
        ).first()
        self.assertEqual(retained["source_record_id"], "SRC-001")

    def test_invalid_age_is_quarantined(self):
        row = self.quarantine.filter(
            self.quarantine.encounter_id == "ENC-002"
        ).first()
        self.assertIn("INVALID_AGE", row["validation_reasons"])

    def test_unknown_facility_is_quarantined(self):
        row = self.quarantine.filter(
            self.quarantine.encounter_id == "ENC-003"
        ).first()
        self.assertIn(
            "UNKNOWN_FACILITY_OR_DISTRICT_MISMATCH",
            row["validation_reasons"],
        )

    def test_report_before_encounter_is_quarantined(self):
        row = self.quarantine.filter(
            self.quarantine.encounter_id == "ENC-004"
        ).first()
        self.assertIn(
            "REPORT_BEFORE_ENCOUNTER",
            row["validation_reasons"],
        )

    def test_quarantine_contains_reason_codes(self):
        empty_or_null_reasons = (
            F.col("validation_reasons").isNull()
            | (F.size(F.col("validation_reasons")) == 0)
        )

        invalid_reason_count = self.quarantine.filter(
            empty_or_null_reasons
        ).count()

        self.assertEqual(invalid_reason_count, 0)
    
    def test_silver_does_not_propagate_unapproved_columns(self):
        self.assertNotIn("unexpected_sensitive_field", self.silver.columns)
        self.assertNotIn("unexpected_sensitive_field", self.quarantine.columns)

    def test_optional_fields_are_present_in_silver(self):
        self.assertIn("patient_id_hash", self.silver.columns)
        self.assertIn("symptom_onset_date", self.silver.columns)
        self.assertIn("source_record_id", self.silver.columns)

    def test_missing_optional_columns_are_handled(self):
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                csv_path = temp_path / "optional_encounters.csv"

                # Deliberately omit both optional columns and include an
                # unapproved field to verify schema enforcement.
                fields = [
                    field for field in FIELDS
                    if field not in ("symptom_onset_date", "source_record_id")
                ] + ["unexpected_sensitive_field"]

                row = make_row("ENC-OPTIONAL-001", "SRC-OPTIONAL-001")
                row.pop("symptom_onset_date")
                row.pop("source_record_id")
                row["unexpected_sensitive_field"] = "must-not-propagate"

                with csv_path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fields)
                    writer.writeheader()
                    writer.writerow(row)

                bronze_path = temp_path / "bronze"
                ingest_encounters(
                    self.spark,
                    str(csv_path),
                    str(bronze_path),
                    "optional-columns-test",
                )

                output_path = temp_path / "processed"
                metrics = validate_encounters(
                    self.spark,
                    str(bronze_path / "batch_id=optional-columns-test"),
                    str(ROOT / "data/reference/bangladesh_districts.csv"),
                    str(ROOT / "data/reference/synthetic_facilities.csv"),
                    str(output_path),
                    "optional-columns-test",
                )

                silver_path = (
                    output_path / "silver" / "batch_id=optional-columns-test"
                )
                silver = self.spark.read.parquet(str(silver_path))

                self.assertEqual(metrics["input_records"], 1)
                self.assertEqual(metrics["accepted_records"], 1)
                self.assertEqual(metrics["quarantined_records"], 0)

                # Optional fields should exist in the output as nulls.
                self.assertIn("symptom_onset_date", silver.columns)
                self.assertIn("source_record_id", silver.columns)

                result = silver.first()
                self.assertIsNone(result["symptom_onset_date"])
                self.assertIsNone(result["source_record_id"])

                # Unapproved input fields must not reach Silver.
                self.assertNotIn(
                    "unexpected_sensitive_field", silver.columns
                )
if __name__ == "__main__":
    unittest.main()
