import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from pipeline.bronze.ingest_encounters import ingest_encounters


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CSV = PROJECT_ROOT / "data" / "synthetic" / "encounters.csv"


class BronzeIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("EpiScope-Bronze-Tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name) / "bronze"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_ingests_all_sample_rows(self):
        count = ingest_encounters(
            self.spark,
            str(SAMPLE_CSV),
            str(self.output_dir),
            "test-batch",
        )
        self.assertEqual(count, 2500)

    def test_preserves_raw_fields_as_strings(self):
        ingest_encounters(
            self.spark,
            str(SAMPLE_CSV),
            str(self.output_dir),
            "test-batch",
        )
        df = self.spark.read.parquet(
            str(self.output_dir / "batch_id=test-batch")
        )

        for field in (
            "encounter_id",
            "age_years",
            "encounter_date",
            "report_date",
        ):
            self.assertEqual(df.schema[field].dataType.simpleString(), "string")

    def test_adds_ingestion_metadata(self):
        ingest_encounters(
            self.spark,
            str(SAMPLE_CSV),
            str(self.output_dir),
            "test-batch",
        )
        df = self.spark.read.parquet(
            str(self.output_dir / "batch_id=test-batch")
        )

        for field in (
            "_batch_id",
            "_source_file",
            "_ingested_at",
            "_ingest_date",
        ):
            self.assertIn(field, df.columns)

        self.assertEqual(
            df.filter(df["_batch_id"] != "test-batch").count(),
            0,
        )

    def test_rerunning_same_batch_does_not_duplicate_rows(self):
        for _ in range(2):
            ingest_encounters(
                self.spark,
                str(SAMPLE_CSV),
                str(self.output_dir),
                "repeatable-batch",
            )

        df = self.spark.read.parquet(
            str(self.output_dir / "batch_id=repeatable-batch")
        )
        self.assertEqual(df.count(), 2500)

    def test_rejects_unsafe_batch_id(self):
        with self.assertRaises(ValueError):
            ingest_encounters(
                self.spark,
                str(SAMPLE_CSV),
                str(self.output_dir),
                "../unsafe",
            )

    def test_rejects_missing_input(self):
        with self.assertRaises(FileNotFoundError):
            ingest_encounters(
                self.spark,
                str(PROJECT_ROOT / "missing.csv"),
                str(self.output_dir),
                "missing-input",
            )


if __name__ == "__main__":
    unittest.main()
