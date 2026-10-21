import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

from pipeline.gold.daily_surveillance import (
    aggregate_daily_surveillance,
)


class GoldDailySurveillanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("EpiScope-Gold-Tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        schema = StructType([
            StructField("encounter_date", DateType(), False),
            StructField("district_code", StringType(), False),
            StructField("disease", StringType(), False),
            StructField("case_classification", StringType(), False),
            StructField("patient_id_hash", StringType(), True),
        ])

        from datetime import date

        rows = []

        # Four cases: must be suppressed.
        for i in range(4):
            rows.append((
                date(2025, 1, 1), "BD-01", "dengue",
                "suspected", f"PID-S-{i}",
            ))

        # Five cases: may be published.
        for i in range(5):
            rows.append((
                date(2025, 1, 2), "BD-02", "dengue",
                "confirmed", f"PID-P-{i}",
            ))

        cls.silver = cls.spark.createDataFrame(rows, schema)
        cls.gold = aggregate_daily_surveillance(cls.silver)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_small_count_is_suppressed(self):
        row = self.gold.filter(
            self.gold.district_code == "BD-01"
        ).first()

        self.assertTrue(row["count_suppressed"])
        self.assertIsNone(row["case_count"])

    def test_count_at_threshold_is_published(self):
        row = self.gold.filter(
            self.gold.district_code == "BD-02"
        ).first()

        self.assertFalse(row["count_suppressed"])
        self.assertEqual(row["case_count"], 5)

    def test_aggregates_by_expected_dimensions(self):
        self.assertEqual(self.gold.count(), 2)
        self.assertIn("encounter_date", self.gold.columns)
        self.assertIn("district_code", self.gold.columns)
        self.assertIn("disease", self.gold.columns)
        self.assertIn("case_classification", self.gold.columns)

    def test_patient_identifiers_are_not_output(self):
        self.assertNotIn("patient_id_hash", self.gold.columns)

    def test_invalid_threshold_is_rejected(self):
        with self.assertRaises(ValueError):
            aggregate_daily_surveillance(self.silver, min_public_count=1)

    def test_missing_dimension_is_rejected(self):
        incomplete = self.silver.drop("disease")

        with self.assertRaises(ValueError):
            aggregate_daily_surveillance(incomplete)


if __name__ == "__main__":
    unittest.main()
