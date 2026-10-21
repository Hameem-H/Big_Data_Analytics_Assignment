import unittest
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

from pipeline.gold.district_surveillance import (
    aggregate_district_daily,
    aggregate_district_weekly,
)


class DistrictSurveillanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("EpiScope-District-Gold-Tests")
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

        # Four cases in one day, plus two more cases in the same week.
        # Weekly aggregation should combine all six cases.
        rows = [
            (date(2025, 1, 6), "BD-01", "dengue", "suspected", "P1"),
            (date(2025, 1, 6), "BD-01", "dengue", "confirmed", "P2"),
            (date(2025, 1, 6), "BD-01", "dengue", "suspected", "P3"),
            (date(2025, 1, 6), "BD-01", "dengue", "confirmed", "P4"),
            (date(2025, 1, 8), "BD-01", "dengue", "suspected", "P5"),
            (date(2025, 1, 10), "BD-01", "dengue", "confirmed", "P6"),
        ]

        cls.silver = cls.spark.createDataFrame(rows, schema)
        cls.daily = aggregate_district_daily(cls.silver)
        cls.weekly = aggregate_district_weekly(cls.silver)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_daily_count_is_suppressed_below_five(self):
        row = self.daily.first()
        self.assertTrue(row["count_suppressed"])
        self.assertIsNone(row["case_count"])

    def test_daily_aggregation_ignores_classification(self):
        self.assertEqual(self.daily.count(), 3)

    def test_weekly_aggregation_combines_days(self):
        self.assertEqual(self.weekly.count(), 1)
        row = self.weekly.first()
        self.assertEqual(row["week_start"], date(2025, 1, 6))
        self.assertEqual(row["case_count"], 6)
        self.assertFalse(row["count_suppressed"])

    def test_patient_identifier_is_not_published(self):
        self.assertNotIn("patient_id_hash", self.daily.columns)
        self.assertNotIn("patient_id_hash", self.weekly.columns)

    def test_daily_and_weekly_dimensions_are_distinct(self):
        self.assertIn("encounter_date", self.daily.columns)
        self.assertIn("week_start", self.weekly.columns)
        self.assertNotIn("encounter_date", self.weekly.columns)

    def test_invalid_suppression_threshold_is_rejected(self):
        with self.assertRaises(ValueError):
            aggregate_district_daily(self.silver, min_public_count=1)


if __name__ == "__main__":
    unittest.main()
