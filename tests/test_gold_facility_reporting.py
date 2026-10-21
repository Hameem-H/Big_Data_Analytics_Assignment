import unittest
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

from pipeline.gold.facility_reporting import (
    aggregate_district_reporting,
    aggregate_reporting_delays,
    build_facility_daily_reporting,
)


class FacilityReportingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("EpiScope-Facility-Reporting-Tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        silver_schema = StructType([
            StructField("facility_id", StringType(), False),
            StructField("district_code", StringType(), False),
            StructField("encounter_date", DateType(), False),
            StructField("report_date", DateType(), False),
        ])
        silver_rows = [
            ("FAC-1", "BD-01", date(2025, 1, 6), date(2025, 1, 8)),
            ("FAC-1", "BD-01", date(2025, 1, 6), date(2025, 1, 8)),
            ("FAC-2", "BD-01", date(2025, 1, 7), date(2025, 1, 7)),
        ]
        cls.silver = cls.spark.createDataFrame(
            silver_rows, silver_schema
        )

        facility_schema = StructType([
            StructField("facility_id", StringType(), False),
            StructField("district_code", StringType(), False),
        ])
        facility_rows = [
            ("FAC-1", "BD-01"),
            ("FAC-2", "BD-01"),
            ("FAC-3", "BD-01"),
        ]
        cls.facilities = cls.spark.createDataFrame(
            facility_rows, facility_schema
        )

        cls.daily = build_facility_daily_reporting(
            cls.silver, cls.facilities
        )
        cls.district = aggregate_district_reporting(cls.daily)
        cls.delays = aggregate_reporting_delays(cls.silver)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_expected_facility_days_are_created(self):
        # Three facilities x two dates = six expected facility-days.
        self.assertEqual(self.daily.count(), 6)

    def test_encounter_activity_marks_facility_as_observed(self):
        row = self.daily.filter(
            (self.daily.facility_id == "FAC-1")
            & (self.daily.report_date == date(2025, 1, 6))
        ).first()
        self.assertTrue(row["report_observed"])

    def test_no_encounter_activity_is_marked_missing(self):
        row = self.daily.filter(
            (self.daily.facility_id == "FAC-3")
            & (self.daily.report_date == date(2025, 1, 6))
        ).first()
        self.assertFalse(row["report_observed"])

    def test_district_completeness_counts_distinct_facilities(self):
        row = self.district.filter(
            self.district.report_date == date(2025, 1, 6)
        ).first()
        self.assertEqual(row["expected_facilities"], 3)
        self.assertEqual(row["observed_facilities"], 1)
        self.assertEqual(row["missing_facilities"], 2)
        self.assertAlmostEqual(row["reporting_completeness_pct"], 33.33, places=2)

    def test_reporting_delay_is_calculated(self):
        row = self.delays.filter(
            self.delays.facility_id == "FAC-1"
        ).first()
        self.assertEqual(row["encounter_records"], 2)
        self.assertEqual(row["mean_delay_days"], 2.0)
        self.assertEqual(row["max_delay_days"], 2)

    def test_output_summaries_have_no_patient_identifier(self):
        self.assertNotIn("patient_id_hash", self.daily.columns)
        self.assertNotIn("patient_id_hash", self.district.columns)
        self.assertNotIn("patient_id_hash", self.delays.columns)


if __name__ == "__main__":
    unittest.main()
