import csv
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
DISTRICTS = ROOT / "data" / "reference" / "bangladesh_districts.csv"
FACILITIES = ROOT / "data" / "reference" / "synthetic_facilities.csv"
SCRIPT = ROOT / "scripts" / "generate_encounters.py"
SAMPLE = ROOT / "data" / "synthetic" / "encounters.csv"
TRUTH = ROOT / "data" / "synthetic" / "ground_truth_outbreaks.csv"
VALID_SEXES = {"female", "male", "other", "unknown", "not_recorded"}
VALID_CLASSIFICATIONS = {"suspected", "probable", "confirmed", "unknown"}


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


class EncounterGeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.districts = rows(DISTRICTS)
        cls.facilities = rows(FACILITIES)
        cls.encounters = rows(SAMPLE)
        cls.truth = rows(TRUTH)
        cls.district_codes = {r["district_code"] for r in cls.districts}
        cls.facility_ids = {r["facility_id"] for r in cls.facilities}

    def test_small_default_sample_has_expected_row_count(self):
        self.assertEqual(len(self.encounters), 2500)

    def test_all_encounters_have_required_fields(self):
        required = {
            "encounter_id", "patient_id_hash", "facility_id", "district_code",
            "disease", "encounter_date", "symptom_onset_date", "report_date",
            "age_years", "sex", "case_classification", "source_system",
            "source_record_id", "ingestion_timestamp",
        }
        self.assertTrue(required.issubset(set(self.encounters[0])))

    def test_clean_reference_rows_use_known_districts_and_facilities(self):
        # Exclude intentionally injected orphan facility records.
        for row in self.encounters:
            if row["facility_id"] != "FAC-ORPHAN-99999":
                self.assertIn(row["facility_id"], self.facility_ids)
                self.assertIn(row["district_code"], self.district_codes)

    def test_dates_are_within_2021_to_2025(self):
        for row in self.encounters:
            if row["encounter_date"] != "2025-99-99":
                value = date.fromisoformat(row["encounter_date"])
                self.assertGreaterEqual(value, date(2021, 1, 1))
                self.assertLessEqual(value, date(2025, 12, 31))

    def test_sex_and_classification_include_controlled_values_or_injected_defect(self):
        for row in self.encounters:
            self.assertTrue(row["sex"] in VALID_SEXES)
            self.assertTrue(
                row["case_classification"] in VALID_CLASSIFICATIONS
                or row["case_classification"] == "invalid_class"
            )

    def test_at_least_30_outbreaks_are_kept_in_separate_ground_truth(self):
        self.assertGreaterEqual(len(self.truth), 30)
        self.assertTrue(all(row["injected_by_generator"] == "true" for row in self.truth))
        self.assertTrue(all(row["outbreak_id"].startswith("GT-") for row in self.truth))

    def test_ground_truth_file_has_no_encounter_payload(self):
        forbidden = {"encounter_id", "patient_id_hash", "facility_id"}
        self.assertTrue(forbidden.isdisjoint(set(self.truth[0])))

    def test_defects_are_injected_at_approximately_configured_rates(self):
        ids = [r["encounter_id"] for r in self.encounters]
        duplicate_excess = len(ids) - len(set(ids))
        orphan_count = sum(r["facility_id"] == "FAC-ORPHAN-99999" for r in self.encounters)
        invalid_count = sum(
            r["age_years"] == "999"
            or r["disease"] == ""
            or r["case_classification"] == "invalid_class"
            or r["encounter_date"] == "2025-99-99"
            for r in self.encounters
        )
        self.assertGreater(duplicate_excess, 0)
        self.assertGreater(orphan_count, 0)
        self.assertGreater(invalid_count, 0)
        self.assertLessEqual(abs(duplicate_excess / len(ids) - 0.02), 0.01)
        self.assertLessEqual(abs(orphan_count / len(ids) - 0.005), 0.01)
        self.assertLessEqual(abs(invalid_count / len(ids) - 0.01), 0.01)

    def test_generator_is_reproducible(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "synthetic"
            subprocess.run([
                sys.executable, str(SCRIPT),
                "--districts", str(DISTRICTS),
                "--facilities", str(FACILITIES),
                "--output-dir", str(out_dir),
                "--scale-factor", "0.0001",
                "--seed", "4345",
            ], check=True, capture_output=True, text=True)
            self.assertEqual(
                (out_dir / "encounters.csv").read_bytes(),
                SAMPLE.read_bytes(),
            )
            self.assertEqual(
                (out_dir / "ground_truth_outbreaks.csv").read_bytes(),
                TRUTH.read_bytes(),
            )

    def test_generator_rejects_less_than_30_outbreaks(self):
        result = subprocess.run([
            sys.executable, str(SCRIPT),
            "--districts", str(DISTRICTS),
            "--facilities", str(FACILITIES),
            "--output-dir", str(ROOT / "tmp-invalid-test"),
            "--scale-factor", "0.00001",
            "--outbreak-count", "5",
        ], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("at least 30", result.stderr)


if __name__ == "__main__":
    unittest.main()
