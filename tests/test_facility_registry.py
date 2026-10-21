import csv
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DISTRICTS = ROOT / "data" / "reference" / "bangladesh_districts.csv"
FACILITIES = ROOT / "data" / "reference" / "synthetic_facilities.csv"
GENERATOR = ROOT / "scripts" / "generate_facility_registry.py"

REQUIRED_FIELDS = {
    "facility_id", "facility_name", "district_code", "facility_type",
    "ownership_type", "bed_capacity", "is_synthetic", "source_system",
}
VALID_TYPES = {
    "district_hospital", "upazila_health_complex", "community_clinic",
    "union_health_family_welfare_centre", "private_hospital", "ngo_clinic",
}
VALID_OWNERS = {"public", "private", "ngo"}


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return set(reader.fieldnames or []), list(reader)


class FacilityRegistryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.district_fields, cls.districts = read_csv(DISTRICTS)
        cls.fields, cls.rows = read_csv(FACILITIES)
        cls.district_codes = {row["district_code"] for row in cls.districts}

    def test_has_at_least_600_facilities(self):
        self.assertGreaterEqual(len(self.rows), 600)

    def test_required_fields_exist_and_are_nonempty(self):
        self.assertTrue(REQUIRED_FIELDS.issubset(self.fields))
        for row in self.rows:
            for field in REQUIRED_FIELDS:
                self.assertTrue(row[field].strip(), f"Empty {field}: {row}")

    def test_facility_ids_are_unique(self):
        ids = [row["facility_id"] for row in self.rows]
        self.assertEqual(len(ids), len(set(ids)))

    def test_every_district_reference_is_valid(self):
        self.assertEqual(
            {row["district_code"] for row in self.rows} - self.district_codes,
            set(),
        )

    def test_every_district_is_covered(self):
        self.assertEqual(
            {row["district_code"] for row in self.rows},
            self.district_codes,
        )

    def test_facilities_are_explicitly_marked_synthetic(self):
        self.assertEqual({row["is_synthetic"] for row in self.rows}, {"true"})
        self.assertEqual(
            {row["source_system"] for row in self.rows},
            {"episcope_synthetic_generator_v1"},
        )

    def test_facility_types_and_ownership_are_valid(self):
        self.assertTrue({row["facility_type"] for row in self.rows}.issubset(VALID_TYPES))
        self.assertTrue({row["ownership_type"] for row in self.rows}.issubset(VALID_OWNERS))

    def test_bed_capacity_is_integer_and_nonnegative(self):
        for row in self.rows:
            capacity = int(row["bed_capacity"])
            self.assertGreaterEqual(capacity, 0)

    def test_each_district_has_ten_synthetic_facilities(self):
        counts = Counter(row["district_code"] for row in self.rows)
        self.assertEqual(set(counts.values()), {10})
        self.assertEqual(len(counts), 64)

    def test_generator_is_reproducible(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "facilities.csv"
            result = subprocess.run(
                [
                    sys.executable, str(GENERATOR),
                    "--districts", str(DISTRICTS),
                    "--output", str(out),
                    "--facilities-per-district", "10",
                    "--seed", "4345",
                ],
                capture_output=True, text=True, check=True,
            )
            self.assertIn("Generated 640", result.stdout)
            self.assertEqual(out.read_bytes(), FACILITIES.read_bytes())


if __name__ == "__main__":
    unittest.main()
