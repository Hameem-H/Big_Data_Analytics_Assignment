import csv
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "data" / "reference" / "bangladesh_districts.csv"

EXPECTED_COUNTS = {
    "Barishal": 6,
    "Chattogram": 11,
    "Dhaka": 13,
    "Khulna": 10,
    "Mymensingh": 4,
    "Rajshahi": 8,
    "Rangpur": 8,
    "Sylhet": 4,
}
EXPECTED_FIELDS = {
    "district_code",
    "district_name_en",
    "district_name_bn",
    "division_name_en",
    "code_system",
    "source_url",
}


class DistrictRegistryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            cls.fields = set(reader.fieldnames or [])
            cls.rows = list(reader)

    def test_has_exactly_64_districts(self):
        self.assertEqual(len(self.rows), 64)

    def test_required_fields_exist_and_are_nonempty(self):
        self.assertTrue(EXPECTED_FIELDS.issubset(self.fields))
        for row in self.rows:
            for field in EXPECTED_FIELDS:
                self.assertTrue(row[field].strip(), f"Empty {field}: {row}")

    def test_district_codes_are_unique(self):
        codes = [row["district_code"] for row in self.rows]
        self.assertEqual(len(codes), len(set(codes)))
        self.assertEqual(set(codes), {f"BD-{i:02d}" for i in range(1, 65)})

    def test_english_district_names_are_unique_case_insensitively(self):
        names = [row["district_name_en"].casefold() for row in self.rows]
        self.assertEqual(len(names), len(set(names)))

    def test_division_counts_match_expected(self):
        counts = Counter(row["division_name_en"] for row in self.rows)
        self.assertEqual(dict(counts), EXPECTED_COUNTS)

    def test_code_system_is_explicitly_internal(self):
        self.assertEqual({row["code_system"] for row in self.rows}, {"episcope_internal_v1"})

    def test_no_district_is_missing_bangla_name(self):
        self.assertTrue(all(row["district_name_bn"].strip() for row in self.rows))


if __name__ == "__main__":
    unittest.main()
