import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "encounter.schema.json"

VALID_RECORD = {
    "encounter_id": "ENC-000001",
    "facility_id": "FAC-0001",
    "district_code": "BD-01",
    "disease": "dengue",
    "encounter_date": "2024-06-12",
    "report_date": "2024-06-14",
    "age_years": 22,
    "sex": "female",
    "case_classification": "suspected",
    "source_system": "synthetic_generator",
    "ingestion_timestamp": "2024-06-14T10:30:00Z",
    "patient_id_hash": "sha256-demo-hash"
}


class EncounterContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with SCHEMA_PATH.open(encoding="utf-8") as f:
            cls.schema = json.load(f)
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(
            cls.schema, format_checker=FormatChecker()
        )

    def test_valid_record_passes(self):
        self.assertEqual(list(self.validator.iter_errors(VALID_RECORD)), [])

    def test_missing_required_field_fails(self):
        record = dict(VALID_RECORD)
        del record["district_code"]
        self.assertTrue(list(self.validator.iter_errors(record)))

    def test_invalid_date_fails(self):
        record = dict(VALID_RECORD, encounter_date="2024-99-42")
        self.assertTrue(list(self.validator.iter_errors(record)))

    def test_age_above_120_fails(self):
        record = dict(VALID_RECORD, age_years=121)
        self.assertTrue(list(self.validator.iter_errors(record)))

    def test_unapproved_case_classification_fails(self):
        record = dict(VALID_RECORD, case_classification="definitely_confirmed")
        self.assertTrue(list(self.validator.iter_errors(record)))

    def test_direct_identifier_is_rejected(self):
        record = dict(VALID_RECORD, patient_name="Example Person")
        self.assertTrue(list(self.validator.iter_errors(record)))

    def test_report_can_arrive_after_encounter(self):
        # Delayed reporting is allowed by the contract.
        record = dict(VALID_RECORD, report_date="2024-07-01")
        self.assertEqual(list(self.validator.iter_errors(record)), [])


if __name__ == "__main__":
    unittest.main()
