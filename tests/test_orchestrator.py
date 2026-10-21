import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.orchestrator import run_pipeline


class OrchestratorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

        self.input = self.root / "encounters.csv"
        self.input.write_text("id,value\n1,test\n", encoding="utf-8")

        self.districts = self.root / "districts.csv"
        self.districts.write_text("district_code\nBD-01\n", encoding="utf-8")

        self.facilities = self.root / "facilities.csv"
        self.facilities.write_text(
            "facility_id,district_code\nFAC-1,BD-01\n",
            encoding="utf-8",
        )

        self.processed = self.root / "processed"
        self.manifests = self.root / "manifests"

        self.patches = [
            patch("pipeline.orchestrator.ingest_encounters", return_value=2),
            patch(
                "pipeline.orchestrator.validate_encounters",
                return_value={
                    "input_records": 2,
                    "accepted_records": 2,
                    "quarantined_records": 0,
                },
            ),
            patch(
                "pipeline.orchestrator.build_daily_surveillance",
                return_value={"aggregate_rows": 1},
            ),
            patch(
                "pipeline.orchestrator.build_district_surveillance",
                return_value={"outputs": {}},
            ),
            patch(
                "pipeline.orchestrator.run_facility_reporting",
                return_value={"outputs": {}},
            ),
        ]
        self.mocks = [p.start() for p in self.patches]
        self.addCleanup(self.stop_patches)

    def stop_patches(self):
        for p in reversed(self.patches):
            p.stop()

    def run_batch(self, batch_id="test-batch", reprocess=False):
        return run_pipeline(
            spark=object(),
            input_path=str(self.input),
            districts_path=str(self.districts),
            facilities_path=str(self.facilities),
            processed_path=str(self.processed),
            batch_id=batch_id,
            manifest_root=str(self.manifests),
            reprocess=reprocess,
        )

    def manifest(self):
        path = self.manifests / "test-batch.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_completed_unchanged_batch_is_skipped(self):
        self.run_batch()
        result = self.run_batch()

        self.assertTrue(result["skipped"])
        self.mocks[0].assert_called_once()

    def test_changed_source_is_rejected_without_reprocess(self):
        self.run_batch()
        self.input.write_text("id,value\n2,changed\n", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "different source"):
            self.run_batch()

    def test_reprocess_preserves_previous_run(self):
        self.run_batch()
        self.input.write_text("id,value\n2,changed\n", encoding="utf-8")

        result = self.run_batch(reprocess=True)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(result["history"]), 1)
        self.assertEqual(result["history"][0]["status"], "completed")

    def test_failed_stage_is_recorded(self):
        self.mocks[1].side_effect = RuntimeError("test failure")

        with self.assertRaisesRegex(RuntimeError, "test failure"):
            self.run_batch()

        manifest = self.manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["stages"]["silver"]["status"], "failed")
        self.assertEqual(manifest["error"]["message"], "test failure")

    def test_invalid_batch_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid batch ID"):
            self.run_batch(batch_id="../unsafe")


if __name__ == "__main__":
    unittest.main()
