"""Coordinate EpiScope batch processing with a persistent JSON manifest."""

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from pipeline.bronze.ingest_encounters import ingest_encounters
from pipeline.silver.validate_encounters import validate_encounters
from pipeline.gold.daily_surveillance import build_daily_surveillance
from pipeline.gold.district_surveillance import build_district_surveillance
from pipeline.gold.facility_reporting import run_facility_reporting


BATCH_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}")


def sha256_file(path: str) -> str:
    """Return the SHA-256 digest of a source file."""
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def load_manifest(path: Path) -> dict | None:
    """Read an existing manifest, or return None if it does not exist."""
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    if not isinstance(manifest, dict):
        raise ValueError(f"Invalid batch manifest: {path}")

    return manifest


def write_manifest(path: Path, manifest: dict) -> None:
    """Write a manifest atomically to avoid partial JSON files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".json.tmp")

    with temporary_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()

    temporary_path.replace(path)


def run_pipeline(
    spark: SparkSession,
    input_path: str,
    districts_path: str,
    facilities_path: str,
    processed_path: str,
    batch_id: str,
    manifest_root: str = "data/processed/manifests",
    reprocess: bool = False,
) -> dict:
    """Run Bronze, Silver, and Gold stages for one source batch."""

    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise ValueError("Invalid batch ID")

    source = Path(input_path)
    if not source.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    for reference_path in (districts_path, facilities_path):
        if not Path(reference_path).is_file():
            raise FileNotFoundError(
                f"Reference CSV does not exist: {reference_path}"
            )

    source_hash = sha256_file(input_path)
    manifest_path = Path(manifest_root) / f"{batch_id}.json"
    previous = load_manifest(manifest_path)

    if previous:
        same_source = previous.get("source_sha256") == source_hash

        if (
            previous.get("status") == "completed"
            and same_source
            and not reprocess
        ):
            return {
                **previous,
                "skipped": True,
                "skip_reason": "Completed batch with unchanged source",
            }

        if not same_source and not reprocess:
            raise ValueError(
                "This batch ID has a different source from its previous run. "
                "Choose a new batch ID or pass --reprocess explicitly."
            )

        history = list(previous.get("history", []))
        previous_run = {
            key: value for key, value in previous.items()
            if key != "history"
        }
        history.append(previous_run)
    else:
        history = []

    manifest = {
        "batch_id": batch_id,
        "source_path": str(source.resolve()),
        "source_sha256": source_hash,
        "status": "running",
        "started_at": utc_now(),
        "updated_at": utc_now(),
        "stages": {},
        "history": history,
    }
    write_manifest(manifest_path, manifest)

    bronze_path = str(
        Path(processed_path) / "bronze" / "encounters"
        / f"batch_id={batch_id}"
    )
    silver_path = str(
        Path(processed_path) / "silver" / f"batch_id={batch_id}"
    )
    gold_path = str(Path(processed_path) / "gold")

    current_stage = None

    try:
        current_stage = "bronze"
        manifest["stages"]["bronze"] = {"status": "running"}
        write_manifest(manifest_path, manifest)

        bronze_count = ingest_encounters(
            spark=spark,
            input_path=input_path,
            output_path=str(
                Path(processed_path) / "bronze" / "encounters"
            ),
            batch_id=batch_id,
        )
        manifest["stages"]["bronze"] = {
            "status": "completed",
            "records": bronze_count,
            "output_path": bronze_path,
        }
        write_manifest(manifest_path, manifest)

        current_stage = "silver"
        manifest["stages"]["silver"] = {"status": "running"}
        write_manifest(manifest_path, manifest)

        silver_metrics = validate_encounters(
            spark=spark,
            bronze_path=bronze_path,
            districts_path=districts_path,
            facilities_path=facilities_path,
            output_path=processed_path,
            batch_id=batch_id,
        )
        manifest["stages"]["silver"] = {
            "status": "completed",
            **silver_metrics,
            "output_path": silver_path,
        }
        write_manifest(manifest_path, manifest)

        current_stage = "gold_daily"
        manifest["stages"]["gold_daily"] = {"status": "running"}
        write_manifest(manifest_path, manifest)
        daily_metrics = build_daily_surveillance(
            spark=spark,
            silver_path=silver_path,
            output_path=gold_path,
            batch_id=batch_id,
        )
        manifest["stages"]["gold_daily"] = {
            "status": "completed",
            **daily_metrics,
        }
        write_manifest(manifest_path, manifest)

        current_stage = "gold_district"
        manifest["stages"]["gold_district"] = {"status": "running"}
        write_manifest(manifest_path, manifest)
        district_metrics = build_district_surveillance(
            spark=spark,
            silver_path=silver_path,
            output_path=gold_path,
            batch_id=batch_id,
        )
        manifest["stages"]["gold_district"] = {
            "status": "completed",
            **district_metrics,
        }
        write_manifest(manifest_path, manifest)

        current_stage = "gold_facility_reporting"
        manifest["stages"]["gold_facility_reporting"] = {
            "status": "running"
        }
        write_manifest(manifest_path, manifest)
        facility_metrics = run_facility_reporting(
            spark=spark,
            silver_path=silver_path,
            facilities_path=facilities_path,
            output_path=gold_path,
            batch_id=batch_id,
        )
        manifest["stages"]["gold_facility_reporting"] = {
            "status": "completed",
            **facility_metrics,
        }

        manifest["status"] = "completed"
        manifest["completed_at"] = utc_now()
        manifest["updated_at"] = utc_now()
        write_manifest(manifest_path, manifest)

        return manifest

    except Exception as exc:
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        if current_stage:
            manifest["stages"][current_stage] = {
                "status": "failed",
                "error": error,
            }

        manifest["status"] = "failed"
        manifest["error"] = error
        manifest["updated_at"] = utc_now()
        write_manifest(manifest_path, manifest)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the EpiScope Bronze-to-Gold pipeline."
    )
    parser.add_argument(
        "--input", default="data/synthetic/encounters.csv"
    )
    parser.add_argument(
        "--districts", default="data/reference/bangladesh_districts.csv"
    )
    parser.add_argument(
        "--facilities", default="data/reference/synthetic_facilities.csv"
    )
    parser.add_argument("--processed", default="data/processed")
    parser.add_argument(
        "--manifest-root", default="data/processed/manifests"
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Explicitly rerun a completed batch",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Pipeline-Orchestrator")
        .getOrCreate()
    )

    try:
        result = run_pipeline(
            spark=spark,
            input_path=args.input,
            districts_path=args.districts,
            facilities_path=args.facilities,
            processed_path=args.processed,
            batch_id=args.batch_id,
            manifest_root=args.manifest_root,
            reprocess=args.reprocess,
        )
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
