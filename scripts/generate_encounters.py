#!/usr/bin/env python3
"""Generate reproducible synthetic EpiScope encounters and separate outbreak ground truth.

The generator streams encounter rows to disk. The default run is small for development.
Use --scale-factor 1 only after benchmarking and confirming sufficient disk space.
"""
import argparse
import csv
import datetime as dt
import random
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DISTRICTS = ROOT / "data" / "reference" / "bangladesh_districts.csv"
DEFAULT_FACILITIES = ROOT / "data" / "reference" / "synthetic_facilities.csv"
DEFAULT_OUT_DIR = ROOT / "data" / "synthetic"
BASE_ENCOUNTERS = 25_000_000
START_DATE = dt.date(2021, 1, 1)
END_DATE = dt.date(2025, 12, 31)
DISEASE_WEIGHTS = {
    "dengue": 0.24,
    "acute_respiratory_infection": 0.22,
    "acute_diarrhoeal_disease": 0.20,
    "malaria": 0.08,
    "suspected_measles": 0.07,
    "skin_infection": 0.08,
    "other_febrile_illness": 0.11,
}
SEXES = ["female", "male", "other", "unknown", "not_recorded"]
CLASSIFICATIONS = ["suspected", "probable", "confirmed", "unknown"]
FACILITY_ORPHAN_RATE = 0.005
INVALID_VALUE_RATE = 0.01
DUPLICATE_ID_RATE = 0.02

ENCOUNTER_FIELDS = [
    "encounter_id", "patient_id_hash", "facility_id", "district_code",
    "disease", "encounter_date", "symptom_onset_date", "report_date",
    "age_years", "sex", "case_classification", "source_system",
    "source_record_id", "ingestion_timestamp",
]
GROUND_TRUTH_FIELDS = [
    "outbreak_id", "district_code", "disease", "start_date", "end_date",
    "peak_multiplier", "injected_by_generator", "seed",
]

def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))

def date_range_days():
    return (END_DATE - START_DATE).days + 1

def load_references(district_path, facility_path):
    districts = read_csv(district_path)
    facilities = read_csv(facility_path)
    if len(districts) != 64:
        raise ValueError(f"Expected 64 districts, found {len(districts)}")
    district_codes = {r.get("district_code", "").strip() for r in districts}
    if "" in district_codes or len(district_codes) != len(districts):
        raise ValueError("District registry has empty or duplicate district_code values")
    facility_ids = {r.get("facility_id", "").strip() for r in facilities}
    if len(facilities) < 600:
        raise ValueError(f"Expected at least 600 facilities, found {len(facilities)}")
    if "" in facility_ids or len(facility_ids) != len(facilities):
        raise ValueError("Facility registry has empty or duplicate facility IDs")
    if any(r.get("district_code") not in district_codes for r in facilities):
        raise ValueError("Facility registry contains unknown district_code")
    return sorted(districts, key=lambda r: r["district_code"]), facilities

def weighted_choice(rng, weights):
    items = list(weights.items())
    threshold = rng.random() * sum(weight for _, weight in items)
    cumulative = 0.0
    for item, weight in items:
        cumulative += weight
        if threshold <= cumulative:
            return item
    return items[-1][0]

def make_outbreaks(districts, rng, seed, count=30):
    """Create outbreak intervals in a separate file, never embedded as a row feature."""
    records = []
    seen = set()
    disease_names = list(DISEASE_WEIGHTS)
    max_start_offset = date_range_days() - 45
    while len(records) < count:
        district_code = rng.choice(districts)["district_code"]
        disease = rng.choice(disease_names)
        key = (district_code, disease)
        if key in seen:
            continue
        seen.add(key)
        start = START_DATE + dt.timedelta(days=rng.randint(0, max_start_offset))
        duration = rng.randint(14, 42)
        end = min(start + dt.timedelta(days=duration - 1), END_DATE)
        records.append({
            "outbreak_id": f"GT-{len(records)+1:03d}",
            "district_code": district_code,
            "disease": disease,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "peak_multiplier": f"{rng.uniform(2.0, 5.0):.2f}",
            "injected_by_generator": "true",
            "seed": str(seed),
        })
    return records

def write_rows(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

def generate(args):
    rng = random.Random(args.seed)
    districts, facilities = load_references(args.districts, args.facilities)
    target_rows = int(BASE_ENCOUNTERS * args.scale_factor)
    if target_rows < 1:
        raise ValueError("Scale factor must produce at least one encounter")
    outbreaks = make_outbreaks(districts, rng, args.seed, count=max(30, args.outbreak_count))
    district_codes = [r["district_code"] for r in districts]
    facility_ids = [r["facility_id"] for r in facilities]
    facility_district = {r["facility_id"]: r["district_code"] for r in facilities}
    events_by_district = defaultdict(list)
    for event in outbreaks:
        events_by_district[event["district_code"]].append(
            (dt.date.fromisoformat(event["start_date"]),
             dt.date.fromisoformat(event["end_date"]),
             event["disease"],
             float(event["peak_multiplier"]))
        )

    encounter_path = args.output_dir / "encounters.csv"
    truth_path = args.output_dir / "ground_truth_outbreaks.csv"
    encounter_path.parent.mkdir(parents=True, exist_ok=True)
    duplicate_count = orphan_count = invalid_count = 0
    with encounter_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ENCOUNTER_FIELDS, lineterminator="\n")
        writer.writeheader()
        for i in range(target_rows):
            encounter_id = f"ENC-{i+1:09d}"
            if i > 0 and rng.random() < DUPLICATE_ID_RATE:
                source_index = rng.randrange(0, i)
                encounter_id = f"ENC-{source_index+1:09d}"
                duplicate_count += 1

            facility_id = rng.choice(facility_ids)
            district_code = facility_district[facility_id]
            encounter_date = START_DATE + dt.timedelta(days=rng.randrange(date_range_days()))
            disease = weighted_choice(rng, DISEASE_WEIGHTS)

            # Add an observable increase in the disease associated with a known event
            # during that event's interval. The detector still receives only encounters.
            active_events = [
                event for event in events_by_district[district_code]
                if event[0] <= encounter_date <= event[1]
            ]
            if active_events and rng.random() < 0.65:
                disease = rng.choice(active_events)[2]

            delay = min(21, int(rng.expovariate(1 / 4.5)))
            report_date = encounter_date + dt.timedelta(days=delay)
            onset_date = encounter_date - dt.timedelta(days=rng.randint(0, 5))
            ingestion_dt = dt.datetime.combine(
                report_date,
                dt.time(hour=rng.randrange(24), minute=rng.randrange(60)),
                tzinfo=dt.timezone.utc,
            )
            row = {
                "encounter_id": encounter_id,
                "patient_id_hash": f"PID-{rng.getrandbits(64):016x}",
                "facility_id": facility_id,
                "district_code": district_code,
                "disease": disease,
                "encounter_date": encounter_date.isoformat(),
                "symptom_onset_date": onset_date.isoformat(),
                "report_date": report_date.isoformat(),
                "age_years": rng.randint(0, 100),
                "sex": rng.choice(SEXES),
                "case_classification": rng.choices(CLASSIFICATIONS, weights=[0.58, 0.17, 0.20, 0.05])[0],
                "source_system": "episcope_synthetic_generator_v1",
                "source_record_id": f"SRC-{i+1:09d}",
                "ingestion_timestamp": ingestion_dt.isoformat().replace("+00:00", "Z"),
            }
            if rng.random() < FACILITY_ORPHAN_RATE:
                row["facility_id"] = "FAC-ORPHAN-99999"
                orphan_count += 1
            if rng.random() < INVALID_VALUE_RATE:
                invalid_field = rng.choice(["age_years", "disease", "case_classification", "encounter_date"])
                if invalid_field == "age_years":
                    row[invalid_field] = "999"
                elif invalid_field == "disease":
                    row[invalid_field] = ""
                elif invalid_field == "case_classification":
                    row[invalid_field] = "invalid_class"
                else:
                    row[invalid_field] = "2025-99-99"
                invalid_count += 1
            writer.writerow(row)

    write_rows(truth_path, GROUND_TRUTH_FIELDS, outbreaks)
    return {
        "encounter_rows": target_rows,
        "duplicate_id_rows_injected": duplicate_count,
        "orphan_facility_rows_injected": orphan_count,
        "invalid_value_rows_injected": invalid_count,
        "outbreaks": len(outbreaks),
        "encounters_path": str(encounter_path),
        "ground_truth_path": str(truth_path),
    }

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--districts", type=Path, default=DEFAULT_DISTRICTS)
    parser.add_argument("--facilities", type=Path, default=DEFAULT_FACILITIES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--scale-factor", type=float, default=0.0001,
                        help="Fraction of 25,000,000 base rows; default generates 2,500 rows")
    parser.add_argument("--seed", type=int, default=4345)
    parser.add_argument("--outbreak-count", type=int, default=30)
    args = parser.parse_args()
    if args.scale_factor <= 0 or args.scale_factor > 1:
        parser.error("--scale-factor must be > 0 and <= 1")
    if args.outbreak_count < 30:
        parser.error("--outbreak-count must be at least 30")
    summary = generate(args)
    print("Synthetic encounter generation complete.")
    for key, value in summary.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    main()
