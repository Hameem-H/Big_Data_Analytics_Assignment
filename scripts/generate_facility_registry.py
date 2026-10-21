#!/usr/bin/env python3
"""Generate a deterministic, explicitly synthetic EpiScope facility registry."""
import argparse
import csv
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DISTRICTS = ROOT / "data" / "reference" / "bangladesh_districts.csv"
DEFAULT_OUTPUT = ROOT / "data" / "reference" / "synthetic_facilities.csv"

FACILITY_TYPES = [
    ("district_hospital", 100, 500, "public"),
    ("upazila_health_complex", 20, 100, "public"),
    ("community_clinic", 0, 5, "public"),
    ("union_health_family_welfare_centre", 0, 10, "public"),
    ("private_hospital", 10, 250, "private"),
    ("ngo_clinic", 0, 15, "ngo"),
]

def read_districts(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 64:
        raise ValueError(f"Expected exactly 64 district rows, found {len(rows)}")
    codes = [row.get("district_code", "").strip() for row in rows]
    if any(not code for code in codes) or len(set(codes)) != len(codes):
        raise ValueError("District registry has empty or duplicate district_code values")
    return sorted(rows, key=lambda row: row["district_code"])

def generate_facilities(districts, facilities_per_district=10, seed=4345):
    if facilities_per_district < 1:
        raise ValueError("facilities_per_district must be >= 1")
    rng = random.Random(seed)
    facilities = []
    facility_number = 1
    # Every district receives the same number of synthetic records so the
    # reference layer is balanced; this is not an estimate of real facility counts.
    type_cycle = [
        "district_hospital",
        "upazila_health_complex",
        "community_clinic",
        "community_clinic",
        "community_clinic",
        "union_health_family_welfare_centre",
        "union_health_family_welfare_centre",
        "private_hospital",
        "ngo_clinic",
        "upazila_health_complex",
    ]
    bounds = {name: (low, high, owner) for name, low, high, owner in FACILITY_TYPES}
    for district in districts:
        for local_idx in range(facilities_per_district):
            facility_type = type_cycle[local_idx % len(type_cycle)]
            low, high, owner = bounds[facility_type]
            facilities.append({
                "facility_id": f"FAC-{facility_number:05d}",
                "facility_name": f"Synthetic Facility {facility_number:05d}",
                "district_code": district["district_code"],
                "facility_type": facility_type,
                "ownership_type": owner,
                "bed_capacity": rng.randint(low, high),
                "is_synthetic": "true",
                "source_system": "episcope_synthetic_generator_v1",
            })
            facility_number += 1
    return facilities

def write_csv(rows, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "facility_id", "facility_name", "district_code", "facility_type",
        "ownership_type", "bed_capacity", "is_synthetic", "source_system",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--districts", type=Path, default=DEFAULT_DISTRICTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--facilities-per-district", type=int, default=10)
    parser.add_argument("--seed", type=int, default=4345)
    args = parser.parse_args()

    districts = read_districts(args.districts)
    facilities = generate_facilities(districts, args.facilities_per_district, args.seed)
    write_csv(facilities, args.output)
    print(f"Generated {len(facilities)} explicitly synthetic facilities.")
    print(f"Districts covered: {len({r['district_code'] for r in facilities})}")
    print(f"Output: {args.output}")

if __name__ == "__main__":
    main()
