# EpiScope Facility Registry — Deliverable 2B

Adds a reproducible generator and a checked-in dataset of 640 explicitly synthetic health-facility records.

## Contents
- `data/reference/synthetic_facilities.csv`
- `scripts/generate_facility_registry.py`
- `tests/test_facility_registry.py`
- `docs/facility_registry.md`

## Run
From the repository root:

```bash
python scripts/generate_facility_registry.py
python -m unittest discover -s tests -v
```

The generator uses the district registry as its reference, creates 10 records per district, and uses seed 4345 by default. These records are synthetic and are not an actual health-facility inventory.
