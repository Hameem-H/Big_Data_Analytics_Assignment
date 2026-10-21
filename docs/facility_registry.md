# EpiScope Synthetic Facility Registry — v1

## Purpose

`data/reference/synthetic_facilities.csv` contains 640 synthetic facility records to support pipeline development and reproducible analytics. These are **not real facilities** and must not be presented as a real-world health-facility inventory.

## Fields

- `facility_id`: unique synthetic ID (`FAC-00001`, etc.).
- `facility_name`: explicitly synthetic name.
- `district_code`: foreign key to `bangladesh_districts.csv`.
- `facility_type`: one of the generator's controlled facility categories.
- `ownership_type`: `public`, `private`, or `ngo`.
- `bed_capacity`: synthetic nonnegative integer; bounds depend on facility type.
- `is_synthetic`: always `true`.
- `source_system`: generator version identifier.

## Generation and reproducibility

The standard generator creates 10 facilities for each of the 64 district registry rows, for 640 total records. Equal distribution is intentional for testing and **does not represent actual facility distribution**.

```bash
python scripts/generate_facility_registry.py
python -m unittest discover -s tests -v
```

The generator uses seed `4345` by default. Override it with `--seed`; use `--facilities-per-district` to change the scale. It reads the district registry and fails if the district list does not contain exactly 64 rows or has empty/duplicate codes.

## Limitations

No coordinates, addresses, phone numbers, or real facility names are included. The generated bed capacities are illustrative synthetic values, not operational capacity data.
