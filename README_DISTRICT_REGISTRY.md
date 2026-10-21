# EpiScope District Registry — Deliverable 2A

Adds the 64-district reference table, source notes, and automated validation tests.

## Files
- `data/reference/bangladesh_districts.csv`
- `docs/reference_data_sources.md`
- `tests/test_district_registry.py`

## Install / test
The tests use only the Python standard library:

```bash
python -m unittest discover -s tests -v
```

Expected: all 7 district-registry tests pass. Existing encounter contract tests should continue to pass too.

## Important
`BD-01` through `BD-64` are stable project-generated identifiers, not official government district codes. Use `district_code` as the join key. English names are normalized labels based on the official Bangladesh National Portal district list.
