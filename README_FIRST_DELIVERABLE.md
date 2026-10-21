# EpiScope — Data Engineer first deliverable

This package adds the first shared data contract and its automated tests. It does not replace existing repository files.

## Contents

- `docs/data_contract.md` — human-readable field meanings, privacy rules, and layer responsibilities.
- `schemas/encounter.schema.json` — machine-readable JSON Schema Draft 2020-12.
- `tests/test_encounter_contract.py` — seven automated tests covering valid records, required fields, date formats, age bounds, controlled values, privacy guardrails, and delayed reporting.
- `requirements-data-engineer.txt` — test dependency.

## Install and test

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-data-engineer.txt
python -m unittest discover -s tests -v
```

Expected result: 7 tests pass.

## Important scope note

This is the first contract, not the complete pipeline. The 64-district reference table, facility registry, disease vocabulary, generator, Spark jobs, quarantine output, and Gold aggregates are subsequent deliverables. Do not generate the full 25-million-row dataset until the small-scale generator and pipeline tests pass.
