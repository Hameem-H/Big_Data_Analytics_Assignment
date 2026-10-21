# EpiScope Encounter Generator — Deliverable 3A

A seeded, configurable generator for synthetic encounter records and a separate outbreak ground-truth file.

## Run

```bash
python scripts/generate_encounters.py
python -m unittest discover -s tests -v
```

Default run: 2,500 encounter rows over 2021–2025, plus at least 30 ground-truth outbreak intervals.

## Full-scale target

`--scale-factor 1` means 25 million rows. Start with the default sample and benchmark smaller scale factors first; do not run the full dataset on a laptop without checking available disk space and runtime.

## Outputs
- `data/synthetic/encounters.csv`
- `data/synthetic/ground_truth_outbreaks.csv`

The ground-truth file is for evaluation only and must remain separate from detector inputs.
