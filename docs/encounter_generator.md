# EpiScope Synthetic Encounter Generator — v1

## Purpose

Generate reproducible synthetic encounter records for 2021–2025 using the district and facility reference registries. The generator supports scale factors, reporting delay, controlled data defects, and a separate outbreak ground-truth file.

## Default development run

```bash
python scripts/generate_encounters.py
python -m unittest discover -s tests -v
```

The default scale factor is `0.0001`, which produces 2,500 encounter rows. This is a small development dataset, not the full assignment dataset.

## Scale factors

The target row count is `25,000,000 * scale_factor`.

- `--scale-factor 0.0001`: 2,500 rows
- `--scale-factor 0.001`: 25,000 rows
- `--scale-factor 0.01`: 250,000 rows
- `--scale-factor 0.25`: 6,250,000 rows
- `--scale-factor 0.5`: 12,500,000 rows
- `--scale-factor 1`: 25,000,000 rows

**Do not run scale factor 1 until the generator has been benchmarked and the Spark pipeline can consume the output.** The full run writes large CSV files and can take substantial time and disk space.

## Output files

- `data/synthetic/encounters.csv`: encounter rows with source IDs, disease, dates, age, sex, case classification, and ingestion timestamp.
- `data/synthetic/ground_truth_outbreaks.csv`: injected outbreak intervals for evaluation. The detector must not read this file as a feature or input.

## Defects and delays

The generator injects approximately 2% duplicate encounter IDs, 0.5% orphan facility references, and 1% invalid values. Defect categories can overlap in a row, so observed rates may vary slightly. Reporting delays are sampled from a right-skewed exponential distribution and capped at 21 days. Report dates may extend into 2026 for encounters near the end of 2025.

## Reproducibility

Default seed: `4345`. Use `--seed` to select another deterministic run. The generator streams encounters to CSV rather than holding all generated encounter records in memory. Defect injection is decided row-by-row to avoid storing large sets of row positions.

## Important limitations

This is a synthetic workload for software testing and method evaluation, not observed public-health data. Disease labels and generated outbreak intervals are simulated. The current outbreak mechanism provides a separated ground-truth register; validate and calibrate detection performance during the analytics stage.
