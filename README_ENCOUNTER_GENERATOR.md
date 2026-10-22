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

## Full-Scale Benchmark (25 Million Records)

Test environment: local Linux machine using PySpark 4.1.3.

| Metric | Result |
|---|---:|
| Generated encounter records | 25,000,000 |
| Synthetic data generation time | 5 min 7.6 sec |
| Bronze records ingested | 25,000,000 |
| Bronze ingestion time | 35.95 sec |
| Bronze Parquet output size | 1.1 GB |
| Bronze ingestion throughput | Approximately 695,500 records/sec |

The generator produced the full 25-million-record dataset. Bronze ingestion successfully processed all records and wrote the output in Parquet format under `data/processed/bronze/encounters/batch_id=full-scale-25m`.

These measurements cover data generation and Bronze ingestion only. Full-scale Silver validation and Gold aggregation were not benchmarked.
