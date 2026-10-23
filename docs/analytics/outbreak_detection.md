# Outbreak Detection and Evaluation

## Method
The detection pipeline aggregates validated encounter records by
district, disease, and week. It applies a C2-style score using a
historical baseline and configurable alert threshold.

## Input
- Validated Silver records:
  data/processed/silver/batch_id=analytics-100k
- Weekly analytical aggregates:
  data/processed/analytics/weekly_input/batch_id=analytics-100k
- Synthetic outbreak ground truth:
  data/synthetic/analytics/ground_truth_outbreaks.csv

## Evaluation Results

| Threshold | Detected / 30 | Sensitivity | False alerts | False alerts per district-year |
|---|---:|---:|---:|---:|
| 3.0 | 16 | 53.3% | 2375 | 7.42 |
| 3.5 | 16 | 53.3% | 2367 | 7.40 |
| 4.0 | 13 | 43.3% | 861 | 2.86 |

Median detection delay was 3 days for all three thresholds.

## Interpretation
Threshold 3.0 detects more known outbreaks but produces more alerts.
Threshold 4.0 reduces false alerts substantially but also reduces
sensitivity. Threshold 3.5 provides little improvement over 3.0.

These results use synthetic ground truth and encounter-based proxies,
not independently verified clinical case counts. The false-alert
definition and denominator should be reviewed before interpreting
these metrics as independent outbreak-event rates.

## Reproduction

Build weekly aggregates:

    python -m analytics.detection.build_weekly_input \
      --input data/processed/silver/batch_id=analytics-100k \
      --output data/processed/analytics/weekly_input/batch_id=analytics-100k

Run detection with a chosen threshold:

    python -m analytics.detection.detect_outbreaks \
      --input data/processed/analytics/weekly_input/batch_id=analytics-100k \
      --output data/processed/analytics/outbreaks/t400 \
      --threshold 4.0

Evaluate alerts:

    python -m analytics.detection.evaluate_outbreaks \
      --alerts data/processed/analytics/outbreaks/t400 \
      --ground-truth data/synthetic/analytics/ground_truth_outbreaks.csv \
      --output data/processed/analytics/outbreak_evaluation/t400

## Handover
The Visualization Engineer should use aggregated outputs only.
Do not expose individual encounter records in dashboard-serving data.
