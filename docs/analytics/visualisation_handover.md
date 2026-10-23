# Analytics Handover for Visualization Engineer

## Dataset
Current demonstration batch: analytics-100k

All encounter-derived values are synthetic proxies, not verified
clinical case counts.

## Output locations

### Weekly analytics input
`data/processed/analytics/weekly_input/batch_id=analytics-100k`

Aggregated weekly case counts by district and disease. This is an
internal analytical dataset derived from validated Silver records.
Do not expose encounter-level data in the dashboard.

### Outbreak detection
`data/processed/analytics/outbreaks/batch_id=analytics-100k`

Contains weekly detection scores and alert flags.

Threshold comparison results are documented in
`docs/analytics/outbreak_detection.md`.

### Forecasts
`data/processed/analytics/forecast/batch_id=analytics-100k/forecasts`

Contains:
- district_code
- disease
- forecast_week
- horizon_weeks
- forecast_cases
- lower80
- upper80

### Forecast evaluation
`data/processed/analytics/forecast/batch_id=analytics-100k/backtest_metrics`

Contains MAE and nominal 80% prediction-interval coverage by disease,
forecast horizon, and model. Evaluation methodology and counts require
further review before these results are treated as final.

### Resource demand and allocation
`data/processed/analytics/resource_allocation/batch_id=analytics-100k`

Contains estimated four-week resource demand, estimated cost,
recommended budget allocation, and an equal-budget baseline.

## Limitations
- Encounter-derived values are synthetic proxies.
- Forecasts do not use weather covariates.
- Prediction intervals are approximate and are not calibrated to 80%
  empirical coverage.
- The allocation is demand-proportional budget allocation, not a
  constrained linear/integer optimisation.
- The baseline is equal allocation, not population-proportional,
  because the district reference file has no population column.
- Analytics output is not automatically privacy-safe. Apply required
  primary and complementary suppression before serving dashboard data.
- Generated Parquet outputs are local artifacts; regenerate them from
  the documented commands rather than committing large data outputs.

## Reproduction commands

Build weekly aggregates:
`python -m analytics.detection.build_weekly_input --input data/processed/silver/batch_id=analytics-100k --output data/processed/analytics/weekly_input/batch_id=analytics-100k`

Run forecasts:
`python -m analytics.forecast.forecast_cases --input data/processed/analytics/weekly_input/batch_id=analytics-100k --output data/processed/analytics/forecast/batch_id=analytics-100k`

Run resource planning:
`python -m analytics.optimise.allocate_resources --forecasts data/processed/analytics/forecast/batch_id=analytics-100k/forecasts --districts data/reference/bangladesh_districts.csv --output data/processed/analytics/resource_allocation/batch_id=analytics-100k --budget 1000000`
