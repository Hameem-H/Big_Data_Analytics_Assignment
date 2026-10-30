#!/bin/sh
set -eu

cd /app

echo "======================================"
echo " EpiScope: generating 100K demo data"
echo "======================================"

python scripts/generate_encounters.py \
  --scale-factor 0.004 \
  --output-dir data/synthetic/analytics \
  --seed 4345 \
  --outbreak-count 30

echo "Running Bronze-to-Gold pipeline..."

python -m pipeline.orchestrator \
  --input data/synthetic/analytics/encounters.csv \
  --batch-id analytics-100k

echo "Building weekly analytics..."

python -m analytics.detection.build_weekly_input \
  --input data/processed/silver/batch_id=analytics-100k \
  --output data/processed/analytics/weekly_input/batch_id=analytics-100k

echo "Detecting outbreaks..."

python -m analytics.detection.detect_outbreaks \
  --input data/processed/analytics/weekly_input/batch_id=analytics-100k \
  --output data/processed/analytics/outbreaks/t400 \
  --threshold 4.0

echo "Evaluating outbreak detection..."

python -m analytics.detection.evaluate_outbreaks \
  --alerts data/processed/analytics/outbreaks/t400 \
  --ground-truth data/synthetic/analytics/ground_truth_outbreaks.csv \
  --output data/processed/analytics/outbreak_evaluation/t400

echo "Generating forecasts..."

python -m analytics.forecast.forecast_cases \
  --input data/processed/analytics/weekly_input/batch_id=analytics-100k \
  --output data/processed/analytics/forecast/batch_id=analytics-100k

echo "Allocating resources..."

python -m analytics.optimise.allocate_resources \
  --forecasts data/processed/analytics/forecast/batch_id=analytics-100k/forecasts \
  --districts data/reference/bangladesh_districts.csv \
  --output data/processed/analytics/resource_allocation/batch_id=analytics-100k \
  --budget 1000000

echo "Exporting dashboard data..."

python serving/export_dashboard_data.py \
  --weekly data/processed/analytics/weekly_input/batch_id=analytics-100k \
  --alerts data/processed/analytics/outbreaks/t400 \
  --forecasts data/processed/analytics/forecast/batch_id=analytics-100k/forecasts \
  --allocation data/processed/analytics/resource_allocation/batch_id=analytics-100k \
  --output serving/exports

echo "======================================"
echo " Pipeline completed."
echo " Dashboard: http://localhost:8000/dashboard/"
echo "======================================"

exec python -m http.server 8000 --bind 0.0.0.0
