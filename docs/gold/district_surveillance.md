# Gold District Surveillance

## Purpose

This Gold-layer job produces daily and weekly disease surveillance
aggregates from validated Silver encounter records.

## Daily aggregate

Grouped by:
- Encounter date
- District code
- Disease

Case classifications are combined into the daily total.

## Weekly aggregate

Grouped by:
- Week start (Monday)
- District code
- Disease

## Privacy

Counts below the configured public threshold are replaced with null and
marked with 'count_suppressed=true' The default threshold is five.

Complementary suppression is applied within each daily or weekly
aggregation to hide an additional visible count when a single suppressed
count could otherwise be inferred from its parent total.

This is a baseline safeguard. Reconstruction across related daily, weekly,
classification-level, or other published aggregates has not been fully
assessed. Outputs should not be considered fully disclosure-safe for
public release until that cross-output risk is reviewed.

## Outputs

- `district_daily/batch_id=<batch-id>/`
- `district_weekly/batch_id=<batch-id>/`
- `quality/district_surveillance/batch_id=<batch-id>/`

Outputs are Parquet, with batch metrics stored separately as JSON text.

## Reprocessing

Each run overwrites the output for its batch ID rather than appending
duplicate aggregate rows.
