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
marked with `count_suppressed=true`. The default threshold is five.

This threshold suppression is a baseline safeguard, not a complete
disclosure-control solution. Complementary suppression and review of
related aggregates are still needed before public release.

## Outputs

- `district_daily/batch_id=<batch-id>/`
- `district_weekly/batch_id=<batch-id>/`
- `quality/district_surveillance/batch_id=<batch-id>/`

Outputs are Parquet, with batch metrics stored separately as JSON text.

## Reprocessing

Each run overwrites the output for its batch ID rather than appending
duplicate aggregate rows.
