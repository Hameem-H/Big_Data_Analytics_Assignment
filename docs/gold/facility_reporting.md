# Gold Facility Reporting Analytics

## Purpose

This module produces facility-day activity, district-level reporting
completeness proxies, and reporting-delay summaries from Silver records.

## Reporting activity

The initial implementation assumes each registered facility is expected
to report every calendar day in the Silver encounter-date range.

A facility is marked as observed when at least one encounter exists for
that facility and encounter date. This is a proxy for reporting activity,
not proof of an official report submission. In particular, a zero-case
report cannot be distinguished from a missing report without a dedicated
report-submission dataset.

## District-level completeness

The district daily table contains expected facilities, observed facilities,
missing facilities, and the percentage observed.

## Reporting delays

The delay summary calculates report date minus encounter date and reports
mean, median, maximum delay, and the number of records reported more than
seven days after encounter.

## Outputs

- `facility_reporting_daily/batch_id=<batch-id>/`
- `district_reporting_daily/batch_id=<batch-id>/`
- `facility_reporting_delays/batch_id=<batch-id>/`
- `quality/facility_reporting/batch_id=<batch-id>/`

Outputs are Parquet, with batch metrics stored separately as JSON text.

## Limitations

Expected daily reporting is an initial assumption. Production reporting
cadence should be configured using the actual reporting policy. Encounter
activity alone cannot establish official submission compliance.
