# EpiScope Data Contract — v0.1

## Purpose

This contract defines the shared schema for encounter-level records entering the EpiScope Bronze layer. The Data Engineer owns the contract; Analytics and Visualization should use the same field names and definitions.

The machine-readable schema is `schemas/encounter.schema.json`.

## Encounter grain

One record represents one reported encounter or case record from a source system. It is not necessarily a unique person or a confirmed diagnosis.

## Required fields

| Field | Type | Meaning |
|---|---|---|
| `encounter_id` | string | Source encounter record identifier |
| `facility_id` | string | Identifier in the facility registry |
| `district_code` | string | Code for one of the project's 64 districts |
| `disease` | string | Canonical disease label |
| `encounter_date` | date | Date of the encounter |
| `report_date` | date | Date the record was reported |
| `age_years` | integer or null | Age in completed years, from 0 to 120 |
| `sex` | enum | `female`, `male`, `other`, `unknown`, or `not_recorded` |
| `case_classification` | enum | `suspected`, `probable`, `confirmed`, or `unknown` |
| `source_system` | string | Source system name or code |
| `ingestion_timestamp` | date-time | UTC ingestion timestamp |

## Optional fields

- `patient_id_hash`: salted pseudonymous patient identifier.
- `symptom_onset_date`: symptom onset date, if available.
- `source_record_id`: source-system record identifier when distinct from `encounter_id`.

## Data quality and privacy rules

1. Direct identifiers, including patient names, phone numbers, national IDs, and full addresses, must not enter Silver or Gold.
2. Keep any secret hashing salt outside the repository and version control.
3. Unknown `district_code` and `facility_id` references must be quarantined during Silver processing.
4. `report_date` may be later than `encounter_date`; reporting delay is expected.
5. If present, `symptom_onset_date` should not be later than `encounter_date` unless an exception is documented.
6. Suppress public-facing aggregate counts of 1–4 and apply complementary suppression where necessary to prevent reconstruction.
7. Deduplication should account for `source_system`; encounter IDs are not assumed to be globally unique across all sources.
8. Late-arriving reports are accepted when `report_date` is later than
   `encounter_date`. To refresh aggregates after source data changes, rerun
   the affected batch with `--reprocess`; separate batch outputs are not
   automatically merged into existing aggregates.
## Layer responsibilities

- **Bronze:** retain source records and ingestion metadata under restricted access.
- **Silver:** normalize values, validate references and dates, deduplicate, pseudonymize, and quarantine invalid records with reason codes.
- **Gold:** publish approved aggregates and apply privacy suppression before serving.

## Contract versioning

Breaking field renames, removals, type changes, or semantic changes require a contract version update and coordination with Analytics and Visualization. Additive optional fields must be documented.
