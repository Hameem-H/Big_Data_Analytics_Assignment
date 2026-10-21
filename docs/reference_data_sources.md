# EpiScope Reference Data — District Registry

## Dataset

`data/reference/bangladesh_districts.csv` contains 64 district records and their parent administrative divisions.

Fields:
- `district_code`: stable internal EpiScope key (`BD-01` … `BD-64`); these are **not official government codes**.
- `district_name_en`: English name used in the project.
- `district_name_bn`: Bangla name.
- `division_name_en`: parent division.
- `code_system`: versioned identifier namespace (`episcope_internal_v1`).
- `source_url`: official Bangladesh National Portal district-list page used to check district membership and division grouping.

## Source and normalization

The district membership and division grouping are based on the Bangladesh National Portal's district list:
https://bangladesh.gov.bd/views/district-list/District-List/

The source presents names in Bangla. English spellings in this CSV are normalized project labels for consistent joins; transliteration variants exist across public sources. Preserve `district_code` as the join key rather than joining on names.

## Limitations

- This file is a district registry, not a boundary geometry dataset.
- It does not contain population, coordinates, polygons, or facility locations.
- Internal `BD-xx` keys are project-generated, not official administrative identifiers.
- If a district is renamed or a division assignment changes, document the change and version the registry rather than silently reassigning existing keys.

## Validation

From the repository root:

```bash
python -m unittest discover -s tests -v
```
