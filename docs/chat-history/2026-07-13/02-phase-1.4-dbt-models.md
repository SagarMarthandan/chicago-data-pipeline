# 2026-07-13 — Phase 1.4: DBT Models

## Topic
DBT transformation layer: staging view + 4 mart tables + 31 data tests (20 standard + 11 dbt-expectations).

## What was done
- Created `dbt/` project: `dbt_project.yml`, `profiles.yml`, `macros/`, `models/staging/`, `models/marts/`, `seeds/`, `packages.yml`
- `macros/try_cast.sql` — warehouse-portable cast (Postgres `::` vs BigQuery `SAFE_CAST`)
- `macros/generate_schema_name.sql` — overrides DBT schema concatenation so models go to `staging`/`mart` (not `staging_staging`/`staging_mart`)
- `models/staging/stg_crime_events.sql` — view: rename, cast, dedup on id via `DISTINCT ON`
- `models/marts/` — `dim_date` (365 rows), `dim_community_area` (77 rows), `dim_crime_type` (323 rows), `fact_crime_events` (263,393 rows)
- `seeds/community_areas.csv` — 77 community areas from Chicago Data Portal (`igwz-8jzy`)
- `packages.yml` — `metaplane/dbt_expectations` 0.10.10 (Great Expectations macros for dbt)
- `.vscode/settings.json` — dbt Power User extension config (`dbt.allowListFolders`, `dbt.dbtPythonPathOverride`)
- `.gitignore` updated — exceptions for `dbt/seeds/*.csv` and `.vscode/settings.json`; added `dbt/profiles.yml` to ignore
- `~/.dbt/profiles.yml` — copy for dbt Power User extension default location
- Plan renumbered: merged old 1.2 (data source spec) into 1.2 (ingestion), shifted 1.3→1.4→1.5→1.6→1.7 down by one

## Errors hit
1. DBT created `staging_mart`/`staging_staging` schemas → `generate_schema_name` macro override
2. `where` config deprecation in DBT 1.11 → moved under `config:`
3. DBT not installed despite being in `pyproject.toml` → `uv sync`
4. `expect_column_values_to_be_in_set` on BOOLEAN fails (`boolean = text`) → replaced with `not_null`
5. Longitude bounds `[-87.9, -87.5]` too tight (801 rows outside) → widened to `[-87.95, -87.52]`
6. dbt Power User "language server not running" → `.vscode/settings.json` + `~/.dbt/profiles.yml`

## Verification
- `dbt debug` — all checks passed
- `dbt build` — 37/37 PASS (1 seed + 5 models + 31 tests)
- Analytical query: top 10 community areas by crime count (Austin: 12,700, Near North Side: 11,196, ...)

## Key decisions
- `generate_schema_name` override returns custom schema as-is (not concatenated)
- Staging = view, marts = table
- `DISTINCT ON (id) ORDER BY id, updated_at DESC` for dedup
- `community_area_id = 0` excluded from relationships test via `where: "community_area_id != 0"`
- `try_cast` Postgres branch does plain cast (fails loudly on bad data — intentional)
- dbt-expectations installed for GE-style range/bounds tests
