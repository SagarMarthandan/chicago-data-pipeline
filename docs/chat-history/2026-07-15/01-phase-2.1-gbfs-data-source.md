# Session 01 — Phase 2.1: Divvy GBFS Data Source Exploration

**Date:** 2026-07-15
**Session type:** Phase 2 start — data source exploration
**Phase:** 2.1

## Summary

Explored the Divvy GBFS (General Bikeshare Feed Specification) live API to understand the data schema before building the streaming pipeline. Fetched all GBFS feed endpoints, documented the full station_status and station_information schemas, and discovered four design-changing findings that will affect Spark streaming and DBT modeling in later sub-phases.

## What Was Done

### 1. GBFS Feed Discovery
Fetched the root `gbfs.json` manifest from `https://gbfs.divvybikes.com/gbfs/gbfs.json`. Found 3 feed groups:
- `gbfs_versions` — API versioning
- `system_information` — system-wide metadata (operator, timezone, etc.)
- `station_information` — static station details (name, location, capacity)
- `station_status` — live station status (bikes available, renting/returning status)

### 2. station_status.json Schema Analysis
Fetched `station_status.json` — 2,016 stations. Each station has:
- **Mandatory (12 fields):** `station_id`, `num_bikes_available`, `num_bikes_available_types` (array of "electric"/"classic"), `is_renting`, `is_returning`, `is_installed`, `last_reported`, `num_docks_available`, `num_docks_disabled`, `eightd_has_available_keys`, `station_id_isnumeric` (optional per spec but present in Divvy)
- **Optional (2 fields):** `num_scooters_available`, `num_scooters_unavailable` — not present on all stations

### 3. station_information.json Schema Analysis
Fetched `station_information.json` — 2,016 stations (same count). Each has:
- `station_id`, `name`, `lat`, `lon`, `address`, `rental_methods`, `capacity`, `rental_uris` (Android/iOS), `eightd_station_services`, `has_kiosk`, `station_type`

### 4. Four Design-Changing Findings

| # | Finding | Impact |
|---|---|---|
| 1 | `station_id` is mixed format: 667 UUIDs + 1,349 numeric strings | Must stay as string throughout pipeline. Plan's DBT model had `station_id::bigint` — will fail on UUIDs. |
| 2 | `is_renting`, `is_returning`, `is_installed` are integers 0/1, NOT booleans | Need explicit `CAST(col AS BOOLEAN)` in Spark/DBT. Plan assumed booleans. |
| 3 | `num_scooters_available`/`num_scooters_unavailable` are optional | Spark schema must use nullable fields. Can't assume they're always present. |
| 4 | One dead station had `last_reported: 86400` (Jan 2, 1970 epoch) | Filter stale stations in Spark or DBT. 86400 = 1 day after epoch, clearly a sensor error. |

## Decisions Made

- **station_id as string throughout pipeline** — UUIDs make bigint impossible. This overrides the plan's `station_id::bigint`.
- **is_* fields need int→boolean cast** — GBFS spec says integer 0/1, not boolean. Spark and DBT must cast.
- **Optional scooter fields are nullable** — Spark schema must tolerate their absence.
- **No code written in this sub-phase** — exploration only. Code starts in 2.2+.

## Files Created/Modified

- `docs/knowledge/data-sources.md` — expanded with full GBFS schema documentation (endpoints, field types, design-changing findings)
- `changelog.md` — added Phase 2.1 entry (no errors, 4 design decisions)
- `docs/operations-performed.md` — added Phase 2.1 audit entry
- `docs/phases/phase-2.1-gbfs-data-source.md` — created phase completion doc
- `docs/phases/README.md` — updated phase index

## Key Context

- GBFS stands for General Bikeshare Feed Specification — an open data standard for bikeshare systems
- Divvy's GBFS base URL: `https://gbfs.divvybikes.com/gbfs/`
- `station_status.json` is the live feed we'll stream (updates every ~60s)
- `station_information.json` is relatively static (station locations rarely change) — could be a seed/dimension table
- 2,016 stations = ~2,016 messages per poll cycle in the producer
- The `station_id_isnumeric` field is present in Divvy's feed but not in the GBFS spec — it's a Divvy-specific extension

## Errors Encountered

None — this was a pure exploration sub-phase with no code execution.

## User Preferences Learned

- User wants to understand the GBFS spec, not just use it — explained what GBFS is and why it exists
