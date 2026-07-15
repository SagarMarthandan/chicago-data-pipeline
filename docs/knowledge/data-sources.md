# Data Sources Reference

### Chicago Crime (Socrata API)

- **Dataset:** "Crimes - 2001 to Present"
- **Resource ID:** `ijzp-q8t2` (NOT `ijzp-q4t2` — the plan had a typo)
- **API endpoint:** `https://data.cityofchicago.org/resource/ijzp-q8t2.json`
- **Catalog lookup:** `https://api.us.socrata.com/api/catalog/v1?q=crimes+2001+present&domains=data.cityofchicago.org`
- **Size:** ~8M rows total. Single year (~263K rows for 2023)
- **Update cadence:** Daily, ~1 day lag
- **Auth:** App token is OPTIONAL. Without it: 1,000 req/hr. With it: 10,000 req/hr

**Socrata API (SoQL) parameters:**

| Parameter | Purpose | Example |
|---|---|---|
| `$limit` | Max rows per request (max 50,000) | `$limit=50000` |
| `$offset` | Skip rows for pagination | `$offset=50000` |
| `$where` | Filter condition (SQL-like) | `$where=year=2023` |
| `$order` | Sort order (required for stable pagination) | `$order=id` |

**App token:** Passed via `X-App-Token` HTTP header. Register at https://data.cityofchicago.org → My Account → App Tokens.

**API response quirks:**
- Returns all values as strings (even numbers and booleans)
- `location` column is a nested dict: `{"latitude": "...", "longitude": "...", "human_address": "..."}`
- `:@computed_region_*` columns are internal geocoding metadata — drop them
- `arrest` and `domestic` come as strings `"true"`/`"false"` — convert to bool

**Key columns:**

| Column | Type (after cleaning) | Notes |
|---|---|---|
| `id` | string | unique case ID — primary key |
| `case_number` | string | agency-specific, not unique |
| `date` | string | ISO 8601 timestamp (e.g. `2023-01-01T05:54:00.000`) |
| `primary_type` | string | e.g. THEFT, BATTERY, NARCOTICS (31 unique values in 2023) |
| `description` | string | sub-category |
| `location_description` | string | e.g. STREET, APARTMENT (0.6% null) |
| `arrest` | bool | whether arrest was made |
| `domestic` | bool | domestic-related |
| `community_area` | float | FK to community area (0.007% null) |
| `district` | int | police district |
| `ward` | float | aldermanic ward (0.001% null) |
| `latitude` / `longitude` | float | 0.8% null — keep as null, don't drop |
| `year` | int | partition column |

### Divvy Bike Share (GBFS API)

- **Discovery endpoint:** `https://gbfs.divvybikes.com/gbfs/gbfs.json`
- **GBFS version:** 1.1
- **Operator:** Lyft (actual feed URLs resolve to `gbfs.lyft.com`)
- **Timezone:** America/Chicago
- **TTL:** 60 seconds (cache refresh interval)
- **No auth required**
- **Languages:** en, fr, es (we use `en`)

**Available feeds (12 total):**

| Feed | URL | Use in pipeline |
|---|---|---|
| `station_status` | `.../en/station_status.json` | **Streaming source** — polled every 60s by Kafka producer |
| `station_information` | `.../en/station_information.json` | **Dimension table** — relatively static (station name, lat, lon, capacity) |
| `system_information` | `.../en/system_information.json` | System metadata (system_id, name, timezone) — not piped |
| `system_regions` | `.../en/system_regions.json` | Only 1 region (Evanston) — not useful for community area mapping |
| `free_bike_status` | `.../en/free_bike_status.json` | GPS of free-floating bikes — not used in Phase 2 |
| `system_alerts` | `.../en/system_alerts.json` | Service alerts — not used in Phase 2 |
| `gbfs_versions` | `.../en/gbfs_versions.json` | Version history — not used |
| `ebikes_at_stations` | `.../en/ebikes_at_stations.json` | E-bike details — not used |
| `system_hours` / `system_calendar` / `system_pricing_plans` | various | Operational metadata — not used |

#### station_status (streaming source)

- **2,016 stations** per poll (verified 2026-07-15)
- **Structure:** `{"data": {"stations": [...]}, "last_updated": <epoch>, "ttl": 60}`
- **Refresh:** ~60 seconds

**Fields in ALL stations:**

| Field | Type | Notes |
|---|---|---|
| `station_id` | string | **Mixed format**: 667 UUIDs (e.g. `a3af8123-...`), 1,349 numeric strings (e.g. `2232759736070696510`). Must stay as string — cannot cast to bigint |
| `num_bikes_available` | int | Never null in observed data |
| `num_bikes_disabled` | int | |
| `num_docks_available` | int | |
| `num_docks_disabled` | int | |
| `is_installed` | int (0/1) | **Not boolean** — integer 0 or 1. Cast to boolean in Spark/DBT |
| `is_renting` | int (0/1) | **Not boolean** — same as above |
| `is_returning` | int (0/1) | **Not boolean** — same as above |
| `last_reported` | int (epoch seconds) | Unix timestamp. One station had `86400` (Jan 2 1970 — dead station). Filter stale stations |
| `legacy_id` | string | Old Divvy station ID (e.g. `"474"`) |
| `num_ebikes_available` | int | |
| `eightd_has_available_keys` | boolean | Actual boolean (unlike is_* fields) |

**Optional fields (not in all stations):**

| Field | Type | Notes |
|---|---|---|
| `num_scooters_available` | int | Present in some stations only — Spark schema must tolerate absence |
| `num_scooters_unavailable` | int | Same — optional |

**Key quirks:**
- `is_renting`/`is_returning`/`is_installed` are **integers 0/1**, NOT booleans — the plan's DBT model assumed boolean; need explicit cast
- `station_id` is a **string** — the plan's DBT model had `station_id::bigint` which will fail on UUID-format IDs
- One station (`2232759736070696510`) had `last_reported: 86400` (epoch for Jan 2, 1970) — likely a decommissioned station. Consider filtering `last_reported > <some recent threshold>`
- All 2,016 station_ids in station_status have matching entries in station_information (perfect 1:1)

#### station_information (dimension source)

- **2,016 stations** (matches station_status exactly)
- **Structure:** `{"data": {"stations": [...]}, "last_updated": <epoch>, "ttl": 60}`
- **Cadence:** Relatively static — changes only when stations are added/removed/relocated

**Fields in ALL stations:**

| Field | Type | Notes |
|---|---|---|
| `station_id` | string | Same IDs as station_status |
| `name` | string | Human-readable (e.g. "Damen Ave & Ogden Ave") |
| `lat` | float | Station latitude |
| `lon` | float | Station longitude |
| `capacity` | int | Total dock capacity |
| `station_type` | string | `classic` or `lightweight` |
| `has_kiosk` | boolean | Whether station has a kiosk |
| `external_id` | string | Same as station_id in most cases |
| `electric_bike_surcharge_waiver` | boolean | |
| `eightd_has_key_dispenser` | boolean | |
| `eightd_station_services` | array | Usually empty |
| `rental_uris` | object | Nested: `{"android": "...", "ios": "..."}` |

**Optional fields (not in all stations):**

| Field | Type | Notes |
|---|---|---|
| `short_name` | string | e.g. "CHI02349" — not present in all stations |
| `rental_methods` | array | e.g. `["KEY", "CREDITCARD", "TRANSITCARD"]` — not in all |

**Note:** `short_name` and `rental_methods` appear in most but not all stations. Spark schema must tolerate absence.

#### Pipeline implications (Phase 2)

- **station_status** → Kafka topic `divvy_station_status` → Spark Structured Streaming → `raw.station_status` (append-only, one row per station per poll)
- **station_information** → DBT seed or one-time Spark load → `mart.dim_station` (dimension table, refreshed periodically)
- **station_id must be string** throughout the pipeline (UUID + numeric mix)
- **is_renting/is_returning/is_installed** need `CAST(col AS BOOLEAN)` in Spark (0→false, 1→true)
- **Optional scooter fields** — use nullable schema in Spark, don't fail on missing
- **No community area in GBFS** — to answer "crime near Divvy station", we'll need to spatially join station lat/lon to community area boundaries (Phase 2.5 or later)
---
