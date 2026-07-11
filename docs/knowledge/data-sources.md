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
- **Feed:** https://gbfs.divvybikes.com/gbfs/gbfs.json
- **Station status:** https://gbfs.divvybikes.com/gbfs/en/station_status.json
- **Station info:** https://gbfs.divvybikes.com/gbfs/en/station_information.json
- **Format:** JSON (GBFS — General Bikeshare Feed Specification)
- **Refresh:** ~60 seconds (genuine live stream)
- **No auth required**

---
