# DBT

### Key Jinja Variables
| Variable | Purpose |
|---|---|
| `adapter.type()` | Check warehouse type for macro dispatch (`'postgres'`, `'bigquery'`) |
| `this` | Reference to the current model |
| `ref('model_name')` | Reference another model (builds dependency graph) |
| `source('schema', 'table')` | Reference a source table |

### Common Commands
```bash
dbt run                          # run all models
dbt run --select staging         # run only staging models
dbt run --select marts           # run only mart models
dbt test                         # run all tests
dbt test --select stg_crime_events  # test one model
dbt compile                      # compile SQL without running
dbt debug                        # check connection/config
dbt docs generate && dbt docs serve  # generate and view docs
```

### Model Layers
- **staging** — rename, type-cast, light cleaning (1:1 with source tables) → written to `staging` schema
- **marts** — final analytics tables (facts + dims) → written to `mart` schema
- **intermediate** (skipped for now) — joins, aggregations; can add `intermediate` schema later if needed

### Macro Dispatch Pattern
```sql
{% macro try_cast(column, target_type) %}
  {% if adapter.type() == 'postgres' %}
    {{ column }}::{{ target_type }}
  {% elif adapter.type() == 'bigquery' %}
    SAFE_CAST({{ column }} AS {{ target_type }})
  {% else %}
    {{ column }}::{{ target_type }}
  {% endif %}
{% endmacro %}
```

### Schema Naming — `generate_schema_name` Override

DBT's default `generate_schema_name` macro **concatenates** the profile's base schema with the model's custom schema:
`staging` (profile) + `_` + `mart` (custom) = `staging_mart`

This is rarely what you want. Override it in `macros/generate_schema_name.sql`:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}
```
This returns the custom schema as-is (`mart`, `staging`), falling back to the profile schema when no custom schema is set.

### DBT 1.11 Test Config

The `where` clause on generic tests must be nested under `config:`, not top-level:
```yaml
# ✅ Correct (DBT 1.11+)
tests:
  - relationships:
      to: ref('dim_community_area')
      field: community_area_id
      config:
        where: "community_area_id != 0"

# ❌ Wrong (deprecated in 1.11)
tests:
  - relationships:
      to: ref('dim_community_area')
      field: community_area_id
      where: "community_area_id != 0"
```

### dbt-expectations (Great Expectations macros for DBT)

Installed via `dbt/packages.yml` → `metaplane/dbt_expectations` 0.10.10 (maintained fork of `calogica/dbt_expectations`). Run `dbt deps` to install.

Provides 50+ GE-style tests as dbt macros. Used in `schema.yml` like standard tests:

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_between:
      min_value: 41.64
      max_value: 42.03
      config:
        where: "latitude IS NOT NULL"
```

**Gotchas:**
- `expect_column_values_to_be_in_set` does NOT work on Postgres BOOLEAN columns — generates `boolean = text` comparison. Use `not_null` instead (BOOLEAN can't hold other values).
- Always check actual `min()/max()` before setting range thresholds — use `SELECT min(col), max(col) FROM table`.
- `where` config goes under `config:` (same as standard tests in DBT 1.11+).

### dbt Power User Extension (VS Code / Devin IDE)

For dbt projects in a subdirectory (e.g. `dbt/` not at workspace root):
1. Create `.vscode/settings.json` with `"dbt.allowListFolders": ["dbt"]`
2. Copy `profiles.yml` to `~/.dbt/profiles.yml` (default location the extension checks)
3. Reload the IDE window after making these changes

---
