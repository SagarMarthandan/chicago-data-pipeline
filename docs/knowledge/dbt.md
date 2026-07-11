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

---
