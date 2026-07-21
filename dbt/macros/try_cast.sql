{#
  try_cast macro — warehouse-portable type casting.

  Postgres has no TRY_CAST (that's Snowflake/DuckDB syntax).
  BigQuery uses SAFE_CAST (returns null on failure).

  The Postgres branch does a PLAIN cast (not guarded). This is deliberate:
  if bad data reaches DBT, the model should fail loudly so you catch it
  at the source (Spark/Kafka should have cleaned it upstream). A silent
  null hides upstream bugs.

  BigQuery's SAFE_CAST is used because BigQuery doesn't raise on cast
  failure — it returns null — so the two branches converge on the same
  "null on bad input" behavior, just via different mechanisms.

  Type mapping: the macro is called with Postgres-style type names
  (int, timestamp). BigQuery needs INT64, TIMESTAMP. The bigquery branch
  maps the common ones. Add new mappings as needed.

  Usage:
    {{ try_cast('community_area', 'int') }} AS community_area_id
#}
{% macro try_cast(column, target_type) %}
  {% if adapter.type() == 'postgres' %}
    {{ column }}::{{ target_type }}
  {% elif adapter.type() == 'bigquery' %}
    {% set bq_type = {
      'int': 'INT64',
      'bigint': 'INT64',
      'timestamp': 'TIMESTAMP',
      'date': 'DATE',
      'double': 'FLOAT64',
      'double precision': 'FLOAT64',
      'varchar': 'STRING',
      'text': 'STRING',
      'boolean': 'BOOLEAN',
      'bool': 'BOOLEAN'
    }.get(target_type, target_type) %}
    SAFE_CAST({{ column }} AS {{ bq_type }})
  {% else %}
    {{ column }}::{{ target_type }}
  {% endif %}
{% endmacro %}
