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

  Usage:
    {{ try_cast('community_area', 'int') }} AS community_area_id
#}
{% macro try_cast(column, target_type) %}
  {% if adapter.type() == 'postgres' %}
    {{ column }}::{{ target_type }}
  {% elif adapter.type() == 'bigquery' %}
    SAFE_CAST({{ column }} AS {{ target_type }})
  {% else %}
    {{ column }}::{{ target_type }}
  {% endif %}
{% endmacro %}
