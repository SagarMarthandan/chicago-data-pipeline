{#
  generate_schema_name — override DBT's default schema concatenation.

  By default, DBT concatenates the profile's base schema with the model's
  custom schema: base_schema + "_" + custom_schema → e.g. "staging_mart".
  We want the custom schema to be the ACTUAL schema name (just "mart",
  just "staging"), not a concatenation.

  This macro returns the custom schema name as-is when one is provided
  in dbt_project.yml. When no custom schema is set (e.g. seeds without a
  schema override), it falls back to the profile's target.schema.

  Result:
    - staging models → staging schema (not staging_staging)
    - mart models → mart schema (not staging_mart)
    - seeds → mart schema (from dbt_project.yml seed config)
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}
