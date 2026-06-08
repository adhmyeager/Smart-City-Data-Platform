/*
macros/generate_schema_name.sql

Override dbt's default schema naming behavior.

By default dbt appends the target schema to custom schema names:
  dev_STAGING, dev_MART etc.

This macro makes dbt use the schema name exactly as defined
in dbt_project.yml without any prefix:
  STAGING, MART (exactly as Snowflake has them)

This is important so Power BI connects to SMART_CITY_DB.MART
not SMART_CITY_DB.dev_MART
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
