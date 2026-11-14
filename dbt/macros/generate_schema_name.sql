{#
  dbt's default would name the schema "<target schema>_<custom schema>"
  (e.g. SILVER_SILVER). Use the custom schema exactly as written instead.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | upper }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
