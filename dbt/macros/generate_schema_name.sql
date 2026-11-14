{# dbt's default prefixes the target schema (gold_staging); use the model's schema as is. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
