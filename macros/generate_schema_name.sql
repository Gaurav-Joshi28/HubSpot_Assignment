/*
================================================================================
FILE: generate_schema_name.sql
LAYER: Macros
================================================================================

PURPOSE:
    Override dbt's default schema naming behavior.
    Use custom schema names directly without any prefix.

DEFAULT BEHAVIOR:
    dbt normally creates: target_schema_custom_schema (e.g., development_staging)

OUR BEHAVIOR:
    Use custom schema directly: staging, development, mart, analytics

AVAILABLE SCHEMAS:
    - raw (source data)
    - stagging (staging models)
    - development (intermediate models)
    - mart (dimension and fact tables)
    - analytics (business question answers)

CONFIGURATION:
    Set +schema in dbt_project.yml for each model folder:
    
    models:
      rental_property:
        staging:
          +schema: stagging
        intermediate:
          +schema: development
        marts:
          +schema: mart
        analyses:
          +schema: analytics
================================================================================
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
