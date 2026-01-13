/*
================================================================================
FILE: detect_schema_changes.sql
LAYER: Macros
================================================================================

PURPOSE:
    Detects when new amenity columns are added to source data and determines
    if a full refresh is required for the fact table.
    
USAGE:
    Run as operation: dbt run-operation check_amenity_schema_change
    
LOGIC:
    - Compares current amenity columns from source with existing columns in target table
    - Returns list of new amenities that don't exist in target
    - Used to decide between incremental vs full-refresh strategy

================================================================================
*/

{% macro check_amenity_schema_change() %}
    {#
        Standalone operation to check for schema changes.
        Run this as: dbt run-operation check_amenity_schema_change
        
        Returns a message indicating if full refresh is needed.
    #}
    
    {# Get all amenity columns from source #}
    {%- set source_amenities = get_amenity_columns() -%}
    
    {# Get target table relation #}
    {%- set target_relation = adapter.get_relation(
        database='RENTAL_PROPERTY',
        schema='MART',
        identifier='FCT_DAILY_LISTING_PERFORMANCE'
    ) -%}
    
    {# Get columns from target table if it exists #}
    {%- set target_columns = [] -%}
    {%- if target_relation is not none -%}
        {%- set columns = adapter.get_columns_in_relation(target_relation) -%}
        {%- for col in columns -%}
            {%- do target_columns.append(col.name) -%}
        {%- endfor -%}
    {%- endif -%}
    
    {# Find amenities in source but not in target #}
    {%- set new_amenities = [] -%}
    {%- for amenity in source_amenities -%}
        {%- if amenity not in target_columns -%}
            {%- do new_amenities.append(amenity) -%}
        {%- endif -%}
    {%- endfor -%}
    
    {% if new_amenities | length > 0 %}
        {{ log("🚨 SCHEMA CHANGE DETECTED!", info=True) }}
        {{ log("New amenities found in source: " ~ new_amenities | join(", "), info=True) }}
        {{ log("", info=True) }}
        {{ log("ACTION REQUIRED:", info=True) }}
        {{ log("Run: dbt run --full-refresh -s fct_daily_listing_performance+", info=True) }}
        {{ return({"schema_changed": true, "new_amenities": new_amenities}) }}
    {% else %}
        {{ log("✅ No schema changes detected. Incremental run is safe.", info=True) }}
        {{ return({"schema_changed": false, "new_amenities": []}) }}
    {% endif %}
{% endmacro %}
