/*
================================================================================
FILE: get_amenity_columns.sql
LAYER: Macros
================================================================================

PURPOSE:
    Dynamically retrieve distinct amenity names from source data at compile time.
    Enables dynamic PIVOT without hardcoding amenity names.

MACROS:
    1. get_amenity_columns() - Gets amenities from amenities_changelog
    2. get_listing_amenity_columns() - Gets amenities from listings

USAGE:
    {% set amenity_columns = get_amenity_columns() %}
    {% for amenity in amenity_columns %}
        "{{ amenity }}"{% if not loop.last %},{% endif %}
    {% endfor %}

BENEFITS:
    - New amenities automatically appear when dbt is recompiled
    - No manual maintenance of amenity lists
    - Single source of truth for amenity names

NOTE:
    These macros execute queries at COMPILE TIME, not runtime.
    Results are cached during the dbt compilation phase.
================================================================================
*/


{% macro get_amenity_columns() %}
{# 
    Retrieve distinct amenity names from amenities_changelog table.
    Used for: stg_amenities_changelog, int_listing_amenities_scd
    
    Returns: List of amenity name strings
#}

{% set query %}
    SELECT DISTINCT trim(f.value::string) as amenity_name
    FROM {{ source('raw', 'amenities_changelog') }},
    LATERAL FLATTEN(input => PARSE_JSON(amenities)) f
    ORDER BY amenity_name
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% set amenity_list = results.columns[0].values() %}
{% else %}
    {% set amenity_list = [] %}
{% endif %}

{{ return(amenity_list) }}

{% endmacro %}


{% macro get_listing_amenity_columns() %}
{# 
    Retrieve distinct amenity names from listings table.
    Used for: stg_listings, int_availability_spans
    
    Returns: List of amenity name strings
    
    NOTE: May differ from get_amenity_columns() if listings and 
    amenities_changelog have different amenity sets.
#}

{% set query %}
    SELECT DISTINCT trim(f.value::string) as amenity_name
    FROM {{ source('raw', 'listings') }},
    LATERAL FLATTEN(input => PARSE_JSON(amenities)) f
    ORDER BY amenity_name
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% set amenity_list = results.columns[0].values() %}
{% else %}
    {% set amenity_list = [] %}
{% endif %}

{{ return(amenity_list) }}

{% endmacro %}
