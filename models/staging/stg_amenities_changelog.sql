{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: stg_amenities_changelog.sql
LAYER: Staging
SCHEMA: stagging
================================================================================

PURPOSE:
    Parse and pivot historical amenity changes for each listing.
    Enables point-in-time accurate amenity tracking (SCD Type 2).

LOGIC:
    1. Flatten JSON amenities array using LATERAL FLATTEN
    2. Create presence indicator (1) for each amenity
    3. Dynamically PIVOT amenities into boolean columns
    4. Column names are generated at compile time (no hardcoding)

SOURCE: raw.amenities_changelog
GRAIN: One row per listing per change date (listing_id + change_at)

KEY TRANSFORMATIONS:
    - amenities JSON array → individual boolean columns via PIVOT
    - Dynamic column generation using get_amenity_columns() macro

DOWNSTREAM DEPENDENCIES:
    - int_listing_amenities_scd (creates validity windows)
================================================================================
*/

{% set amenity_columns = get_amenity_columns() %}

with flattened_amenities as (
    select 
        listing_id,
        change_at::date as change_at,
        trim(f.value::string) as amenity_name,
        1 as present
    from {{ source('raw', 'amenities_changelog') }},
    lateral flatten(input => parse_json(amenities)) f 
),

pivoted as (
    select * 
    from flattened_amenities
    pivot (
        max(present) 
        for amenity_name in (
            {% for amenity in amenity_columns %}
                '{{ amenity }}'{% if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
)

select
    listing_id,
    change_at,
    {% for amenity in amenity_columns %}
    coalesce("'{{ amenity }}'"::boolean, false) as "{{ amenity }}"{% if not loop.last %},{% endif %}
    {% endfor %}
from pivoted
order by listing_id, change_at
