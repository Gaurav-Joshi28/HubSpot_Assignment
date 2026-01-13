{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: int_listing_amenities_scd.sql
LAYER: Intermediate
SCHEMA: development
================================================================================

PURPOSE:
    Implement SCD Type 2 (Slowly Changing Dimension) for listing amenities.
    Creates validity windows (valid_from, valid_to) for each amenity state.
    Enables point-in-time accurate amenity lookups.

LOGIC:
    1. Take amenity snapshots from stg_amenities_changelog
    2. Use LEAD() window function to find the next change date
    3. Set valid_to = next_change_date - 1 day (or 9999-12-31 if current)
    4. Each row represents a period when the listing had a specific amenity set

EXAMPLE:
    If listing 123 added "Air conditioning" on 2022-03-15:
    | listing_id | valid_from | valid_to   | Air conditioning |
    | 123        | 2022-01-01 | 2022-03-14 | FALSE            |
    | 123        | 2022-03-15 | 9999-12-31 | TRUE             |

SOURCE: stg_amenities_changelog
GRAIN: One row per listing per validity period

DOWNSTREAM DEPENDENCIES:
    - int_calendar_enriched (joins on date BETWEEN valid_from AND valid_to)
    - int_listings_history
    - dim_listings
    - fct_daily_listing_performance
================================================================================
*/

{% set amenity_columns = get_amenity_columns() %}

with changelog as (
    select * from {{ ref('stg_amenities_changelog') }}
),

with_validity as (
    select
        listing_id,
        change_at as valid_from,
        coalesce(
            dateadd(day, -1, lead(change_at) over (
                partition by listing_id 
                order by change_at
            )),
            '9999-12-31'::date
        ) as valid_to,
        {% for amenity in amenity_columns %}
        "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    from changelog
)

select * from with_validity
