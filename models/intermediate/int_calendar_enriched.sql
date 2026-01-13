{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: int_calendar_enriched.sql
LAYER: Intermediate
SCHEMA: development
================================================================================

PURPOSE:
    Enrich daily calendar data with listing attributes and point-in-time 
    correct amenities. This is the main building block for the fact table.

LOGIC:
    1. Start with stg_calendar (day × listing grain)
    2. Join stg_listings for static listing attributes
    3. Join int_listing_amenities_scd for time-aware amenities
       - Uses: calendar_date BETWEEN valid_from AND valid_to
       - Ensures amenities reflect what the listing had ON THAT DATE
    4. Calculate daily_revenue (price if reserved, else 0)
    5. Create occupancy flag for aggregation

KEY FEATURE: Point-in-time correct amenity attribution
    - If a listing added AC on March 15, 2022:
      - Revenue on March 14 → counted as "without AC"
      - Revenue on March 16 → counted as "with AC"

SOURCE: stg_calendar, stg_listings, int_listing_amenities_scd
GRAIN: One row per listing per date

DOWNSTREAM DEPENDENCIES:
    - fct_daily_listing_performance

JOIN RATIONALE:
    - calendar → listings: INNER JOIN because every calendar row must have a
      valid listing; orphan calendar entries are treated as data quality issues
      and excluded early.
    - calendar → amenities_scd: LEFT JOIN because not every date will have an
      SCD record; uses >= and <= for clearer optimizer hints on date ranges.
================================================================================
*/

{% set amenity_columns = get_amenity_columns() %}

with calendar as (
    select * from {{ ref('stg_calendar') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

amenities_scd as (
    select * from {{ ref('int_listing_amenities_scd') }}
),

enriched as (
    select
        -- Calendar grain
        c.listing_id,
        c.calendar_date,
        
        -- Date dimensions
        c.calendar_month,
        c.calendar_year,
        c.day_of_week,
        c.is_weekend,
        
        -- Availability and reservation status
        c.is_available,
        c.is_reserved,
        c.reservation_id,
        
        -- Pricing
        c.daily_price,
        
        -- Stay constraints
        c.minimum_nights,
        c.maximum_nights,
        
        -- Listing attributes
        l.listing_name,
        l.neighborhood,
        l.property_type,
        l.room_type,
        l.accommodates,
        l.bedrooms,
        l.beds,
        l.bathrooms,
        l.base_price,
        l.host_id,
        l.host_name,
        l.review_scores_rating,
        l.number_of_reviews,
        
        -- Time-aware amenities (from SCD model)
        -- These reflect the amenities the listing had ON THIS SPECIFIC DATE
        {% for amenity in amenity_columns %}
        coalesce(a."{{ amenity }}", l."{{ amenity }}") as "{{ amenity }}",
        {% endfor %}
        
        -- Revenue calculation
        case 
            when c.is_reserved then c.daily_price 
            else 0 
        end as daily_revenue,
        
        -- Occupancy flags for aggregation
        case when c.is_reserved then 1 else 0 end as is_occupied

    -- INNER JOIN for listings: calendar entries must have valid listing data
    from calendar c
    inner join listings l on c.listing_id = l.listing_id
    -- LEFT JOIN for amenities: not all dates have SCD records; uses >= and <= for optimizer
    left join amenities_scd a 
        on c.listing_id = a.listing_id 
        and c.calendar_date >= a.valid_from 
        and c.calendar_date <= a.valid_to
)

select * from enriched
