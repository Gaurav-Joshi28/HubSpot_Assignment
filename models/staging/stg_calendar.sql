{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: stg_calendar.sql
LAYER: Staging
SCHEMA: stagging
================================================================================

PURPOSE:
    Clean and standardize daily calendar/availability data for each listing.
    This provides the foundation for all revenue and occupancy analysis.

LOGIC:
    1. Convert availability flag from 't'/'f' strings to boolean
    2. Derive reservation status from reservation_id presence
    3. Add time dimension columns (month, year, day_of_week, weekend flag)
    4. Preserve pricing and stay constraint fields

SOURCE: raw.calendar
GRAIN: One row per listing per date (listing_id + calendar_date)

KEY TRANSFORMATIONS:
    - available 't'/'f' → true/false (boolean)
    - Derive is_reserved from reservation_id
    - Add is_weekend flag

DOWNSTREAM DEPENDENCIES:
    - int_availability_spans
    - int_calendar_enriched
    - fct_daily_listing_performance
================================================================================
*/

with source as (
    select * from {{ source('raw', 'calendar') }}
),

cleaned as (
    select
        -- Composite key: listing_id + calendar_date
        listing_id,
        date as calendar_date,
        
        -- Availability flag (convert 't'/'f' to boolean)
        case 
            when lower(available) = 't' then true
            when lower(available) = 'true' then true
            when lower(available) = '1' then true
            else false 
        end as is_available,
        
        -- Reservation tracking
        reservation_id,
        case when reservation_id is not null then true else false end as is_reserved,
        
        -- Pricing for this specific date
        price as daily_price,
        
        -- Stay constraints set by the host
        minimum_nights,
        maximum_nights,
        
        -- Derived date dimensions for easier analysis
        date_trunc('month', date) as calendar_month,
        date_trunc('year', date) as calendar_year,
        dayofweek(date) as day_of_week,
        case when dayofweek(date) in (0, 6) then true else false end as is_weekend

    from source
)

select * from cleaned
