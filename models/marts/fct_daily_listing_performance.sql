{{
    config(
        materialized='incremental',
        schema='mart',
        unique_key=['listing_id', 'calendar_date'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

/*
================================================================================
FILE: fct_daily_listing_performance.sql
LAYER: Marts (Fact)
SCHEMA: mart
================================================================================

PURPOSE:
    Primary fact table for rental property analytics.
    Contains daily revenue, occupancy, pricing, and amenity data per listing.
    Supports period-over-period analysis and amenity impact studies.

LOGIC:
    1. Start with stg_calendar for the day × listing grain
    2. Join stg_listings for static listing attributes
    3. Join int_listing_amenities_scd for point-in-time correct amenities
    4. Join stg_reviews for daily review scores
    5. Calculate derived metrics:
       - daily_revenue: Price if reserved, else 0
       - is_occupied: 1 if reserved, else 0
       - price_variance: Difference from base price
    6. Add time dimension columns for aggregation

INCREMENTAL STRATEGY:
    - First run: Full load of all data
    - Subsequent runs: Only process last 7 days
    - Uses MERGE to handle late-arriving data and updates
    
SCHEMA CHANGE HANDLING (New Amenities):
    When a new amenity is added to the source:
    1. get_amenity_columns() macro detects the new amenity at compile time
    2. on_schema_change='sync_all_columns' adds new column to target table
    3. Incremental run populates new column for recent 7 days only
    4. Historical rows will have NULL for new amenity
    
    To backfill historical data: Run `dbt run --full-refresh -s fct_daily_listing_performance`
    
    Recommended: Set up weekly full refresh OR create a separate detection macro
    that triggers full refresh when schema changes are detected.

KEY FEATURES:
    - Incremental materialization for performance
    - Point-in-time correct amenity attribution
    - Pre-calculated revenue and occupancy flags
    - Date key for dim_date joins

SOURCE: stg_calendar, stg_listings, int_listing_amenities_scd, stg_reviews
GRAIN: One row per listing per calendar date

DOWNSTREAM DEPENDENCIES:
    - fct_monthly_listing_performance
    - fct_monthly_neighborhood_summary
    - problem_1_amenity_revenue
    - problem_2_neighborhood_pricing

JOIN RATIONALE:
    - calendar_data → listings_data: INNER JOIN because calendar rows must have
      a valid listing; removing orphans early improves clarity and performance.
    - calendar_data → amenities_scd: LEFT JOIN with explicit >= and <= to
      preserve dates without SCD rows while giving the optimizer clearer
      predicates on the date range.
    - enriched → reviews_data: LEFT JOIN because many dates have no reviews.
================================================================================
*/

{% set amenity_columns = get_amenity_columns() %}

with calendar_data as (
    select
        listing_id,
        calendar_date,
        is_available,
        is_reserved,
        reservation_id,
        daily_price,
        minimum_nights,
        maximum_nights,
        calendar_month,
        calendar_year,
        day_of_week,
        is_weekend
    from {{ ref('stg_calendar') }}
    
    {% if is_incremental() %}
    -- Only process recent data on incremental runs
    where calendar_date >= dateadd(day, -7, (select max(calendar_date) from {{ this }}))
    {% endif %}
),

listings_data as (
    select
        listing_id,
        listing_name,
        neighborhood,
        property_type,
        room_type,
        accommodates,
        bedrooms,
        beds,
        bathrooms,
        host_id,
        host_name,
        base_price,
        number_of_reviews,
        review_scores_rating,
        {% for amenity in amenity_columns %}
        "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('stg_listings') }}
),

amenities_scd as (
    select * from {{ ref('int_listing_amenities_scd') }}
),

reviews_data as (
    select
        listing_id,
        review_date,
        review_score,
        review_id
    from {{ ref('stg_reviews') }}
),

-- Enrich calendar with listing attributes and time-aware amenities
enriched as (
    select
        c.listing_id,
        c.calendar_date,
        c.is_available,
        c.is_reserved,
        c.reservation_id,
        c.daily_price,
        c.minimum_nights,
        c.maximum_nights,
        c.calendar_month,
        c.calendar_year,
        c.day_of_week,
        c.is_weekend,
        
        -- Listing attributes
        l.listing_name,
        l.neighborhood,
        l.property_type,
        l.room_type,
        l.accommodates,
        l.bedrooms,
        l.beds,
        l.bathrooms,
        l.host_id,
        l.host_name,
        l.base_price,
        l.number_of_reviews,
        l.review_scores_rating,
        
        -- Time-aware amenities (prefer SCD value, fallback to listing value)
        {% for amenity in amenity_columns %}
        coalesce(a."{{ amenity }}", l."{{ amenity }}") as "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
        
    -- INNER JOIN for listings: calendar entries must have valid listing
    from calendar_data c
    inner join listings_data l 
        on c.listing_id = l.listing_id
    -- LEFT JOIN for amenities: not all dates may have SCD records
    left join amenities_scd a
        on c.listing_id = a.listing_id
        and c.calendar_date >= a.valid_from 
        and c.calendar_date <= a.valid_to
),

with_reviews as (
    select
        e.*,
        coalesce(r.review_score, 0) as daily_review_score,
        r.review_id
    from enriched e
    left join reviews_data r
        on e.listing_id = r.listing_id 
        and e.calendar_date = r.review_date
),

final as (
    select
        -- Keys
        listing_id,
        calendar_date,
        
        -- Date key for joining to dim_date
        to_char(calendar_date, 'YYYYMMDD')::int as date_key,
        
        -- Availability & Booking
        is_available,
        is_reserved,
        reservation_id,
        minimum_nights,
        maximum_nights,
        
        -- Pricing
        daily_price as adjusted_price,
        base_price,
        coalesce(daily_price, 0) - coalesce(base_price, 0) as price_variance,
        
        -- Listing Attributes
        listing_name,
        neighborhood,
        property_type,
        room_type,
        accommodates,
        bedrooms,
        beds,
        bathrooms,
        host_id,
        host_name,
        
        -- Review Metrics
        number_of_reviews,
        review_scores_rating,
        daily_review_score,
        review_id,
        
        -- Computed Metrics
        case 
            when is_reserved = true then daily_price 
            else 0 
        end as daily_revenue,
        
        case 
            when is_reserved = true then 1 
            else 0 
        end as is_occupied,
        
        case 
            when is_available = true then 1 
            else 0 
        end as available_flag,
        
        case 
            when is_available = false and is_reserved = false then 1 
            else 0 
        end as is_blocked,
        
        -- Time Dimensions
        date_trunc('week', calendar_date)::date as calendar_week,
        calendar_month,
        date_trunc('quarter', calendar_date)::date as calendar_quarter,
        calendar_year,
        day_of_week,
        is_weekend,
        
        -- Amenities (point-in-time correct)
        {% for amenity in amenity_columns %}
        "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %},
        
        -- Audit
        current_timestamp() as dbt_updated_at
        
    from with_reviews
)

select * from final
