{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: int_availability_spans.sql
LAYER: Intermediate
SCHEMA: development
================================================================================

PURPOSE:
    Identify consecutive spans of available days for each listing.
    Used to answer "What is the maximum duration one could stay?"

LOGIC:
    1. Filter calendar to only available days (is_available = true)
    2. Use gap-and-islands technique to identify consecutive date groups:
       - Subtract row_number from date to create a group identifier
       - Consecutive dates will have the same group value
    3. Aggregate each group to get span start, end, and duration
    4. Apply BOTH minimum_nights and maximum_nights constraints:
       - If span duration < minimum_nights: booking not possible (effective = 0)
       - If span duration > maximum_nights: cap at maximum_nights
    5. Join listing attributes and amenities for filtering

JOIN RATIONALE:
    - availability_spans → listings uses INNER JOIN because spans come from the
      calendar and must have a valid listing; orphan calendar rows are treated
      as data quality issues and are excluded early for clarity and performance.

CONSTRAINTS:
    - minimum_nights: Host requires at least N nights booking
    - maximum_nights: Host allows at most N nights booking
    - If consecutive_available_days < minimum_nights, guest CANNOT book

EXAMPLE:
    Available: 2022-01-01, 2022-01-02, 2022-01-03, 2022-01-05
    Groups:    A          A           A           B
    Spans:     Jan 1-3 (3 days), Jan 5 (1 day)
    
    If minimum_nights = 2:
      - Jan 1-3: effective_max = 3 (meets minimum)
      - Jan 5: effective_max = 0 (below minimum, can't book)

SOURCE: stg_calendar, stg_listings
GRAIN: One row per availability span per listing

DOWNSTREAM DEPENDENCIES:
    - problem_3a_max_stay_duration
    - problem_3b_max_stay_lockbox_firstaid
================================================================================
*/

{% set amenity_columns = get_listing_amenity_columns() %}

with calendar as (
    select * from {{ ref('stg_calendar') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

-- Only look at available days
available_days as (
    select
        listing_id,
        calendar_date,
        minimum_nights,
        maximum_nights,
        -- Create a group identifier for consecutive days
        -- Subtracting row_number from date: consecutive dates get same value
        dateadd(day, -row_number() over (
            partition by listing_id 
            order by calendar_date
        ), calendar_date) as span_group
    from calendar
    where is_available = true
),

-- Group consecutive days into spans
availability_spans as (
    select
        listing_id,
        span_group,
        min(calendar_date) as span_start_date,
        max(calendar_date) as span_end_date,
        count(*) as consecutive_available_days,
        -- Use the minimum of minimum_nights across the span (most restrictive)
        max(minimum_nights) as min_nights_required,
        -- Use the minimum of maximum_nights across the span (most restrictive)
        min(nullif(maximum_nights, 0)) as max_nights_allowed
    from available_days
    group by listing_id, span_group
),

-- Calculate the effective maximum stay duration
spans_with_max_stay as (
    select
        s.listing_id,
        s.span_start_date,
        s.span_end_date,
        s.consecutive_available_days,
        s.min_nights_required,
        s.max_nights_allowed,
        
        -- Effective max stay considers BOTH minimum and maximum constraints
        -- If span is below minimum nights required, guest CANNOT book at all
        case 
            when s.min_nights_required is not null 
                 and s.consecutive_available_days < s.min_nights_required
            then 0  -- Cannot book: below minimum nights required
            when s.max_nights_allowed is not null 
                 and s.max_nights_allowed > 0 
            then least(s.consecutive_available_days, s.max_nights_allowed)
            else s.consecutive_available_days
        end as effective_max_stay_nights,
        
        -- Flag if span is bookable (meets minimum requirement)
        case 
            when s.min_nights_required is null 
                 or s.consecutive_available_days >= s.min_nights_required
            then true
            else false
        end as is_bookable,
        
        l.listing_name,
        l.neighborhood,
        l.property_type,
        l.room_type,
        l.accommodates,
        
        -- Include all dynamic amenity columns for filtering
        {% for amenity in amenity_columns %}
        l."{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    -- INNER JOIN: Only include spans for valid listings
    -- (calendar entries without matching listings are data quality issues)
    from availability_spans s
    inner join listings l on s.listing_id = l.listing_id
)

select * from spans_with_max_stay
