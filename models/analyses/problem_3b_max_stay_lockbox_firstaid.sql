{{
    config(
        materialized='view',
        schema='analytics'
    )
}}

/*
================================================================================
FILE: problem_3b_max_stay_lockbox_firstaid.sql
LAYER: Analytics
SCHEMA: analytics
================================================================================

PURPOSE:
    Answer Business Problem #3B - Maximum Stay with Specific Amenities
    
    Find the maximum duration one could stay in listings that have 
    BOTH a lockbox AND a first aid kit listed in the amenities.

BUSINESS QUESTION:
    "What is the longest stay for listings with BOTH Lockbox AND First Aid Kit?"
    
EXAMPLE OUTPUT:
    "Listing 10986 has a lockbox [but no first aid kit, so excluded].
    The correct result should show that across the results, the longest 
    possible stay is much shorter than the answer to #3A."

LOGIC:
    1. First validate which amenity column names exist in the data
    2. Filter int_availability_spans for listings with BOTH amenities
       - Check for variations: "Lockbox", "Lock box", "Self check-in"
       - Check for variations: "First aid kit", "First Aid Kit"
    3. Apply minimum_nights constraint (can't book if below minimum)
    4. Find maximum span per qualifying listing
    5. Compare to overall maximum (from all listings)
    6. Calculate how much shorter this is than the unrestricted max

KEY INSIGHT:
    - Listing 10986 has Lockbox but NOT First Aid Kit → excluded
    - Max stay with both amenities << overall max (from Problem 3A)

AMENITY VALIDATION:
    This query dynamically checks for amenity column existence before filtering.
    If column doesn't exist, it will be handled gracefully.

SOURCE: int_availability_spans, dim_listings, dim_date
GRAIN: One row per qualifying listing
================================================================================
*/

{% set amenity_columns = get_listing_amenity_columns() %}

{# Check if specific amenities exist in the data #}
{% set has_lockbox = 'Lockbox' in amenity_columns %}
{% set has_first_aid_kit = 'First aid kit' in amenity_columns %}
{% set has_self_checkin = 'Self check-in' in amenity_columns %}
{% set has_first_aid_kit_caps = 'First Aid Kit' in amenity_columns %}

with filtered_spans as (
    -- Only include listings with BOTH Lockbox AND First Aid Kit
    select *
    from {{ ref('int_availability_spans') }}
    where 1=1
        -- Lockbox check (handle variations)
        {% if has_lockbox %}
        and "Lockbox" = true
        {% elif has_self_checkin %}
        and "Self check-in" = true
        {% else %}
        and false  -- No lockbox-type amenity found, return empty result
        {% endif %}
        -- First Aid Kit check (handle variations)
        {% if has_first_aid_kit %}
        and "First aid kit" = true
        {% elif has_first_aid_kit_caps %}
        and "First Aid Kit" = true
        {% else %}
        and false  -- No first aid kit amenity found, return empty result
        {% endif %}
),

listing_max_stays as (
    select
        fs.listing_id,
        fs.listing_name,
        fs.neighborhood,
        {% if has_lockbox %}
        fs."Lockbox" as has_lockbox,
        {% elif has_self_checkin %}
        fs."Self check-in" as has_lockbox,
        {% else %}
        false as has_lockbox,
        {% endif %}
        {% if has_first_aid_kit %}
        fs."First aid kit" as has_first_aid_kit,
        {% elif has_first_aid_kit_caps %}
        fs."First Aid Kit" as has_first_aid_kit,
        {% else %}
        false as has_first_aid_kit,
        {% endif %}
        max(fs.effective_max_stay_nights) as max_possible_stay_nights,
        max_by(fs.span_start_date, fs.effective_max_stay_nights) as best_span_start,
        max_by(fs.span_end_date, fs.effective_max_stay_nights) as best_span_end,
        count(*) as total_availability_spans,
        round(avg(fs.effective_max_stay_nights), 1) as avg_span_duration
    from filtered_spans fs
    group by 
        fs.listing_id, 
        fs.listing_name, 
        fs.neighborhood
        {% if has_lockbox %}
        , fs."Lockbox"
        {% elif has_self_checkin %}
        , fs."Self check-in"
        {% endif %}
        {% if has_first_aid_kit %}
        , fs."First aid kit"
        {% elif has_first_aid_kit_caps %}
        , fs."First Aid Kit"
        {% endif %}
),

-- Get the overall max stay for comparison (from Problem 3A)
overall_max as (
    select max(effective_max_stay_nights) as global_max_stay
    from {{ ref('int_availability_spans') }}
),

enriched as (
    select
        lms.*,
        
        -- Listing attributes
        dl.property_type,
        dl.room_type,
        dl.accommodates,
        dl.capacity_tier,
        dl.price_tier,
        dl.base_price,
        dl.review_scores_rating,
        
        -- Best span details
        dd.season as best_span_season,
        dd.month_name as best_span_month,
        
        -- Rankings
        dense_rank() over (order by lms.max_possible_stay_nights desc) as stay_rank,
        
        -- Comparison to global max
        om.global_max_stay,
        lms.max_possible_stay_nights - om.global_max_stay as days_shorter_than_max
        
    from listing_max_stays lms
    cross join overall_max om
    left join {{ ref('dim_listings') }} dl 
        on lms.listing_id = dl.listing_id 
        and dl.is_current = true
    left join {{ ref('dim_date') }} dd 
        on lms.best_span_start = dd.date_day
)

select
    listing_id,
    listing_name,
    neighborhood,
    property_type,
    room_type,
    accommodates,
    capacity_tier,
    price_tier,
    base_price,
    review_scores_rating,
    
    -- Required amenities (verified present)
    has_lockbox,
    has_first_aid_kit,
    
    max_possible_stay_nights,
    best_span_start,
    best_span_end,
    best_span_season,
    best_span_month,
    
    total_availability_spans,
    avg_span_duration,
    
    stay_rank,
    global_max_stay,
    days_shorter_than_max,
    
    -- Summary insight
    'Max stay with Lockbox+First Aid Kit is ' || 
    max_possible_stay_nights || ' nights, which is ' ||
    abs(days_shorter_than_max) || ' nights shorter than the overall maximum of ' ||
    global_max_stay || ' nights' as insight_summary,
    
    -- Amenity validation info
    '{{ "Lockbox" if has_lockbox else ("Self check-in" if has_self_checkin else "NOT FOUND") }}' as lockbox_column_used,
    '{{ "First aid kit" if has_first_aid_kit else ("First Aid Kit" if has_first_aid_kit_caps else "NOT FOUND") }}' as first_aid_kit_column_used
    
from enriched
order by max_possible_stay_nights desc, listing_id
