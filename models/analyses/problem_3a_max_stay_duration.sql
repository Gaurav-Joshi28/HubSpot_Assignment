{{
    config(
        materialized='view',
        schema='analytics'
    )
}}

/*
================================================================================
FILE: problem_3a_max_stay_duration.sql
LAYER: Analytics
SCHEMA: analytics
================================================================================

PURPOSE:
    Answer Business Problem #3A - Maximum Stay Duration (All Listings)
    
    Find the maximum duration one could stay in each listing, 
    based on the availability and what the owner allows.

BUSINESS QUESTION:
    "What is the longest consecutive period a guest could book each listing?"
    
EXAMPLE OUTPUT:
    "Listing 863788 is heavily booked. The largest timespan for which it is 
    available is four days from September 18th to 21st in 2021. 
    The correct solution should show that three listings are tied for the 
    longest possible stay."

LOGIC:
    1. Source availability spans from int_availability_spans
       (which uses gap-and-islands technique to find consecutive available days)
    2. Group by listing to find maximum span per listing
    3. Capture the best span's start/end dates
    4. Join dim_listings for listing attributes
    5. Add rankings (overall and within neighborhood)
    6. Flag ties for longest stay

KEY METRICS:
    - max_possible_stay_nights: Longest consecutive available period
    - best_span_start/end: When this period occurs
    - overall_stay_rank: Ranking across all listings
    - longest_stay_flag: Indicates ties for #1

SOURCE: int_availability_spans, dim_listings, dim_date
GRAIN: One row per listing
================================================================================
*/

with listing_max_stays as (
    select
        ias.listing_id,
        ias.listing_name,
        ias.neighborhood,
        max(ias.effective_max_stay_nights) as max_possible_stay_nights,
        -- Get the span details for the maximum stay
        max_by(ias.span_start_date, ias.effective_max_stay_nights) as best_span_start,
        max_by(ias.span_end_date, ias.effective_max_stay_nights) as best_span_end,
        count(*) as total_availability_spans,
        avg(ias.effective_max_stay_nights) as avg_span_duration
    from {{ ref('int_availability_spans') }} ias
    group by ias.listing_id, ias.listing_name, ias.neighborhood
),

enriched as (
    select
        lms.*,
        
        -- Listing attributes from current dimension
        dl.property_type,
        dl.room_type,
        dl.accommodates,
        dl.capacity_tier,
        dl.price_tier,
        dl.rating_tier,
        dl.base_price,
        dl.review_scores_rating,
        
        -- Best span season
        dd.season as best_span_season,
        dd.is_peak_season as best_span_in_peak_season,
        
        -- Ranking
        dense_rank() over (order by lms.max_possible_stay_nights desc) as overall_stay_rank,
        dense_rank() over (
            partition by dl.neighborhood 
            order by lms.max_possible_stay_nights desc
        ) as neighborhood_stay_rank
        
    from listing_max_stays lms
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
    rating_tier,
    base_price,
    review_scores_rating,
    
    max_possible_stay_nights,
    best_span_start,
    best_span_end,
    best_span_season,
    best_span_in_peak_season,
    
    total_availability_spans,
    round(avg_span_duration, 1) as avg_span_duration,
    
    overall_stay_rank,
    neighborhood_stay_rank,
    
    -- Flag ties for longest stay
    case 
        when overall_stay_rank = 1 then 'Tied for Longest'
        else null
    end as longest_stay_flag
    
from enriched
order by max_possible_stay_nights desc, listing_id
