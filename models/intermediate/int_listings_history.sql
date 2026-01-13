{{
    config(
        materialized='view',
        schema='development'
    )
}}

/*
================================================================================
FILE: int_listings_history.sql
LAYER: Intermediate
SCHEMA: development
================================================================================

PURPOSE:
    Prepare listing data for SCD Type 2 dimension.
    Combines current listing attributes with historical amenity changes
    to track listing evolution over time.

LOGIC:
    1. Get current listing attributes from stg_listings
    2. Get amenity change dates from int_listing_amenities_scd
    3. Create a timeline of listing states based on amenity changes
    4. Use LEAD() to calculate validity windows
    5. Mark most recent record as is_current = true

NOTE:
    Currently uses amenity changes as the trigger for new SCD records.
    In production, you might also track price changes, room type changes, etc.

SOURCE: stg_listings, int_listing_amenities_scd
GRAIN: One row per listing per validity period

DOWNSTREAM DEPENDENCIES:
    - dim_listings
================================================================================
*/

with current_listings as (
    select
        listing_id,
        listing_name,
        neighborhood,
        property_type,
        room_type,
        accommodates,
        beds,
        host_id,
        base_price,
        number_of_reviews,
        review_scores_rating,
        first_review_date,
        last_review_date,
        -- Use first_review_date as proxy for listing creation
        coalesce(first_review_date, '2020-01-01'::date) as listing_created_date
    from {{ ref('stg_listings') }}
),

-- Get amenity change dates to create SCD records
amenity_change_dates as (
    select distinct
        listing_id,
        valid_from as record_date
    from {{ ref('int_listing_amenities_scd') }}
),

-- Create timeline of changes per listing
listing_timeline as (
    select
        l.listing_id,
        coalesce(a.record_date, l.listing_created_date) as valid_from,
        l.listing_name,
        l.neighborhood,
        l.property_type,
        l.room_type,
        l.accommodates,
        l.beds,
        l.host_id,
        l.base_price,
        l.number_of_reviews,
        l.review_scores_rating
    from current_listings l
    left join amenity_change_dates a on l.listing_id = a.listing_id
),

ranked_timeline as (
    select
        *,
        lead(valid_from) over (partition by listing_id order by valid_from) as next_valid_from
    from listing_timeline
)

select
    listing_id,
    listing_name,
    neighborhood,
    property_type,
    room_type,
    accommodates,
    beds,
    host_id,
    base_price,
    number_of_reviews,
    review_scores_rating,
    valid_from,
    coalesce(
        dateadd(day, -1, next_valid_from),
        '9999-12-31'::date
    ) as valid_to,
    case 
        when next_valid_from is null then true 
        else false 
    end as is_current
from ranked_timeline
