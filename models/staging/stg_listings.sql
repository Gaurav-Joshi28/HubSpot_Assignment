{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: stg_listings.sql
LAYER: Staging
SCHEMA: stagging
================================================================================

PURPOSE:
    Clean and standardize raw listing data from the source system.
    This is the single source of truth for all listing attributes.

LOGIC:
    1. Parse JSON amenities array using LATERAL FLATTEN
    2. Dynamically PIVOT amenities into boolean columns (no hardcoding)
    3. Clean price field (remove $ and commas, convert to numeric)
    4. Parse bathrooms from text field
    5. Standardize date fields (host_since, first_review, last_review)
    6. Join base listing data with pivoted amenities

SOURCE: raw.listings
GRAIN: One row per listing (listing_id)

KEY TRANSFORMATIONS:
    - price "$125.00" → 125.00 (numeric)
    - amenities JSON → individual boolean columns
    - bathrooms_text "1.5 baths" → 1.5 (numeric)

DOWNSTREAM DEPENDENCIES:
    - int_listings_history
    - int_availability_spans
    - int_calendar_enriched
    - dim_listings
================================================================================
*/

{% set amenity_columns = get_listing_amenity_columns() %}

with source as (
    select * from {{ source('raw', 'listings') }}
),

flattened_amenities as (
    select 
        id as listing_id,
        trim(f.value::string) as amenity_name,
        1 as present
    from source,
    lateral flatten(input => parse_json(amenities)) f 
),

amenities_pivoted as (
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
),

-- Rename pivot columns to clean names
amenities_cleaned as (
    select
        listing_id,
        {% for amenity in amenity_columns %}
        coalesce("'{{ amenity }}'"::boolean, false) as "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    from amenities_pivoted
),

listings_base as (
    select
        -- Primary key
        id as listing_id,
        
        -- Listing attributes
        name as listing_name,
        neighborhood,
        property_type,
        room_type,
        accommodates,
        bedrooms,
        beds,
        
        -- Parse bathrooms from text
        try_to_number(
            regexp_substr(bathrooms_text, '[0-9]+\\.?[0-9]*'),
            10, 1
        ) as bathrooms,
        bathrooms_text as bathrooms_raw,
        
        -- Host information
        host_id,
        host_name,
        host_location,
        try_to_date(host_since::varchar) as host_since_date,
        host_verifications,
        
        -- Pricing (clean $X,XXX.XX format to numeric)
        try_to_number(
            replace(replace(price, '$', ''), ',', ''),
            10, 2
        ) as base_price,
        
        -- Review metrics
        number_of_reviews,
        try_to_date(first_review) as first_review_date,
        try_to_date(last_review) as last_review_date,
        review_scores_rating,
        
        -- Raw amenities
        amenities as amenities_raw

    from source
)

select
    l.*,
    {% for amenity in amenity_columns %}
    a."{{ amenity }}"{% if not loop.last %},{% endif %}
    {% endfor %}
from listings_base l
left join amenities_cleaned a on l.listing_id = a.listing_id
