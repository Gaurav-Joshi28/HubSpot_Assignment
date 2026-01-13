{{
    config(
        materialized='table',
        schema='mart'
    )
}}

/*
================================================================================
FILE: dim_listings.sql
LAYER: Marts (Dimension)
SCHEMA: mart
================================================================================

PURPOSE:
    Listing dimension with SCD Type 2 history tracking.
    Includes listing attributes, derived tiers, and point-in-time amenities.

LOGIC:
    1. Source listing history from int_listings_history
    2. Join amenities from int_listing_amenities_scd for each validity period
    3. Generate surrogate key from listing_id + valid_from
    4. Calculate derived tier attributes:
       - capacity_tier: Small/Medium/Large/Extra Large
       - price_tier: Budget/Economy/Mid-Range/Premium/Luxury
       - rating_tier: Based on review scores
    5. Include all dynamic amenity columns

KEY FEATURES:
    - Surrogate key (listing_sk) for dimension joins
    - Natural key (listing_id) for business identification
    - Tier classifications for segmentation
    - Point-in-time correct amenities
    - SCD Type 2 full history

SOURCE: int_listings_history, int_listing_amenities_scd
GRAIN: One row per listing per validity period

DOWNSTREAM DEPENDENCIES:
    - problem_3a_max_stay_duration
    - problem_3b_max_stay_lockbox_firstaid
================================================================================
*/

with listings_with_amenities as (
    select
        lh.listing_id,
        lh.listing_name,
        lh.neighborhood,
        lh.property_type,
        lh.room_type,
        lh.accommodates,
        lh.beds,
        lh.host_id,
        lh.base_price,
        lh.number_of_reviews,
        lh.review_scores_rating,
        lh.valid_from,
        lh.valid_to,
        lh.is_current,
        
        -- Join amenities for this time period
        {% for amenity in get_amenity_columns() %}
        coalesce(a."{{ amenity }}", false) as "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
        
    from {{ ref('int_listings_history') }} lh
    left join {{ ref('int_listing_amenities_scd') }} a
        on lh.listing_id = a.listing_id
        and lh.valid_from between a.valid_from and a.valid_to
),

listing_scd as (
    select
        -- Surrogate key for SCD
        {{ generate_surrogate_key(['listing_id', 'valid_from']) }} as listing_sk,
        
        -- Natural key
        listing_id,
        
        -- Attributes
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
        
        -- Derived attributes: Capacity tier
        case
            when accommodates <= 2 then 'Small (1-2)'
            when accommodates <= 4 then 'Medium (3-4)'
            when accommodates <= 6 then 'Large (5-6)'
            else 'Extra Large (7+)'
        end as capacity_tier,
        
        -- Derived attributes: Price tier
        case
            when base_price < 50 then 'Budget'
            when base_price < 100 then 'Economy'
            when base_price < 200 then 'Mid-Range'
            when base_price < 500 then 'Premium'
            else 'Luxury'
        end as price_tier,
        
        -- Derived attributes: Rating tier
        case
            when review_scores_rating >= 4.8 then 'Exceptional'
            when review_scores_rating >= 4.5 then 'Excellent'
            when review_scores_rating >= 4.0 then 'Good'
            when review_scores_rating >= 3.0 then 'Average'
            else 'Below Average'
        end as rating_tier,
        
        -- Amenity columns
        {% for amenity in get_amenity_columns() %}
        "{{ amenity }}"{% if not loop.last %},{% endif %}
        {% endfor %},
        
        -- SCD metadata
        valid_from,
        valid_to,
        is_current,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
        
    from listings_with_amenities
)

select * from listing_scd
