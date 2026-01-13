{{
    config(
        materialized='view'
    )
}}

/*
================================================================================
FILE: stg_reviews.sql
LAYER: Staging
SCHEMA: stagging
================================================================================

PURPOSE:
    Clean and standardize review data for listings.
    Provides review scores and dates for performance analysis.

LOGIC:
    1. Rename columns to standard naming convention
    2. Convert review_date to proper date type
    3. Preserve review_score for aggregation

SOURCE: raw.generated_reviews
GRAIN: One row per review (review_id)

NOTE: Source data contains 2 null review IDs (known data quality issue)

DOWNSTREAM DEPENDENCIES:
    - fct_daily_listing_performance (joins on listing_id + review_date)
================================================================================
*/

with source as (
    select * from {{ source('raw', 'generated_reviews') }}
),

cleaned as (
    select
        id as review_id,
        listing_id,
        review_date::date as review_date,
        review_score
    from source
)

select * from cleaned
