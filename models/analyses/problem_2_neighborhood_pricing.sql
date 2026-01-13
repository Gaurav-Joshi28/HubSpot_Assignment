{{
    config(
        materialized='view',
        schema='analytics'
    )
}}

/*
================================================================================
FILE: problem_2_neighborhood_pricing.sql
LAYER: Analytics
SCHEMA: analytics
================================================================================

PURPOSE:
    Answer Business Problem #2 - Neighborhood Pricing Analysis
    
    Find the average price increase for each neighborhood from 
    July 12th 2021 to July 11th 2022.

BUSINESS QUESTION:
    "What is the average price change per neighborhood over a 1-year period?"
    
EXAMPLE OUTPUT:
    "The Back Bay neighborhood only has one listing (10813), so the 
    difference of $44 is the average for the whole neighborhood."

LOGIC:
    1. Get prices for each listing around July 12th, 2021 (use 7-day window for robustness)
    2. Get prices for same listings around July 11th, 2022 (use 7-day window)
    3. Use LEFT JOIN to capture all listings (show data availability)
    4. Calculate price difference per listing (handle nulls explicitly)
    5. Average the difference by neighborhood
    6. Include listing IDs for reference and data quality flags
    7. Rank neighborhoods by average price increase

KEY METRICS:
    - avg_price_increase: Average $ increase across listings
    - avg_pct_increase: Average % increase
    - listing_ids: Which listings are included
    - data_completeness: % of listings with data on both dates

SOURCE: fct_daily_listing_performance
GRAIN: One row per neighborhood
================================================================================
*/

with prices_july_2021 as (
    -- Get prices around July 12th, 2021 (7-day window for robustness)
    -- If listing wasn't available on exact date, use closest available price
    select
        listing_id,
        listing_name,
        neighborhood,
        avg(adjusted_price) as price_july_2021,
        count(*) as days_with_data_2021
    from {{ ref('fct_daily_listing_performance') }}
    where calendar_date between '2021-07-09' and '2021-07-15'
      and adjusted_price is not null
      and adjusted_price > 0
    group by listing_id, listing_name, neighborhood
),

prices_july_2022 as (
    -- Get prices around July 11th, 2022 (7-day window)
    select
        listing_id,
        avg(adjusted_price) as price_july_2022,
        count(*) as days_with_data_2022
    from {{ ref('fct_daily_listing_performance') }}
    where calendar_date between '2022-07-08' and '2022-07-14'
      and adjusted_price is not null
      and adjusted_price > 0
    group by listing_id
),

-- Get all listings to understand data completeness
all_listings as (
    select distinct 
        listing_id,
        neighborhood
    from {{ ref('fct_daily_listing_performance') }}
    where neighborhood is not null
),

price_changes as (
    -- LEFT JOIN to capture all listings, show data availability
    select
        al.listing_id,
        al.neighborhood,
        p1.listing_name,
        p1.price_july_2021,
        p2.price_july_2022,
        -- Handle nulls explicitly
        case 
            when p1.price_july_2021 is not null and p2.price_july_2022 is not null
            then p2.price_july_2022 - p1.price_july_2021
            else null
        end as price_difference,
        -- Data availability flags
        case when p1.price_july_2021 is not null then 1 else 0 end as has_2021_data,
        case when p2.price_july_2022 is not null then 1 else 0 end as has_2022_data,
        case 
            when p1.price_july_2021 is not null and p2.price_july_2022 is not null 
            then 1 else 0 
        end as has_both_dates
    from all_listings al
    left join prices_july_2021 p1 on al.listing_id = p1.listing_id
    left join prices_july_2022 p2 on al.listing_id = p2.listing_id
),

neighborhood_averages as (
    -- Average price increase by neighborhood (only for listings with BOTH dates)
    select
        neighborhood,
        -- Counts
        count(listing_id) as total_listings,
        sum(has_both_dates) as listings_with_both_dates,
        sum(has_2021_data) as listings_with_2021_data,
        sum(has_2022_data) as listings_with_2022_data,
        
        -- Data completeness percentage
        round(sum(has_both_dates) * 100.0 / nullif(count(listing_id), 0), 1) as data_completeness_pct,
        
        -- Price metrics (only for listings with both dates)
        round(avg(case when has_both_dates = 1 then price_july_2021 end), 2) as avg_price_july_2021,
        round(avg(case when has_both_dates = 1 then price_july_2022 end), 2) as avg_price_july_2022,
        round(avg(price_difference), 2) as avg_price_increase,
        round(min(price_difference), 2) as min_price_change,
        round(max(price_difference), 2) as max_price_change,
        round(stddev(price_difference), 2) as stddev_price_change,
        
        -- Percentage increase
        round(
            avg(price_difference) / nullif(avg(case when has_both_dates = 1 then price_july_2021 end), 0) * 100, 
            2
        ) as avg_pct_increase,
        
        -- Aggregate listing IDs with complete data
        listagg(
            case when has_both_dates = 1 then listing_id::varchar end, 
            ', '
        ) within group (order by listing_id) as listing_ids_with_both_dates,
        
        -- Aggregate listings missing 2022 data
        listagg(
            case when has_2021_data = 1 and has_2022_data = 0 then listing_id::varchar end,
            ', '
        ) within group (order by listing_id) as listings_missing_2022_data
        
    from price_changes
    group by neighborhood
)

select
    -- Neighborhood identification
    neighborhood as "Neighborhood",
    
    -- Listing counts and data quality
    total_listings as "Total Listings",
    listings_with_both_dates as "Listings with Both Dates",
    data_completeness_pct as "Data Completeness (%)",
    
    -- Listing IDs for reference (per assessment example)
    listing_ids_with_both_dates as "Listing IDs",
    
    -- Price comparison
    avg_price_july_2021 as "Avg Price (July 2021)",
    avg_price_july_2022 as "Avg Price (July 2022)",
    
    -- Price change metrics
    avg_price_increase as "Avg Price Increase ($)",
    avg_pct_increase as "Avg Price Increase (%)",
    min_price_change as "Min Price Change ($)",
    max_price_change as "Max Price Change ($)",
    stddev_price_change as "StdDev Price Change",
    
    -- Data quality notes
    listings_missing_2022_data as "Listings Missing 2022 Data",
    
    -- Ranking (excluding neighborhoods with no complete data)
    case 
        when listings_with_both_dates > 0 
        then rank() over (
            partition by case when listings_with_both_dates > 0 then 1 else 0 end
            order by avg_price_increase desc nulls last
        )
        else null
    end as "Rank by Increase"
    
from neighborhood_averages
where listings_with_both_dates > 0  -- Only show neighborhoods with comparable data
order by avg_price_increase desc nulls last
