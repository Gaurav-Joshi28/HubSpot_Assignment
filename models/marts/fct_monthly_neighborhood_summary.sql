{{
    config(
        materialized='table',
        schema='mart'
    )
}}

/*
================================================================================
FILE: fct_monthly_neighborhood_summary.sql
LAYER: Marts (Fact - Aggregate)
SCHEMA: mart
================================================================================

PURPOSE:
    Pre-aggregated monthly metrics by neighborhood.
    Provides executive-level market analysis and competitive positioning.

LOGIC:
    1. Aggregate fct_daily_listing_performance to neighborhood × month grain
    2. Calculate market-level KPIs:
       - Active listings count
       - Total and average revenue
       - Occupancy rate across neighborhood
       - Room type distribution
    3. Calculate rankings within each month
    4. Calculate market share metrics
    5. Add month-over-month trend indicators

KEY METRICS:
    - revenue_rank: Neighborhood ranking by revenue within month
    - revenue_market_share_pct: % of total market revenue
    - neighborhood_revpar: RevPAR at neighborhood level
    - revenue_mom_change_pct: Month-over-month revenue trend

USE CASES:
    - Which neighborhoods are trending up/down?
    - Market share analysis
    - Investment opportunity identification
    - Competitive pricing analysis

SOURCE: fct_daily_listing_performance
GRAIN: One row per neighborhood per month

DOWNSTREAM DEPENDENCIES:
    - Used directly for executive dashboards
================================================================================
*/

with neighborhood_monthly as (
    select
        neighborhood,
        calendar_month,
        calendar_year,
        
        -- Listing counts
        count(distinct listing_id) as active_listings,
        
        -- Revenue metrics
        sum(daily_revenue) as total_revenue,
        avg(daily_revenue) as avg_daily_revenue_per_listing,
        
        -- Occupancy metrics (is_occupied and available_flag are already integers)
        sum(is_occupied) as total_occupied_nights,
        sum(available_flag) as total_available_nights,
        sum(is_blocked) as total_blocked_nights,
        count(*) as total_listing_nights,
        
        -- Occupancy rate
        round(
            sum(is_occupied)::float / nullif(sum(is_occupied) + sum(available_flag), 0) * 100,
            2
        ) as avg_occupancy_rate_pct,
        
        -- Pricing metrics
        avg(adjusted_price) as avg_nightly_rate,
        percentile_cont(0.5) within group (order by adjusted_price) as median_nightly_rate,
        min(adjusted_price) as min_nightly_rate,
        max(adjusted_price) as max_nightly_rate,
        
        -- Room type distribution
        count(distinct case when room_type = 'Entire home/apt' then listing_id end) as entire_home_listings,
        count(distinct case when room_type = 'Private room' then listing_id end) as private_room_listings,
        count(distinct case when room_type = 'Shared room' then listing_id end) as shared_room_listings,
        
        -- Review metrics
        avg(review_scores_rating) as avg_review_score,
        count(review_id) as total_reviews,
        
        -- RevPAR
        round(
            sum(daily_revenue)::float / nullif(sum(is_occupied) + sum(available_flag), 0),
            2
        ) as neighborhood_revpar
        
    from {{ ref('fct_daily_listing_performance') }}
    where neighborhood is not null
    group by 
        neighborhood,
        calendar_month,
        calendar_year
),

with_rankings as (
    select
        n.*,
        
        -- Rankings within month
        rank() over (
            partition by calendar_month 
            order by total_revenue desc
        ) as revenue_rank,
        
        rank() over (
            partition by calendar_month 
            order by avg_occupancy_rate_pct desc
        ) as occupancy_rank,
        
        rank() over (
            partition by calendar_month 
            order by avg_nightly_rate desc
        ) as price_rank,
        
        rank() over (
            partition by calendar_month 
            order by avg_review_score desc nulls last
        ) as review_rank,
        
        -- Market share
        round(
            total_revenue::float / sum(total_revenue) over (partition by calendar_month) * 100,
            2
        ) as revenue_market_share_pct,
        
        round(
            active_listings::float / sum(active_listings) over (partition by calendar_month) * 100,
            2
        ) as listing_market_share_pct,
        
        -- MoM changes
        lag(total_revenue) over (
            partition by neighborhood 
            order by calendar_month
        ) as prev_month_revenue,
        
        lag(avg_occupancy_rate_pct) over (
            partition by neighborhood 
            order by calendar_month
        ) as prev_month_occupancy
        
    from neighborhood_monthly n
)

select
    *,
    
    -- MoM revenue change
    round(
        (total_revenue - prev_month_revenue)::float / nullif(prev_month_revenue, 0) * 100,
        2
    ) as revenue_mom_change_pct,
    
    -- MoM occupancy change
    avg_occupancy_rate_pct - prev_month_occupancy as occupancy_mom_change_pts,
    
    -- Audit
    current_timestamp() as dbt_updated_at
    
from with_rankings
