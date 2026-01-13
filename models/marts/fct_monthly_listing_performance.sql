{{
    config(
        materialized='table',
        schema='mart'
    )
}}

/*
================================================================================
FILE: fct_monthly_listing_performance.sql
LAYER: Marts (Fact - Aggregate)
SCHEMA: mart
================================================================================

PURPOSE:
    Pre-aggregated monthly metrics per listing.
    Provides faster query performance for monthly dashboards and reports.
    ~30x smaller than daily fact table.

LOGIC:
    1. Aggregate fct_daily_listing_performance to monthly grain
    2. Calculate monthly KPIs:
       - total_revenue, avg_daily_revenue
       - occupied_nights, available_nights, blocked_nights
       - occupancy_rate_pct
       - RevPAR (Revenue Per Available Room-night)
    3. Calculate month-over-month changes using LAG()
    4. Include listing attributes for filtering

KEY METRICS:
    - occupancy_rate_pct: occupied / (occupied + available) * 100
    - revpar: total_revenue / (occupied + available)
    - revenue_mom_change_pct: Month-over-month revenue change
    - occupancy_mom_change_pts: MoM occupancy point change

SOURCE: fct_daily_listing_performance
GRAIN: One row per listing per month

DOWNSTREAM DEPENDENCIES:
    - Used directly for monthly reporting dashboards
================================================================================
*/

with monthly_metrics as (
    select
        listing_id,
        calendar_month,
        calendar_year,
        
        -- Listing attributes (use mode for most common value in month)
        max(listing_name) as listing_name,
        max(neighborhood) as neighborhood,
        max(property_type) as property_type,
        max(room_type) as room_type,
        max(accommodates) as accommodates,
        max(beds) as beds,
        max(host_id) as host_id,
        
        -- Revenue metrics
        sum(daily_revenue) as total_revenue,
        avg(daily_revenue) as avg_daily_revenue,
        max(daily_revenue) as max_daily_revenue,
        avg(adjusted_price) as avg_nightly_rate,
        avg(base_price) as avg_base_price,
        
        -- Occupancy metrics (cast boolean flags to int for sum)
        sum(is_occupied) as occupied_nights,
        sum(available_flag) as available_nights,
        sum(is_blocked) as blocked_nights,
        count(*) as total_nights,
        
        -- Occupancy rate (occupied / (occupied + available))
        round(
            sum(is_occupied)::float / nullif(sum(is_occupied) + sum(available_flag), 0) * 100,
            2
        ) as occupancy_rate_pct,
        
        -- Review metrics
        avg(review_scores_rating) as avg_review_score,
        count(review_id) as review_count,
        sum(daily_review_score) as total_review_score,
        
        -- Pricing metrics
        min(adjusted_price) as min_nightly_rate,
        max(adjusted_price) as max_nightly_rate,
        stddev(adjusted_price) as price_volatility,
        
        -- RevPAR (Revenue Per Available Room-Night)
        round(
            sum(daily_revenue)::float / nullif(sum(is_occupied) + sum(available_flag), 0),
            2
        ) as revpar
        
    from {{ ref('fct_daily_listing_performance') }}
    group by 
        listing_id,
        calendar_month,
        calendar_year
),

with_mom_changes as (
    select
        m.*,
        
        -- Month-over-month changes
        lag(total_revenue) over (
            partition by listing_id 
            order by calendar_month
        ) as prev_month_revenue,
        
        lag(occupancy_rate_pct) over (
            partition by listing_id 
            order by calendar_month
        ) as prev_month_occupancy_rate,
        
        lag(avg_nightly_rate) over (
            partition by listing_id 
            order by calendar_month
        ) as prev_month_avg_rate
        
    from monthly_metrics m
)

select
    *,
    
    -- MoM revenue change
    round(
        (total_revenue - prev_month_revenue)::float / nullif(prev_month_revenue, 0) * 100,
        2
    ) as revenue_mom_change_pct,
    
    -- MoM occupancy change (absolute points)
    occupancy_rate_pct - prev_month_occupancy_rate as occupancy_mom_change_pts,
    
    -- MoM rate change
    round(
        (avg_nightly_rate - prev_month_avg_rate)::float / nullif(prev_month_avg_rate, 0) * 100,
        2
    ) as rate_mom_change_pct,
    
    -- Audit
    current_timestamp() as dbt_updated_at
    
from with_mom_changes
