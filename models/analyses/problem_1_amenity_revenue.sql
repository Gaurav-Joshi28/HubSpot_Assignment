{{
    config(
        materialized='view',
        schema='analytics'
    )
}}

/*
================================================================================
FILE: problem_1_amenity_revenue.sql
LAYER: Analytics
SCHEMA: analytics
================================================================================

PURPOSE:
    Answer Business Problem #1 - Amenity Revenue Analysis
    
    Find the total revenue and percentage of revenue by month 
    segmented by whether or not air conditioning exists on the listing.

BUSINESS QUESTION:
    "What percentage of monthly revenue comes from listings with/without AC?"
    
EXAMPLE OUTPUT:
    "21.2% of revenue in July 2022 came from listings without air conditioning"

LOGIC:
    1. Dynamically detect the correct AC amenity column name
       (handles variations: "Air conditioning", "AC", "A/C", "Air Conditioning")
    2. Group fct_daily_listing_performance by month and AC status
    3. Sum revenue for each segment
    4. Calculate monthly totals
    5. Compute percentage of monthly revenue per segment
    6. Add year-over-year comparison using LAG()
    7. Join dim_date for time intelligence (season, fiscal year)

KEY METRICS:
    - pct_of_monthly_revenue: Segment's share of monthly revenue
    - avg_revenue_per_listing: Revenue efficiency by segment
    - yoy_revenue_change_pct: Year-over-year trend

AMENITY VALIDATION:
    This query dynamically checks for AC column existence before using it.
    If column doesn't exist, uses a default approach.

SOURCE: fct_daily_listing_performance, dim_date
GRAIN: One row per month per AC status (with/without)

JOIN RATIONALE:
    - fact → dim_date uses INNER JOIN because dim_date is complete for all
      calendar_date values; this enforces time dimensions and avoids null dates.
================================================================================
*/

{% set amenity_columns = get_amenity_columns() %}

{# Check for different variations of AC amenity #}
{% set has_air_conditioning = 'Air conditioning' in amenity_columns %}
{% set has_ac = 'AC' in amenity_columns %}
{% set has_ac_slash = 'A/C' in amenity_columns %}
{% set has_air_conditioning_caps = 'Air Conditioning' in amenity_columns %}

{# Determine which column to use #}
{% set ac_column = 
    '"Air conditioning"' if has_air_conditioning else (
    '"AC"' if has_ac else (
    '"A/C"' if has_ac_slash else (
    '"Air Conditioning"' if has_air_conditioning_caps else 'NULL'
    )))
%}

with monthly_revenue_by_ac as (
    select
        f.calendar_month,
        d.calendar_year,
        d.month_name,
        d.quarter_name,
        d.fiscal_year,
        d.season,
        -- Dynamically use the correct AC column
        {{ ac_column }} as has_air_conditioning,
        sum(f.daily_revenue) as total_revenue,
        count(distinct f.listing_id) as listing_count,
        sum(f.is_occupied) as occupied_nights
    -- INNER JOIN: dim_date should contain all dates in fact table
    from {{ ref('fct_daily_listing_performance') }} f
    inner join {{ ref('dim_date') }} d 
        on f.calendar_date = d.date_day
    group by 1, 2, 3, 4, 5, 6, 7
),

monthly_totals as (
    select
        calendar_month,
        sum(total_revenue) as month_total_revenue,
        sum(listing_count) as month_total_listings
    from monthly_revenue_by_ac
    group by 1
),

with_yoy as (
    select
        mr.*,
        lag(mr.total_revenue) over (
            partition by mr.has_air_conditioning, date_part('month', mr.calendar_month)
            order by mr.calendar_month
        ) as same_month_last_year_revenue
    from monthly_revenue_by_ac mr
)

select
    yoy.calendar_month,
    yoy.calendar_year,
    yoy.month_name,
    yoy.quarter_name,
    yoy.fiscal_year,
    yoy.season,
    yoy.has_air_conditioning,
    case 
        when yoy.has_air_conditioning then 'With AC' 
        else 'Without AC' 
    end as ac_segment,
    yoy.total_revenue,
    yoy.listing_count,
    yoy.occupied_nights,
    mt.month_total_revenue,
    
    -- Percentage of monthly revenue
    round(
        (yoy.total_revenue / nullif(mt.month_total_revenue, 0)) * 100, 
        1
    ) as pct_of_monthly_revenue,
    
    -- Average revenue per listing
    round(yoy.total_revenue / nullif(yoy.listing_count, 0), 2) as avg_revenue_per_listing,
    
    -- Year-over-year change
    yoy.same_month_last_year_revenue,
    round(
        (yoy.total_revenue - yoy.same_month_last_year_revenue) / 
        nullif(yoy.same_month_last_year_revenue, 0) * 100,
        1
    ) as yoy_revenue_change_pct,
    
    -- Amenity validation info
    '{{ ac_column | replace('"', '') }}' as ac_column_used

from with_yoy yoy
join monthly_totals mt on yoy.calendar_month = mt.calendar_month
order by yoy.calendar_month, yoy.has_air_conditioning desc
