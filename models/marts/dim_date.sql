{{
    config(
        materialized='table',
        schema='mart'
    )
}}

/*
================================================================================
FILE: dim_date.sql
LAYER: Marts (Dimension)
SCHEMA: mart
================================================================================

PURPOSE:
    Comprehensive date dimension table for time intelligence.
    Supports calendar analysis, fiscal year reporting, and seasonality studies.

LOGIC:
    1. Generate a date spine from 2020 to 2030 using Snowflake's generator
    2. Calculate all calendar attributes (day, week, month, quarter, year)
    3. Add fiscal year calculations (assumes April 1 start)
    4. Identify weekends and holidays
    5. Classify seasons (Northern Hemisphere)
    6. Create period-over-period helper columns

KEY FEATURES:
    - date_key: Integer key (YYYYMMDD) for efficient joins
    - Fiscal year support (configurable start month)
    - Holiday flags (US major holidays)
    - Season classification
    - Peak rental season identification
    - Same day last week/month/year for comparisons

SOURCE: Generated (no source table)
GRAIN: One row per calendar date

DOWNSTREAM DEPENDENCIES:
    - fct_daily_listing_performance (joins on date_key)
    - All analytics views for time intelligence
================================================================================
*/

with date_spine as (
    -- Generate dates from 2020 to 2030
    select
        dateadd(day, seq4(), '2020-01-01'::date) as date_day
    from table(generator(rowcount => 4018))  -- ~11 years of dates
),

holidays as (
    -- US Federal Holidays (simplified - actual holidays may vary by year)
    select date_day, holiday_name
    from (
        values
        -- These are example dates; in production, use a holiday calendar table
        ('2021-01-01'::date, 'New Year''s Day'),
        ('2021-07-04'::date, 'Independence Day'),
        ('2021-12-25'::date, 'Christmas Day'),
        ('2022-01-01'::date, 'New Year''s Day'),
        ('2022-07-04'::date, 'Independence Day'),
        ('2022-12-25'::date, 'Christmas Day'),
        ('2023-01-01'::date, 'New Year''s Day'),
        ('2023-07-04'::date, 'Independence Day'),
        ('2023-12-25'::date, 'Christmas Day'),
        ('2024-01-01'::date, 'New Year''s Day'),
        ('2024-07-04'::date, 'Independence Day'),
        ('2024-12-25'::date, 'Christmas Day'),
        ('2025-01-01'::date, 'New Year''s Day'),
        ('2025-07-04'::date, 'Independence Day'),
        ('2025-12-25'::date, 'Christmas Day')
    ) as h(date_day, holiday_name)
)

select
    -- Primary key
    d.date_day,
    
    -- Day attributes
    dayofweek(d.date_day) as day_of_week,           -- 0=Sunday, 6=Saturday
    dayname(d.date_day) as day_name,                 -- 'Mon', 'Tue', etc.
    dayofmonth(d.date_day) as day_of_month,
    dayofyear(d.date_day) as day_of_year,
    
    -- Week attributes
    weekofyear(d.date_day) as week_of_year,
    date_trunc('week', d.date_day)::date as week_start_date,
    dateadd(day, 6, date_trunc('week', d.date_day))::date as week_end_date,
    
    -- Month attributes
    month(d.date_day) as month_number,
    monthname(d.date_day) as month_name,
    date_trunc('month', d.date_day)::date as month_start_date,
    last_day(d.date_day) as month_end_date,
    
    -- Quarter attributes
    quarter(d.date_day) as quarter_number,
    'Q' || quarter(d.date_day) as quarter_name,
    date_trunc('quarter', d.date_day)::date as quarter_start_date,
    
    -- Year attributes
    year(d.date_day) as calendar_year,
    date_trunc('year', d.date_day)::date as year_start_date,
    
    -- Fiscal year (assuming April 1 start)
    case 
        when month(d.date_day) >= 4 then year(d.date_day)
        else year(d.date_day) - 1
    end as fiscal_year,
    case 
        when month(d.date_day) >= 4 then quarter(d.date_day) - 1
        else quarter(d.date_day) + 3
    end as fiscal_quarter,
    
    -- Weekend/Weekday
    case 
        when dayofweek(d.date_day) in (0, 6) then true 
        else false 
    end as is_weekend,
    case 
        when dayofweek(d.date_day) in (0, 6) then false 
        else true 
    end as is_weekday,
    
    -- Holiday
    case when h.holiday_name is not null then true else false end as is_holiday,
    h.holiday_name,
    
    -- Season (Northern Hemisphere)
    case
        when month(d.date_day) in (12, 1, 2) then 'Winter'
        when month(d.date_day) in (3, 4, 5) then 'Spring'
        when month(d.date_day) in (6, 7, 8) then 'Summer'
        when month(d.date_day) in (9, 10, 11) then 'Fall'
    end as season,
    
    -- Peak rental season (Summer + major holidays)
    case
        when month(d.date_day) in (6, 7, 8) then true
        when h.holiday_name is not null then true
        else false
    end as is_peak_season,
    
    -- Period-over-period helpers
    dateadd(day, -7, d.date_day) as same_day_last_week,
    dateadd(month, -1, d.date_day) as same_day_last_month,
    dateadd(year, -1, d.date_day) as same_day_last_year,
    
    -- Relative date flags (calculated at query time in views, but useful reference)
    datediff(day, d.date_day, current_date()) as days_ago,
    
    -- ISO format for compatibility
    to_char(d.date_day, 'YYYY-MM-DD') as date_iso,
    to_char(d.date_day, 'YYYYMMDD')::int as date_key,
    
    -- Audit timestamp: when this row was last processed by dbt
    current_timestamp() as dbt_updated_at

from date_spine d
left join holidays h on d.date_day = h.date_day
where d.date_day <= '2030-12-31'
order by d.date_day
