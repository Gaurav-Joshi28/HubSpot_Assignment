/*
================================================================================
FILE: check_late_arrivals.sql
LAYER: Macros
================================================================================

PURPOSE:
    Detect late-arriving records in raw data whose business date is older than
    the incremental window (default 7 days), but were recently ingested. This
    signals that a backfill/full refresh is needed to update historical rows.

LOGIC:
    1) Find max calendar_date already in the fact (fct_daily_listing_performance).
    2) In raw.calendar, find rows where:
         - calendar_date < (max_fact_date - window_days)
         - _loaded_at is recent (within lookback_loaded_hours)
    3) Return the count; log it for easy parsing.

USAGE:
    dbt run-operation check_late_arrivals --args '{"window_days": 7, "lookback_loaded_hours": 48}'

RETURNS:
    Integer count of late-arriving rows.
================================================================================
*/

{% macro check_late_arrivals(window_days=7, lookback_loaded_hours=48) %}

    {%- set calendar_cols = adapter.get_columns_in_relation(source('raw', 'calendar')) if execute else [] -%}
    {%- set has_loaded_at = calendar_cols | map(attribute='name') | map('lower') | select('equalto', '_loaded_at') | list | length > 0 -%}

    {%- if not has_loaded_at -%}
        {{ log("LATE_ARRIVAL_COUNT=0 (no _loaded_at column present)", info=True) }}
        {{ return(0) }}
    {%- endif -%}

    {%- set query -%}
        with fact_max as (
            select max(calendar_date) as max_date
            from {{ ref('fct_daily_listing_performance') }}
        ),
        candidates as (
            select
                c.listing_id,
                c.date::date as calendar_date,
                c._loaded_at
            from {{ source('raw', 'calendar') }} c
            cross join fact_max f
            where f.max_date is not null
              and c.date::date < dateadd(day, -{{ window_days }}, f.max_date)
              and c._loaded_at >= dateadd(hour, -{{ lookback_loaded_hours }}, current_timestamp())
        )
        select count(*) as late_count from candidates
    {%- endset -%}

    {% set result = run_query(query) %}
    {% if execute %}
        {% set late_count = result.columns[0].values()[0] %}
    {% else %}
        {% set late_count = 0 %}
    {% endif %}

    {{ log("LATE_ARRIVAL_COUNT=" ~ late_count, info=True) }}
    {{ return(late_count) }}

{% endmacro %}
