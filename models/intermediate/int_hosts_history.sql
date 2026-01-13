{{
    config(
        materialized='view',
        schema='development'
    )
}}

/*
================================================================================
FILE: int_hosts_history.sql
LAYER: Intermediate
SCHEMA: development
================================================================================

PURPOSE:
    Prepare host data for SCD Type 2 dimension.
    Derives host history from listings data since no dedicated host changelog exists.

LOGIC:
    1. Extract distinct host records from stg_listings
    2. Use first_review_date as proxy for when host record was observed
    3. Deduplicate to get most recent record per host
    4. Set validity windows (valid_from = host_since_date, valid_to = 9999-12-31)

LIMITATION:
    In production, you would have a separate hosts_changelog table.
    Current implementation treats all hosts as having single SCD record.

SOURCE: stg_listings
GRAIN: One row per host (currently single record per host)

DOWNSTREAM DEPENDENCIES:
    - dim_hosts
================================================================================
*/

with host_records as (
    select distinct
        host_id,
        host_name,
        host_location,
        host_since_date,
        -- Use listing's first review date as a proxy for when this host record was valid
        first_review_date as record_observed_date
    from {{ ref('stg_listings') }}
    where host_id is not null
),

-- Deduplicate and get the most recent record per host
ranked_hosts as (
    select
        host_id,
        host_name,
        host_location,
        host_since_date,
        record_observed_date,
        row_number() over (
            partition by host_id 
            order by record_observed_date desc nulls last
        ) as rn
    from host_records
)

select
    host_id,
    host_name,
    host_location,
    host_since_date,
    record_observed_date,
    -- For now, since we don't have historical changes, treat as current
    coalesce(host_since_date, '1900-01-01'::date) as valid_from,
    '9999-12-31'::date as valid_to,
    true as is_current
from ranked_hosts
where rn = 1
