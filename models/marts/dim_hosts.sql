{{
    config(
        materialized='table',
        schema='mart'
    )
}}

/*
================================================================================
FILE: dim_hosts.sql
LAYER: Marts (Dimension)
SCHEMA: mart
================================================================================

PURPOSE:
    Host dimension with SCD Type 2 history tracking.
    Provides host attributes and derived metrics for analysis.

LOGIC:
    1. Source host history from int_hosts_history
    2. Generate surrogate key from host_id + valid_from
    3. Calculate derived attributes:
       - host_tenure_years: Years since host joined
       - host_experience_tier: New/Established/Veteran classification
    4. Preserve SCD metadata (valid_from, valid_to, is_current)

KEY FEATURES:
    - Surrogate key (host_sk) for dimension joins
    - Natural key (host_id) for business identification
    - Experience tier classification
    - SCD Type 2 support (currently single record per host)

SOURCE: int_hosts_history
GRAIN: One row per host per validity period

DOWNSTREAM DEPENDENCIES:
    - Can be joined to fact tables via host_id
================================================================================
*/

with host_scd as (
    select
        -- Surrogate key for SCD
        {{ generate_surrogate_key(['host_id', 'valid_from']) }} as host_sk,
        
        -- Natural key
        host_id,
        
        -- Attributes
        host_name,
        host_location,
        host_since_date,
        
        -- Derived attributes
        datediff(year, host_since_date, current_date()) as host_tenure_years,
        case
            when datediff(year, host_since_date, current_date()) < 1 then 'New Host'
            when datediff(year, host_since_date, current_date()) < 3 then 'Established Host'
            else 'Veteran Host'
        end as host_experience_tier,
        
        -- SCD metadata
        valid_from,
        valid_to,
        is_current,
        
        -- Audit columns
        current_timestamp() as dbt_updated_at
        
    from {{ ref('int_hosts_history') }}
)

select * from host_scd
