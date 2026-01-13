/*
================================================================================
FILE: generate_surrogate_key.sql
LAYER: Macros
================================================================================

PURPOSE:
    Generate a surrogate key from a list of fields.
    Custom implementation to avoid dependency on dbt_utils package.

LOGIC:
    1. Concatenate all fields with '|' separator
    2. Handle NULLs by converting to empty string
    3. Apply MD5 hash to create unique identifier

USAGE:
    {{ generate_surrogate_key(['listing_id', 'valid_from']) }} as listing_sk

OUTPUT:
    32-character MD5 hash string

EXAMPLE:
    Input: listing_id=123, valid_from='2022-01-01'
    Concatenated: '123|2022-01-01'
    Output: 'a1b2c3d4e5f6...' (MD5 hash)

NOTE:
    This is a simplified version of dbt_utils.generate_surrogate_key.
    For production, consider using the full dbt_utils package.
================================================================================
*/

{% macro generate_surrogate_key(field_list) %}
{# 
    Generate a surrogate key from a list of fields.
    
    Args:
        field_list: List of column names to include in the key
        
    Returns:
        SQL expression that produces an MD5 hash
#}
    md5(
        concat_ws('|',
            {% for field in field_list %}
                coalesce(cast({{ field }} as varchar), '')
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
