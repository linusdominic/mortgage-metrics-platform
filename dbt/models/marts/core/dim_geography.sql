{#
    County-grain geography from the Census 2020 county file, plus one
    "county not reported" member per state for LAR rows without a county code.
#}
with counties as (

    select * from {{ ref('stg_census__counties') }}

),

states as (

    select distinct
        state_code,
        state_fips
    from counties

),

members as (

    select
        state_code,
        state_fips,
        county_fips,
        county_name,
        true as is_county_reported
    from counties

    union all

    select
        state_code,
        state_fips,
        cast(null as varchar) as county_fips,
        'County not reported' as county_name,
        false as is_county_reported
    from states

)

select
    cast({{ surrogate_key(['state_code', "coalesce(county_fips, 'unknown')"]) }} as varchar) as geography_key,
    cast(state_code as varchar) as state_code,
    cast(state_fips as varchar) as state_fips,
    cast(county_fips as varchar) as county_fips,
    cast(county_name as varchar) as county_name,
    cast(is_county_reported as boolean) as is_county_reported
from members
