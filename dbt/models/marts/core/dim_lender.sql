{#
    Type 2 dimension over the lender name, which institutions do change between
    filing years (rebrands, mergers, re-registrations, capitalisation).

    One row per (lei, name as reported). The key is a hash of exactly those two
    values, so a fact row's lender key depends only on its own year's filer list
    and stays valid when an older year is backfilled later. The effective range
    is recomputed from all loaded years on every build.
#}
with names_by_year as (

    select * from {{ ref('int_lender__names_by_year') }}

),

versions as (

    select
        lender_key,
        lei,
        lender_name,
        bool_or(is_in_filer_list) as is_in_filer_list,
        min(activity_year) as first_reported_year,
        max(activity_year) as last_reported_year,
        count(*) as years_reported
    from names_by_year
    group by 1, 2, 3

),

latest as (

    select
        lei,
        max(activity_year) as latest_year
    from names_by_year
    group by 1

)

select
    cast(versions.lender_key as varchar) as lender_key,
    cast(versions.lei as varchar) as lei,
    cast(versions.lender_name as varchar) as lender_name,
    cast(upper(versions.lender_name) as varchar) as lender_name_normalized,
    cast(versions.is_in_filer_list as boolean) as is_in_filer_list,
    cast(versions.first_reported_year as integer) as valid_from_year,
    cast(versions.last_reported_year as integer) as valid_to_year,
    cast(versions.years_reported as integer) as years_reported,
    cast(versions.last_reported_year = latest.latest_year as boolean) as is_current,
    cast(
        row_number() over (
            partition by versions.lei
            order by versions.first_reported_year, versions.last_reported_year, versions.lender_name
        ) as integer
    ) as version_number
from versions
inner join latest
    on versions.lei = latest.lei
