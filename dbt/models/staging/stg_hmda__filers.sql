{#
    One row per (lei, activity_year). The filers endpoint is called once per
    state, so an institution active in several states appears several times; a
    handful of institutions are also listed under more than one spelling in the
    same year. The spelling with the most LAR records wins, ties broken by name.
#}
with source as (

    select * from {{ ref('base_hmda__filers') }}

),

per_name as (

    select
        lei,
        activity_year,
        trim(institution_name) as lender_name,
        sum(lar_count) as lar_count_in_scope
    from source
    where lei is not null and trim(institution_name) <> ''
    group by 1, 2, 3

),

ranked as (

    select
        *,
        row_number() over (
            partition by lei, activity_year
            order by lar_count_in_scope desc, lender_name asc
        ) as name_rank,
        sum(lar_count_in_scope) over (partition by lei, activity_year) as lar_count_total
    from per_name

)

select
    lei,
    activity_year,
    lender_name,
    cast(lar_count_total as bigint) as lar_count_in_scope
from ranked
where name_rank = 1
