{#
    Year-over-year row-count change per state. A state that drops to zero, or
    moves by more than the configured percentages, is flagged. Mortgage volume
    is cyclical (2022 -> 2023 fell sharply with rates), so "warn" is expected to
    fire on real data; "fail" is reserved for changes that look like a broken load.
#}
with volumes as (

    select
        activity_year,
        state_code,
        count(*) as fact_rows
    from {{ ref('fct_loan_applications') }}
    group by 1, 2

),

lagged as (

    select
        *,
        lag(fact_rows) over (partition by state_code order by activity_year) as previous_year_rows,
        lag(activity_year) over (partition by state_code order by activity_year) as previous_year
    from volumes

),

compared as (

    select
        *,
        100.0 * (fact_rows - previous_year_rows) / nullif(previous_year_rows, 0) as pct_change
    from lagged

)

select
    cast(activity_year as integer) as activity_year,
    cast(state_code as varchar) as state_code,
    cast(fact_rows as bigint) as fact_rows,
    cast(previous_year as integer) as previous_year,
    cast(previous_year_rows as bigint) as previous_year_rows,
    cast(round(pct_change, 2) as double) as pct_change,
    cast(
        case
            when previous_year_rows is null or previous_year <> activity_year - 1 then 'no_baseline'
            when abs(pct_change) >= {{ var('volume_fail_pct') }} then 'fail'
            when abs(pct_change) >= {{ var('volume_warn_pct') }} then 'warn'
            else 'ok'
        end as varchar
    ) as status
from compared
