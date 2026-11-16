{#
    Year-grain date dimension. The public LAR removes application and action
    dates, so the activity year is the only time attribute that exists. Each year
    carries the Freddie Mac PMMS summary for market-rate context.
#}
with years as (

    select cast(year_value as integer) as activity_year
    from generate_series(
        {{ var('first_activity_year') }},
        cast(extract(year from current_date) as integer)
    ) as series (year_value)

)

select
    cast(years.activity_year as integer) as date_key,
    cast(years.activity_year as integer) as activity_year,
    cast(make_date(years.activity_year, 1, 1) as date) as year_start_date,
    cast(make_date(years.activity_year, 12, 31) as date) as year_end_date,
    cast(pmms.survey_weeks as integer) as pmms_survey_weeks,
    cast(pmms.pmms_30y_avg as double) as pmms_30y_avg,
    cast(pmms.pmms_30y_min as double) as pmms_30y_min,
    cast(pmms.pmms_30y_max as double) as pmms_30y_max,
    cast(pmms.pmms_15y_avg as double) as pmms_15y_avg
from years
left join {{ ref('int_pmms__annual') }} as pmms
    on years.activity_year = pmms.rate_year
