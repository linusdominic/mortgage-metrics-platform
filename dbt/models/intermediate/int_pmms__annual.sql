-- Annual summary of the weekly PMMS survey. HMDA public data carries only the
-- activity year, so a year is the finest grain at which the two can be joined.
select
    rate_year,
    count(*) as survey_weeks,
    round(avg(rate_30y_fixed), 4) as pmms_30y_avg,
    min(rate_30y_fixed) as pmms_30y_min,
    max(rate_30y_fixed) as pmms_30y_max,
    round(avg(rate_15y_fixed), 4) as pmms_15y_avg
from {{ ref('stg_pmms__weekly_rates') }}
group by 1
