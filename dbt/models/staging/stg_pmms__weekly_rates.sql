select
    week_date,
    cast(extract(year from week_date) as integer) as rate_year,
    rate_30y_fixed,
    rate_15y_fixed
from {{ source('bronze', 'pmms_weekly') }}
where rate_30y_fixed is not null
