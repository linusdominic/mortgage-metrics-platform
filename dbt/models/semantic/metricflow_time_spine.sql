-- Day-grain time spine required by MetricFlow.
select cast(day_value as date) as date_day
from generate_series(
    make_date({{ var('first_activity_year') }}, 1, 1),
    make_date(cast(extract(year from current_date) as integer), 12, 31),
    interval 1 day
) as series (day_value)
