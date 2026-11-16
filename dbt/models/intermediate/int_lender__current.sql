-- Each lei with the name it reported in its most recent loaded year.
-- Source relation of the snap_lender_registry snapshot.
select
    lei,
    lender_name,
    activity_year as as_of_year
from {{ ref('int_lender__names_by_year') }}
qualify row_number() over (partition by lei order by activity_year desc) = 1
