select
    county_fips,
    state_fips,
    state_code,
    county_name
from {{ source('bronze', 'census_counties') }}
