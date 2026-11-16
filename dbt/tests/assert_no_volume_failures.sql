-- A year-over-year volume change beyond var('volume_fail_pct') looks like a
-- broken or partial load, not a market move. Blocks the build.
select *
from {{ ref('ops_volume_anomalies') }}
where status = 'fail'
