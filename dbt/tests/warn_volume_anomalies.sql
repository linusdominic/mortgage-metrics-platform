{{ config(severity='warn') }}

-- Changes beyond var('volume_warn_pct') are surfaced but do not block.
select *
from {{ ref('ops_volume_anomalies') }}
where status = 'warn'
