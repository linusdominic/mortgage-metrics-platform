{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        pre_hook="{{ delete_pending_partitions() }}",
        post_hook="{{ log_loaded_partitions() }}"
    )
}}

-- Grain: one stated denial reason per denied application (bridge to the fact).
select
    cast({{ surrogate_key(['application_id', 'reason_position']) }} as varchar) as denial_reason_id,
    cast(application_id as varchar) as application_id,
    cast(activity_year as integer) as activity_year,
    cast(make_date(activity_year, 1, 1) as date) as activity_year_start,
    cast(state_code as varchar) as state_code,
    cast(reason_position as integer) as reason_position,
    cast(denial_reason_code as integer) as denial_reason_code,
    cast(denial_reason as varchar) as denial_reason,
    cast(_source_sha256 as varchar) as _source_sha256,
    cast(current_timestamp as timestamp with time zone) as _loaded_at
from {{ ref('int_hmda__denial_reasons') }}
where {{ pending_partition_predicate('partition_year', 'partition_state') }}
