{{
    config(
        schema='quarantine',
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        pre_hook="{{ delete_pending_partitions() }}",
        post_hook="{{ log_loaded_partitions() }}"
    )
}}

-- LAR rows that failed a staging rule, kept with the reason instead of dropped.
select
    cast(application_id as varchar) as application_id,
    cast(partition_year as integer) as activity_year,
    cast(partition_state as varchar) as state_code,
    cast(source_row_number as bigint) as source_row_number,
    cast(reject_reason as varchar) as reject_reason,
    cast(lei as varchar) as lei,
    cast(activity_year as integer) as reported_activity_year,
    cast(state_code as varchar) as reported_state_code,
    cast(action_taken as integer) as action_taken,
    cast(loan_amount as double) as loan_amount,
    cast(_source_sha256 as varchar) as _source_sha256,
    cast(current_timestamp as timestamp with time zone) as quarantined_at
from {{ ref('stg_hmda__lar_typed') }}
where
    reject_reason is not null
    and {{ pending_partition_predicate('partition_year', 'partition_state') }}
