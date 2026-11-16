{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        pre_hook="{{ delete_pending_partitions() }}",
        post_hook="{{ log_loaded_partitions() }}"
    )
}}

{#
    Row-count and checksum reconciliation per LAR partition, across every hop:

      raw manifest -> bronze Parquet -> staging (accepted + quarantined) -> fact

    Recomputed for the partitions loaded in this run. The singular test
    assert_reconciliation_balanced fails the build if any hop does not balance.
#}
with manifest as (

    select *
    from {{ ref('stg_ops__ingest_manifest') }}
    where dataset = 'hmda_lar' and {{ pending_partition_predicate() }}

),

bronze as (

    select
        partition_year as activity_year,
        partition_state as state_code,
        count(*) as bronze_rows,
        sum(loan_amount) as bronze_loan_amount,
        count(*) filter (where reject_reason is null) as staged_rows,
        sum(loan_amount) filter (where reject_reason is null) as staged_loan_amount,
        count(*) filter (where reject_reason is not null) as rejected_rows,
        min(_source_sha256) as bronze_sha256,
        count(distinct _source_sha256) as bronze_sha256_count
    from {{ ref('stg_hmda__lar_typed') }}
    where {{ pending_partition_predicate('partition_year', 'partition_state') }}
    group by 1, 2

),

quarantine as (

    select
        activity_year,
        state_code,
        count(*) as quarantined_rows
    from {{ ref('rejected_lar_rows') }}
    where {{ pending_partition_predicate() }}
    group by 1, 2

),

fact as (

    select
        activity_year,
        state_code,
        count(*) as fact_rows,
        count(distinct application_id) as fact_distinct_ids,
        sum(loan_amount) as fact_loan_amount,
        min(_source_sha256) as fact_sha256
    from {{ ref('fct_loan_applications') }}
    where {{ pending_partition_predicate() }}
    group by 1, 2

),

joined as (

    select
        manifest.activity_year,
        manifest.state_code,
        manifest.source_sha256,
        manifest.raw_bytes,
        manifest.raw_row_count,
        coalesce(bronze.bronze_rows, 0) as bronze_rows,
        coalesce(bronze.staged_rows, 0) as staged_rows,
        coalesce(bronze.rejected_rows, 0) as rejected_rows,
        coalesce(quarantine.quarantined_rows, 0) as quarantined_rows,
        coalesce(fact.fact_rows, 0) as fact_rows,
        coalesce(fact.fact_distinct_ids, 0) as fact_distinct_ids,
        coalesce(bronze.bronze_loan_amount, 0) as bronze_loan_amount,
        coalesce(bronze.staged_loan_amount, 0) as staged_loan_amount,
        coalesce(fact.fact_loan_amount, 0) as fact_loan_amount,
        bronze.bronze_sha256,
        bronze.bronze_sha256_count,
        fact.fact_sha256
    from manifest
    left join bronze
        on manifest.activity_year = bronze.activity_year and manifest.state_code = bronze.state_code
    left join quarantine
        on manifest.activity_year = quarantine.activity_year and manifest.state_code = quarantine.state_code
    left join fact
        on manifest.activity_year = fact.activity_year and manifest.state_code = fact.state_code

),

checked as (

    select
        *,
        coalesce(
            raw_row_count = bronze_rows
            and bronze_sha256_count = 1
            and source_sha256 = bronze_sha256,
            false
        ) as is_raw_to_bronze_balanced,
        coalesce(
            bronze_rows = staged_rows + quarantined_rows
            and rejected_rows = quarantined_rows,
            false
        ) as is_bronze_to_staging_balanced,
        coalesce(
            staged_rows = fact_rows
            and fact_rows = fact_distinct_ids
            and abs(staged_loan_amount - fact_loan_amount) < 0.01
            and (fact_rows = 0 or source_sha256 = fact_sha256),
            false
        ) as is_staging_to_fact_balanced
    from joined

)

select
    cast(activity_year as integer) as activity_year,
    cast(state_code as varchar) as state_code,
    cast(source_sha256 as varchar) as source_sha256,
    cast(raw_bytes as bigint) as raw_bytes,
    cast(raw_row_count as bigint) as raw_row_count,
    cast(bronze_rows as bigint) as bronze_rows,
    cast(staged_rows as bigint) as staged_rows,
    cast(quarantined_rows as bigint) as quarantined_rows,
    cast(fact_rows as bigint) as fact_rows,
    cast(bronze_loan_amount as double) as bronze_loan_amount,
    cast(staged_loan_amount as double) as staged_loan_amount,
    cast(fact_loan_amount as double) as fact_loan_amount,
    cast(is_raw_to_bronze_balanced as boolean) as is_raw_to_bronze_balanced,
    cast(is_bronze_to_staging_balanced as boolean) as is_bronze_to_staging_balanced,
    cast(is_staging_to_fact_balanced as boolean) as is_staging_to_fact_balanced,
    cast(
        is_raw_to_bronze_balanced and is_bronze_to_staging_balanced and is_staging_to_fact_balanced
        as boolean
    ) as is_balanced,
    cast(current_timestamp as timestamp with time zone) as reconciled_at
from checked
