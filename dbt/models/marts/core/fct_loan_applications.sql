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
    Grain: one HMDA LAR record (an application, a preapproval request or a
    purchased loan) reported by one institution for one activity year.

    Loaded by partition overwrite: see macros/partition_overwrite.sql.
#}
select
    cast(application_id as varchar) as application_id,
    cast(lender_key as varchar) as lender_key,
    cast(geography_key as varchar) as geography_key,
    cast(loan_product_key as varchar) as loan_product_key,
    cast(applicant_demographics_key as varchar) as applicant_demographics_key,
    cast(date_key as integer) as date_key,
    cast(activity_year as integer) as activity_year,
    cast(make_date(activity_year, 1, 1) as date) as activity_year_start,
    cast(state_code as varchar) as state_code,
    cast(lei as varchar) as lei,
    cast(action_taken as integer) as action_taken_code,
    cast(actions.label as varchar) as action_taken,
    cast(is_application as boolean) as is_application,
    cast(is_purchased_loan as boolean) as is_purchased_loan,
    cast(is_originated as boolean) as is_originated,
    cast(is_approved as boolean) as is_approved,
    cast(is_denied as boolean) as is_denied,
    cast(is_decisioned as boolean) as is_decisioned,
    cast(is_withdrawn_or_incomplete as boolean) as is_withdrawn_or_incomplete,
    cast(is_preapproval_request as boolean) as is_preapproval_request,
    cast(primary_denial_reason_code as integer) as primary_denial_reason_code,
    cast(coalesce(reasons.label, 'Not denied or not stated') as varchar) as primary_denial_reason,
    cast(debt_to_income_band as varchar) as debt_to_income_band,
    cast(loan_amount as double) as loan_amount,
    cast(property_value as double) as property_value,
    cast(income_thousands as double) as income_thousands,
    cast(income_status as varchar) as income_status,
    cast(interest_rate as double) as interest_rate,
    cast(interest_rate_status as varchar) as interest_rate_status,
    cast(rate_spread as double) as rate_spread,
    cast(rate_spread_status as varchar) as rate_spread_status,
    cast(rate_spread_band as varchar) as rate_spread_band,
    cast(loan_to_value_ratio as double) as loan_to_value_ratio,
    cast(loan_term_months as double) as loan_term_months,
    cast(total_loan_costs as double) as total_loan_costs,
    cast(loan_to_income_ratio as double) as loan_to_income_ratio,
    cast(rate_over_pmms_30y as double) as rate_over_pmms_30y,
    cast(source_row_number as bigint) as source_row_number,
    cast(_source_sha256 as varchar) as _source_sha256,
    cast(current_timestamp as timestamp with time zone) as _loaded_at,
    cast('{{ invocation_id }}' as varchar) as _dbt_invocation_id
from {{ ref('int_hmda__applications_enriched') }} as applications
left join {{ ref('hmda_codes') }} as actions
    on
        actions.field = 'action_taken'
        and applications.action_taken = actions.code
left join {{ ref('hmda_codes') }} as reasons
    on
        reasons.field = 'denial_reason'
        and applications.primary_denial_reason_code = reasons.code
where {{ pending_partition_predicate('applications.partition_year', 'applications.partition_state') }}
