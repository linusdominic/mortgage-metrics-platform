{#
    Application-grain rows with outcome flags, derived measures and the
    dimension keys. Every input is from the row's own activity year, so a fact
    partition can be rebuilt without touching any other partition.
#}
with applications as (

    select * from {{ ref('stg_hmda__loan_applications') }}

),

lenders as (

    select * from {{ ref('int_lender__names_by_year') }}

),

pmms as (

    select * from {{ ref('int_pmms__annual') }}

),

flagged as (

    select
        applications.*,
        lenders.lender_key,
        pmms.pmms_30y_avg,

        -- Purchased loans are reported under HMDA but were not applications to the reporter.
        action_taken <> 6 as is_application,
        action_taken = 6 as is_purchased_loan,
        action_taken = 1 as is_originated,
        action_taken in (1, 2) as is_approved,
        action_taken = 3 as is_denied,
        action_taken in (1, 2, 3) as is_decisioned,
        action_taken in (4, 5) as is_withdrawn_or_incomplete,
        action_taken in (7, 8) as is_preapproval_request
    from applications
    left join lenders
        on
            applications.lei = lenders.lei
            and applications.activity_year = lenders.activity_year
    left join pmms
        on applications.activity_year = pmms.rate_year

)

select
    application_id,
    partition_year,
    partition_state,
    activity_year,
    state_code,
    lei,
    source_row_number,
    _source_sha256,

    -- dimension keys
    lender_key,
    {{ surrogate_key(['state_code', "coalesce(county_fips, 'unknown')"]) }} as geography_key,
    {{ surrogate_key([
        'loan_type', 'loan_purpose', 'lien_status', 'occupancy_type', 'construction_method',
        'conforming_loan_limit', 'open_end_line_of_credit', 'reverse_mortgage',
        'business_or_commercial_purpose'
    ]) }} as loan_product_key,
    {{ surrogate_key(['derived_ethnicity', 'derived_race', 'derived_sex', 'applicant_age']) }}
        as applicant_demographics_key,
    activity_year as date_key,

    -- outcome
    action_taken,
    is_application,
    is_purchased_loan,
    is_originated,
    is_approved,
    is_denied,
    is_decisioned,
    is_withdrawn_or_incomplete,
    is_preapproval_request,
    case when is_denied and denial_reason_1 <> 10 then denial_reason_1 end as primary_denial_reason_code,
    denial_reason_1,
    denial_reason_2,
    denial_reason_3,
    denial_reason_4,
    debt_to_income_band,

    -- measures
    loan_amount,
    property_value,
    income_thousands,
    income_status,
    interest_rate,
    interest_rate_status,
    rate_spread,
    rate_spread_status,
    loan_to_value_ratio,
    loan_term_months,
    total_loan_costs,
    case
        when income_status = 'reported' and income_thousands > 0
            then loan_amount / (income_thousands * 1000)
    end as loan_to_income_ratio,

    case
        when not is_originated then 'Not originated'
        when rate_spread is null then 'Not reported'
        when rate_spread < 0 then 'Below 0'
        when rate_spread < 0.5 then '0 to <0.5'
        when rate_spread < 1.0 then '0.5 to <1.0'
        when rate_spread < 1.5 then '1.0 to <1.5'
        when rate_spread < 3.0 then '1.5 to <3.0'
        else '3.0 or more'
    end as rate_spread_band,

    -- Note rate minus the annual average 30-year PMMS rate, for loans the survey
    -- is comparable to: originated, first-lien, 30-year, closed-end, not reverse.
    case
        when
            is_originated
            and interest_rate is not null
            and loan_term_months = 360
            and lien_status = 1
            and open_end_line_of_credit = 2
            and reverse_mortgage = 2
            then interest_rate - pmms_30y_avg
    end as rate_over_pmms_30y
from flagged
