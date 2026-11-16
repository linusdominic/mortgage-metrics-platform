{#
    Every bronze LAR row, typed and classified. Nothing is filtered here: each row
    gets a reject_reason (null when the row is usable) and the two downstream
    models split on it, so accepted + quarantined always equals bronze.
#}
with source as (

    select * from {{ ref('base_hmda__lar') }}

),

typed as (

    select
        -- partition and lineage
        cast(year as integer) as partition_year,
        cast(state as varchar) as partition_state,
        source_row_number,
        _source_sha256,
        _ingested_at,

        -- identifiers and geography
        activity_year,
        state_code,
        lei,
        nullif(county_code, 'NA') as county_fips,
        nullif(census_tract, 'NA') as census_tract,
        derived_msa_md as msa_md_code,

        -- application outcome
        action_taken,
        preapproval,
        purchaser_type,
        {{ hmda_code_or_null('denial_reason_1') }} as denial_reason_1,
        {{ hmda_code_or_null('denial_reason_2') }} as denial_reason_2,
        {{ hmda_code_or_null('denial_reason_3') }} as denial_reason_3,
        {{ hmda_code_or_null('denial_reason_4') }} as denial_reason_4,
        coalesce(denial_reason_1 = 1111, false) as is_denial_reason_exempt,

        -- product attributes
        loan_type,
        loan_purpose,
        lien_status,
        occupancy_type,
        construction_method,
        conforming_loan_limit,
        open_end_line_of_credit,
        reverse_mortgage,
        business_or_commercial_purpose,
        hoepa_status,
        derived_loan_product_type,
        derived_dwelling_category,

        -- applicant attributes (as derived and published by the FFIEC)
        derived_ethnicity,
        derived_race,
        derived_sex,
        applicant_age,

        -- amounts and pricing: number + status for every sentinel-carrying field
        loan_amount,
        {{ hmda_numeric('property_value') }} as property_value,
        {{ hmda_value_status('property_value') }} as property_value_status,
        {{ hmda_numeric('income') }} as income_thousands,
        {{ hmda_value_status('income') }} as income_status,
        {{ hmda_numeric('interest_rate') }} as interest_rate,
        {{ hmda_value_status('interest_rate') }} as interest_rate_status,
        {{ hmda_numeric('rate_spread') }} as rate_spread,
        {{ hmda_value_status('rate_spread') }} as rate_spread_status,
        {{ hmda_numeric('loan_to_value_ratio') }} as loan_to_value_ratio,
        {{ hmda_value_status('loan_to_value_ratio') }} as loan_to_value_ratio_status,
        {{ hmda_numeric('loan_term') }} as loan_term_months,
        {{ hmda_value_status('loan_term') }} as loan_term_status,
        {{ hmda_numeric('total_loan_costs') }} as total_loan_costs,
        {{ hmda_value_status('total_loan_costs') }} as total_loan_costs_status,
        {{ hmda_numeric('origination_charges') }} as origination_charges,
        {{ hmda_numeric('discount_points') }} as discount_points,
        {{ hmda_numeric('lender_credits') }} as lender_credits,

        -- debt-to-income is published as a mix of buckets and whole percents
        case
            when debt_to_income_ratio is null then 'Missing'
            when debt_to_income_ratio = 'NA' then 'Not applicable'
            when debt_to_income_ratio = 'Exempt' then 'Exempt'
            when {{ safe_cast_double('debt_to_income_ratio') }} between 36 and 49.999 then '36%-<50%'
            else debt_to_income_ratio
        end as debt_to_income_band,

        -- census tract context
        tract_minority_population_percent,
        tract_to_msa_income_percentage
    from source

),

classified as (

    select
        *,
        case
            when
                coalesce(activity_year <> partition_year, true)
                or coalesce(state_code <> partition_state, true)
                then 'partition_mismatch'
            when lei is null or length(lei) <> 20
                then 'invalid_lei'
            when action_taken is null or not ({{ in_code_domain('action_taken', 'action_taken') }})
                then 'invalid_action_taken'
            when loan_amount is null or loan_amount <= 0
                then 'invalid_loan_amount'
            when
                loan_type is null or not ({{ in_code_domain('loan_type', 'loan_type') }})
                or loan_purpose is null or not ({{ in_code_domain('loan_purpose', 'loan_purpose') }})
                or lien_status is null or not ({{ in_code_domain('lien_status', 'lien_status') }})
                or occupancy_type is null or not ({{ in_code_domain('occupancy_type', 'occupancy_type') }})
                or construction_method is null
                or not ({{ in_code_domain('construction_method', 'construction_method') }})
                or open_end_line_of_credit is null
                or not ({{ in_code_domain('open_end_line_of_credit', 'open_end_line_of_credit') }})
                or reverse_mortgage is null
                or not ({{ in_code_domain('reverse_mortgage', 'reverse_mortgage') }})
                or business_or_commercial_purpose is null
                or not ({{ in_code_domain('business_or_commercial_purpose', 'business_or_commercial_purpose') }})
                then 'unknown_product_code'
            when
                conforming_loan_limit is null
                or not ({{ in_text_domain('conforming_loan_limit', 'conforming_loan_limit') }})
                or derived_ethnicity is null or not ({{ in_text_domain('derived_ethnicity', 'derived_ethnicity') }})
                or derived_race is null or not ({{ in_text_domain('derived_race', 'derived_race') }})
                or derived_sex is null or not ({{ in_text_domain('derived_sex', 'derived_sex') }})
                or applicant_age is null or not ({{ in_text_domain('applicant_age', 'applicant_age') }})
                then 'unknown_domain_value'
        end as reject_reason
    from typed

)

select
    {{ surrogate_key(['partition_year', 'partition_state', 'source_row_number']) }} as application_id,
    *
from classified
