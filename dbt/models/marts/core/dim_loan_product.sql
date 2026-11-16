{#
    Junk dimension: the cross product of the low-cardinality loan attribute
    domains. Built from the seeded code lists rather than from observed rows so
    it is complete, deterministic and independent of which partitions are loaded.
#}
{% set code_fields = [
    'loan_type', 'loan_purpose', 'lien_status', 'occupancy_type', 'construction_method',
    'open_end_line_of_credit', 'reverse_mortgage', 'business_or_commercial_purpose'
] %}

with
{% for field in code_fields %}
    {{ field }} as (

        select
            code as {{ field }}_code,
            label as {{ field }}
        from {{ ref('hmda_codes') }}
        where field = '{{ field }}'

    ),
{% endfor %}

conforming as (

    select
        value as conforming_loan_limit_code,
        case value
            when 'C' then 'Conforming'
            when 'NC' then 'Nonconforming'
            when 'U' then 'Undetermined'
            else 'Not applicable'
        end as conforming_loan_limit
    from {{ ref('hmda_derived_domains') }}
    where domain = 'conforming_loan_limit'

)

select
    cast({{ surrogate_key([
        'loan_type_code', 'loan_purpose_code', 'lien_status_code', 'occupancy_type_code',
        'construction_method_code', 'conforming_loan_limit_code', 'open_end_line_of_credit_code',
        'reverse_mortgage_code', 'business_or_commercial_purpose_code'
    ]) }} as varchar) as loan_product_key,
    cast(loan_type_code as integer) as loan_type_code,
    cast(loan_type as varchar) as loan_type,
    cast(loan_purpose_code as integer) as loan_purpose_code,
    cast(loan_purpose as varchar) as loan_purpose,
    cast(lien_status_code as integer) as lien_status_code,
    cast(lien_status as varchar) as lien_status,
    cast(occupancy_type_code as integer) as occupancy_type_code,
    cast(occupancy_type as varchar) as occupancy_type,
    cast(construction_method_code as integer) as construction_method_code,
    cast(construction_method as varchar) as construction_method,
    cast(conforming_loan_limit_code as varchar) as conforming_loan_limit_code,
    cast(conforming_loan_limit as varchar) as conforming_loan_limit,
    cast(open_end_line_of_credit_code as integer) as open_end_line_of_credit_code,
    cast(open_end_line_of_credit as varchar) as open_end_line_of_credit,
    cast(reverse_mortgage_code as integer) as reverse_mortgage_code,
    cast(reverse_mortgage as varchar) as reverse_mortgage,
    cast(business_or_commercial_purpose_code as integer) as business_or_commercial_purpose_code,
    cast(business_or_commercial_purpose as varchar) as business_or_commercial_purpose
from loan_type
{% for field in code_fields[1:] %}
    cross join {{ field }}
{% endfor %}
cross join conforming
