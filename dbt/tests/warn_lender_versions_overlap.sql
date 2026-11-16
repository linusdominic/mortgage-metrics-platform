{{ config(severity='warn') }}

-- A lender that goes back to an earlier name (A -> B -> A) produces two versions
-- whose year ranges overlap. Fact keys are still right (they follow the name
-- filed each year); the ranges in dim_lender are then only a summary.
select
    a.lei,
    a.lender_name as name_a,
    b.lender_name as name_b
from {{ ref('dim_lender') }} as a
inner join {{ ref('dim_lender') }} as b
    on
        a.lei = b.lei
        and a.lender_key < b.lender_key
        and a.valid_from_year <= b.valid_to_year
        and a.valid_to_year >= b.valid_from_year
