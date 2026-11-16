-- Outcome flags must partition the action codes the way the metrics assume.
select application_id
from {{ ref('fct_loan_applications') }}
where
    (is_approved and is_denied)
    or (is_originated and not is_approved)
    or (is_decisioned <> (is_approved or is_denied))
    or (is_purchased_loan = is_application)
    or (rate_over_pmms_30y is not null and not is_originated)
