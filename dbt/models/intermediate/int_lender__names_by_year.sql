{#
    Lender name per (lei, activity_year), for every lei that appears in the LAR.
    A lei missing from that year's filer list gets an explicit placeholder so the
    fact never carries a null lender key.
#}
with lar_lenders as (

    select distinct
        lei,
        activity_year
    from {{ ref('stg_hmda__loan_applications') }}

)

select
    lar_lenders.lei,
    lar_lenders.activity_year,
    coalesce(filers.lender_name, 'Unknown lender (not in filer list)') as lender_name,
    filers.lei is not null as is_in_filer_list,
    {{ surrogate_key(['lar_lenders.lei', "coalesce(filers.lender_name, 'Unknown lender (not in filer list)')"]) }}
        as lender_key
from lar_lenders
left join {{ ref('stg_hmda__filers') }} as filers
    on
        lar_lenders.lei = filers.lei
        and lar_lenders.activity_year = filers.activity_year
