{#
    Junk dimension over the FFIEC-derived applicant attributes. These are the
    published "derived" fields (applicant and co-applicant combined), not the raw
    per-applicant codes. See the responsible-use note in the README before
    slicing metrics by them.
#}
{% set domains = ['derived_ethnicity', 'derived_race', 'derived_sex', 'applicant_age'] %}

with
{% for domain in domains %}
    {{ domain }} as (

        select
            value as {{ domain }},
            sort_order as {{ domain }}_sort
        from {{ ref('hmda_derived_domains') }}
        where domain = '{{ domain }}'

    ){% if not loop.last %},{% endif %}
{% endfor %}

select
    cast({{ surrogate_key(domains) }} as varchar) as applicant_demographics_key,
    cast(derived_ethnicity as varchar) as derived_ethnicity,
    cast(derived_race as varchar) as derived_race,
    cast(derived_sex as varchar) as derived_sex,
    cast(
        case applicant_age
            when '8888' then 'Not applicable'
            when '9999' then 'Not available'
            else applicant_age
        end as varchar
    ) as applicant_age_band,
    cast(applicant_age_sort as integer) as applicant_age_band_sort
from derived_ethnicity
cross join derived_race
cross join derived_sex
cross join applicant_age
