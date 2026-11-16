{#
    One row per (denied application, stated reason). A denial can cite up to four
    reasons. Code 10 ("not applicable") and exempt rows carry no reason and are
    dropped; stg already mapped the exempt code 1111 to null.
#}
with denied as (

    select
        application_id,
        partition_year,
        partition_state,
        activity_year,
        state_code,
        _source_sha256,
        denial_reason_1,
        denial_reason_2,
        denial_reason_3,
        denial_reason_4
    from {{ ref('int_hmda__applications_enriched') }}
    where is_denied

),

unpivoted as (

    {% for position in range(1, 5) %}
        select
            application_id,
            partition_year,
            partition_state,
            activity_year,
            state_code,
            _source_sha256,
            {{ position }} as reason_position,
            denial_reason_{{ position }} as denial_reason_code
        from denied
        where denial_reason_{{ position }} is not null and denial_reason_{{ position }} <> 10
        {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select
    unpivoted.*,
    codes.label as denial_reason
from unpivoted
left join {{ ref('hmda_codes') }} as codes
    on
        codes.field = 'denial_reason'
        and unpivoted.denial_reason_code = codes.code
