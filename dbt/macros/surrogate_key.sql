{#
    Deterministic surrogate key: md5 over the pipe-joined, null-safe inputs.
    Same idea as dbt_utils.generate_surrogate_key; written out here because it
    is the only thing this project would need the package for.
#}
{% macro surrogate_key(columns) -%}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '_null_'){% if not loop.last %} || '|' ||{% endif %}
        {%- endfor %}
    )
{%- endmacro %}
