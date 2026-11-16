{#
    HMDA numeric fields carry two sentinels, "NA" and "Exempt", next to real
    numbers. These macros are the single place that interprets them.

    hmda_numeric(col)       -> double, null for NA / Exempt / empty / unparseable
    hmda_value_status(col)  -> reported | not_applicable | exempt | missing | unparseable

    Keeping the status next to the number lets a metric say "average of reported
    values" and lets data quality tests tell "legitimately absent" from "broken".
#}

{% macro safe_cast_double(expression) -%}
    {{ return(adapter.dispatch('safe_cast_double', 'mortgage_metrics')(expression)) }}
{%- endmacro %}

{% macro default__safe_cast_double(expression) -%}
    try_cast({{ expression }} as double)
{%- endmacro %}

{% macro bigquery__safe_cast_double(expression) -%}
    safe_cast({{ expression }} as float64)
{%- endmacro %}

{% macro redshift__safe_cast_double(expression) -%}
    case
        when {{ expression }} ~ '^-?[0-9]+(\\.[0-9]+)?$' then cast({{ expression }} as double precision)
    end
{%- endmacro %}

{% macro hmda_numeric(column) -%}
    {{ safe_cast_double("case when " ~ column ~ " in ('NA', 'Exempt') then null else " ~ column ~ " end") }}
{%- endmacro %}

{% macro hmda_value_status(column) -%}
    case
        when {{ column }} is null then 'missing'
        when {{ column }} = 'NA' then 'not_applicable'
        when {{ column }} = 'Exempt' then 'exempt'
        when {{ safe_cast_double(column) }} is null then 'unparseable'
        else 'reported'
    end
{%- endmacro %}

{# Integer code columns use 1111 for "Exempt". #}
{% macro hmda_code_or_null(column) -%}
    case when {{ column }} = 1111 then null else {{ column }} end
{%- endmacro %}
