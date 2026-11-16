{#- Membership checks against the seeded HMDA domains, so valid values are listed once. -#}
{% macro in_code_domain(column, field) -%}
    {{ column }} in (select c.code from {{ ref('hmda_codes') }} as c where c.field = '{{ field }}')
{%- endmacro %}

{% macro in_text_domain(column, domain) -%}
    {{ column }} in (select d.value from {{ ref('hmda_derived_domains') }} as d where d.domain = '{{ domain }}')
{%- endmacro %}
