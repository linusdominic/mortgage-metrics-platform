{#
    Use the configured schema name as-is (staging, marts, ops, ...), not
    <target_schema>_<custom>. The ci target prefixes every schema with ci_ so a
    slim CI build can live beside the deferred production relations.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set base = custom_schema_name | trim if custom_schema_name is not none else target.schema -%}
    {%- if target.name == 'ci' -%}
        ci_{{ base }}
    {%- else -%}
        {{ base }}
    {%- endif -%}
{%- endmacro %}
