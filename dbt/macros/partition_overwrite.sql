{#
    Partition-overwrite incremental loading, driven by source file hashes.

    A partition is one (activity_year, state_code) LAR file. For each
    incremental model the control table ops.partition_load_log records which
    file hash was last loaded per partition. A partition is "pending" when

      * the model is being built from scratch (first run or --full-refresh), or
      * its year is listed in var('reprocess_years'), or
      * the manifest hash differs from the logged hash (new or changed file).

    The model then
      pre-hook   deletes the pending partitions from the target,
      body       selects only the pending partitions (literal predicate, so the
                 Parquet scan prunes to those files),
      post-hook  records the loaded hashes.

    Re-running with nothing pending selects zero rows. Re-running a pending
    partition replaces it wholesale, so the result does not depend on how many
    times, or in which order, runs happened. That is the idempotency contract
    the backfill test checks.
#}

{% macro create_ops_objects() %}
    {% set log_schema = generate_schema_name('ops', none) %}
    create schema if not exists {{ target.database }}.{{ log_schema }};
    create table if not exists {{ partition_load_log() }} (
        model_name varchar not null,
        activity_year integer not null,
        state_code varchar not null,
        source_sha256 varchar not null,
        loaded_at timestamp not null,
        invocation_id varchar not null
    );
{% endmacro %}

{% macro partition_load_log() -%}
    {{ target.database }}.{{ generate_schema_name('ops', none) }}.partition_load_log
{%- endmacro %}

{% macro pending_partitions() %}
    {#- Returns a list of {'activity_year', 'state_code', 'sha256'} dicts. -#}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}
    {#- Orchestrators that template vars as strings (Cosmos) pass "[2022]"; accept both forms. -#}
    {% set reprocess_years = var('reprocess_years', []) %}
    {% if reprocess_years is string %}
        {% set reprocess_years = fromjson(reprocess_years) %}
    {% endif %}
    {% set query %}
        select m.activity_year, m.state_code, m.sha256
        from {{ source('ops', 'ingest_manifest') }} as m
        where
            m.dataset = 'hmda_lar'
            {% if is_incremental() %}
                and (
                    not exists (
                        select 1
                        from {{ partition_load_log() }} as l
                        where
                            l.model_name = '{{ this.name }}'
                            and l.activity_year = m.activity_year
                            and l.state_code = m.state_code
                            and l.source_sha256 = m.sha256
                    )
                    {% if reprocess_years %}
                        or m.activity_year in ({{ reprocess_years | join(', ') }})
                    {% endif %}
                )
            {% endif %}
        order by 1, 2
    {% endset %}
    {% set rows = run_query(query) %}
    {% set out = [] %}
    {% for row in rows %}
        {% do out.append({'activity_year': row[0], 'state_code': row[1], 'sha256': row[2]}) %}
    {% endfor %}
    {{ return(out) }}
{% endmacro %}

{% macro pending_partition_predicate(year_column='activity_year', state_column='state_code') -%}
    {#- Literal predicate over the pending partitions; `1 = 0` when none. -#}
    {%- set pending = pending_partitions() -%}
    {%- if pending | length == 0 -%}
        1 = 0
    {%- else -%}
        (
            {%- for p in pending %}
            ({{ year_column }} = {{ p.activity_year }} and {{ state_column }} = '{{ p.state_code }}')
            {%- if not loop.last %} or {% endif -%}
            {%- endfor %}
        )
    {%- endif -%}
{%- endmacro %}

{% macro delete_pending_partitions() %}
    {% if is_incremental() %}
        delete from {{ this }} where {{ pending_partition_predicate() }}
    {% else %}
        select 1
    {% endif %}
{% endmacro %}

{% macro log_loaded_partitions() %}
    {% set pending = pending_partitions() %}
    {% if pending | length == 0 %}
        select 1
    {% else %}
        delete from {{ partition_load_log() }}
        where model_name = '{{ this.name }}' and {{ pending_partition_predicate() }};
        insert into {{ partition_load_log() }}
        values
        {% for p in pending %}
            (
                '{{ this.name }}', {{ p.activity_year }}, '{{ p.state_code }}', '{{ p.sha256 }}',
                current_timestamp::timestamp, '{{ invocation_id }}'
            ){% if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
