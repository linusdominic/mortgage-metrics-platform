-- The set of (year, state, file hash) in the fact must equal the set in the
-- manifest. Catches a stale partition (file changed, fact not reloaded), a fact
-- partition with no source file, and a landed file that never reached the fact.
with manifest as (

    select
        activity_year,
        state_code,
        source_sha256
    from {{ ref('stg_ops__ingest_manifest') }}
    where dataset = 'hmda_lar' and raw_row_count > 0

),

fact as (

    select distinct
        activity_year,
        state_code,
        _source_sha256 as source_sha256
    from {{ ref('fct_loan_applications') }}

)

select
    coalesce(manifest.activity_year, fact.activity_year) as activity_year,
    coalesce(manifest.state_code, fact.state_code) as state_code,
    manifest.source_sha256 as manifest_sha256,
    fact.source_sha256 as fact_sha256
from manifest
full outer join fact
    on
        manifest.activity_year = fact.activity_year
        and manifest.state_code = fact.state_code
        and manifest.source_sha256 = fact.source_sha256
where manifest.source_sha256 is null or fact.source_sha256 is null
