-- Pass-through view over the external Parquet source. It exists so the bronze
-- columns are introspectable in the catalog (dbt unit tests need that to build
-- typed fixtures); the source itself is a read_parquet() call, not a relation.
select * from {{ source('bronze', 'hmda_lar') }}
