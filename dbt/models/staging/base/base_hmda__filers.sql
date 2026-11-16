-- Pass-through view over the external Parquet source; see base_hmda__lar.
select * from {{ source('bronze', 'hmda_filers') }}
