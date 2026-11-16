select
    cast(dataset as varchar) as dataset,
    cast(activity_year as integer) as activity_year,
    cast(state_code as varchar) as state_code,
    cast(file_name as varchar) as file_name,
    cast(source_url as varchar) as source_url,
    cast(bytes as bigint) as raw_bytes,
    cast(sha256 as varchar) as source_sha256,
    cast(row_count as bigint) as raw_row_count,
    cast(fetched_at as timestamp with time zone) as fetched_at,
    cast(http_status as integer) as http_status,
    cast(attempts as integer) as attempts,
    cast(download_seconds as double) as download_seconds
from {{ source('ops', 'ingest_manifest') }}
